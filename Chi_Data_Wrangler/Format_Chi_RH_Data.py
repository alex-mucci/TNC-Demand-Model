import pandas as pd
import numpy as np


def format_chi_rh_data(tods, years, agg, chi_rh_h5_filepath, suppressed_trips_filepath, empty_matrix_filepath, raw_outfile, no_suppressed_private_outfile, no_suppressed_shared_outfile, final_outfile):
	df_all_private = pd.DataFrame()
	df_all_shared = pd.DataFrame()

	df_raw = pd.DataFrame()

	# for year in years:
		# print('Working on year ' + str(year))
	for tod in tods:
		print('Working on tod ' + str(tod))
		df = pd.read_hdf(chi_rh_h5_filepath,key = 'Weekday_' + str(tod))
        
		df['TRAVEL_TIME_MINUTES'] = df['Trip Seconds']/60
		print(df['TRAVEL_TIME_MINUTES'].max())
		print(df['Trip Miles'].max())

		df = df[(~np.isnan(df['Pickup Census Tract']))& (~np.isnan(df['Dropoff Census Tract']))]
		df['DAY'] = df['Trip Start Timestamp'].dt.day


		#df['PRIVATE_TRIPS'] = np.where(df['Shared Trip Authorized'] == False, 1, 0)
		#df['SHARED_TRIPS'] = np.where(df['Shared Trip Authorized'] == True, 1, 0)  

		## the filters are higher than the longest trip because the trip could be pooled
		# filter out the trips that are longer than 50 miles becuase the longest possible trip is 35 miles going from O'Hare airport to south east corner
		df2 = df[df['Trip Miles'] <= 50]
		# filter out the trips that are longer than 2 hours becuase the longest possible trip is 1 hours going from O'Hare airport to south east corner
		df2 = df2[df2['TRAVEL_TIME_MINUTES'] <= 120]


		#I decided to filter based on speed instead because it incorporates both travel time and distance
		df2['SPEED'] = df2['Trip Miles']/(df2['TRAVEL_TIME_MINUTES']/60)
		df2 = df2[df2['SPEED'] > 1]
		df2 = df2[df2['SPEED'] < 60]
		
		print('Maximum Travel Time')
		print(df2['TRAVEL_TIME_MINUTES'].max())
        
		print('Maximum Trip Length')
		print(df2['Trip Miles'].max())
        
		print('Maximum Speed')
		print(df2['SPEED'].max())

		#census tract 17031980000 replaces the trips assigned to census tract 17031770700 because they are likely misasigned. 
		#Census tract 17031980000 contains contains O'Hare airport and census tract is adjacent. The trips assigned to census tract 17031770700 are likely trips from the airport.
		df2.loc[df2['Pickup Census Tract'] == 17031770700, 'Pickup Census Tract'] = 17031980000
		df2.loc[df2['Dropoff Census Tract'] == 17031770700, 'Dropoff Census Tract'] = 17031980000


		#trips to census tract 17031810502 are likely misasigned but it is not clear which census tract they should be assigned to.
		#there is only one trip, so it is droped
		df2 = df2[df2['Pickup Census Tract'] != 17031810502]
		df2 = df2[df2['Dropoff Census Tract'] != 17031810502]

		#drop the trips that have a trip length of 0 or travel time of 0 because they do not 
		#make sense
		df2 = df2[df2['Trip Miles'] > 0]
		df2 = df2[df2['TRAVEL_TIME_MINUTES'] > 0]

		df_private = df2[df2['Shared Trip Authorized'] == 0]
		df_shared = df2[df2['Shared Trip Authorized'] == 1]
		df_private['TRIPS'] = 1
		df_shared['TRIPS'] = 1
		
		df_private = df_private.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR', 'MONTH','DAY'], as_index =False).agg(agg)
		df_shared = df_shared.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR', 'MONTH','DAY'], as_index =False).agg(agg)
		
		df_all_private = df_all_private.append(df_private)
		df_all_shared = df_all_shared.append(df_shared)



	df_all_private = df_all_private.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR', 'MONTH','DAY'], as_index =False).agg(agg)
	df_all_shared = df_all_shared.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR', 'MONTH','DAY'], as_index =False).agg(agg)

	df_all_private = df_all_private.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR','MONTH'], as_index =False).agg(agg)
	df_all_shared = df_all_shared.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR','MONTH'], as_index =False).agg(agg)
        

	print('Writting out the trip record files!')
	df_all_private.to_csv(no_suppressed_private_outfile)
	df_all_shared.to_csv(no_suppressed_shared_outfile)
    
    
	df_final = df_all_private.merge(df_all_shared, on = ['Pickup Census Tract','Dropoff Census Tract', 'YEAR', 'MONTH'], how = 'outer',suffixes = ('_PRIVATE','_SHARED'))
	df_all_private = pd.DataFrame()
	df_all_shared = pd.DataFrame()
    
	print('Adding in the Suppressed Trips!')
	sup_trips = pd.read_csv(suppressed_trips_filepath)
	#sup_trips = sup_trips[(sup_trips['MONTH'] == month)&(sup_trips['YEAR'] == year)&(sup_trips['TOD'] == tod)]


	df_final = df_final.merge(sup_trips, how = 'outer', left_on = ['Pickup Census Tract','Dropoff Census Tract', 'YEAR', 'MONTH'],right_on = ['GEOID_PICKUP', 'GEOID_DROPOFF', 'YEAR', 'MONTH'],suffixes = ('','_SUPPRESSED'))

# fill in the missing values with zero because they should be zero... for example you might have a private but not a shared trip between zones
	df_final['TRIPS_SHARED'] = df_final['TRIPS_SHARED'].fillna(0)
	df_final['TRIPS_PRIVATE'] = df_final['TRIPS_PRIVATE'].fillna(0)
	df_final['SUP_PRIVATE_TRIPS'] = df_final['SUP_PRIVATE_TRIPS'].fillna(0)
	df_final['SUP_SHARED_TRIPS'] = df_final['SUP_SHARED_TRIPS'].fillna(0)
    
#clean up the column names to make it easier to use them when modeling... 
## I dont have the trip attributes of the suppressed trips split out by shared/private so only the trips variable has 4 categories/values
### I did not split them out because the script assigning suppressed trips takes long enough already and I did not feel like it was necessary



    
	df_final['TRIPS_SHARED_UNSUPPRESSED'] = df_final['TRIPS_SHARED']
	df_final['TRIPS_PRIVATE_UNSUPPRESSED'] = df_final['TRIPS_PRIVATE']
	df_final['TRIPS_PRIVATE_SUPPRESSED'] = df_final['SUP_PRIVATE_TRIPS']
	df_final['TRIPS_SHARED_SUPPRESSED'] = df_final['SUP_SHARED_TRIPS']
	df_final['TRAVEL_TIME_MINUTES_UNSUPPRESSED'] = 	(df_final['TRAVEL_TIME_MINUTES_SHARED'] + df_final['TRAVEL_TIME_MINUTES_PRIVATE'])/2
	df_final['TRAVEL_TIME_MINUTES_SUPPRESSED'] = df_final['Trip Seconds']/60
	df_final['DISTANCE_UNSUPPRESSED'] = (df_final['Trip Miles_SHARED'] + df_final['Trip Miles_PRIVATE'])/2
	df_final['DISTANCE_SUPPRESSED'] = df_final['Trip Miles']
	df_final['FARE_UNSUPPRESSED'] = (df_final['Fare_SHARED'] + df_final['Fare_PRIVATE'])/2
	df_final['FARE_SUPPRESSED'] = df_final['Fare']
	df_final['TIP_UNSUPPRESSED'] = (df_final['Tip_SHARED'] + df_final['Tip_PRIVATE'])/2
	df_final['TIP_SUPPRESSED'] = df_final['Tip']
	df_final['TAX_AND_SURGE_UNSUPPRESSED'] = (df_final['Additional Charges_SHARED'] + df_final['Additional Charges_PRIVATE'])/2
	df_final['TAX_AND_SURGE_SUPPRESSED'] = df_final['Additional Charges']
	df_final['TOTAL_COST_UNSUPPRESSED'] = (df_final['Trip Total_SHARED'] + df_final['Trip Total_PRIVATE'])/2
	df_final['TOTAL_COST_SUPPRESSED'] = df_final['Trip Total']
	df_final['TRIPS_POOLED_UNSUPPRESSED'] = (df_final['Trips Pooled_SHARED'] + df_final['Trips Pooled_PRIVATE'])/2
	df_final['TRIPS_POOLED_SUPPRESSED'] = df_final['Trips Pooled']

	df_final = df_final.drop(['SUP_PRIVATE_TRIPS','SUP_SHARED_TRIPS','Trip Seconds','Trip Miles','Trip Total', 'Additional Charges','Tip','Fare','Trips Pooled'], axis = 1)

	df_final['TRIPS_SHARED'] = df_final['TRIPS_SHARED_UNSUPPRESSED'] + df_final['TRIPS_SHARED_SUPPRESSED']
	df_final['TRIPS_PRIVATE'] = df_final['TRIPS_PRIVATE_UNSUPPRESSED'] + df_final['TRIPS_PRIVATE_SUPPRESSED']
	df_final['TRIPS_SUPPRESSED'] = df_final['TRIPS_SHARED_SUPPRESSED'] + df_final['TRIPS_PRIVATE_SUPPRESSED']
	df_final['TRIPS_UNSUPPRESSED'] = df_final['TRIPS_SHARED_UNSUPPRESSED'] + df_final['TRIPS_PRIVATE_UNSUPPRESSED']

	df_final['TRIPS_ALL'] = df_final['TRIPS_PRIVATE'] + df_final['TRIPS_SHARED']

	df_final['TRAVEL_TIME_MINUTES'] = (df_final['TRAVEL_TIME_MINUTES_SUPPRESSED'] + df_final['TRAVEL_TIME_MINUTES_UNSUPPRESSED'])/2
	df_final['DISTANCE'] = (df_final['DISTANCE_SUPPRESSED'] + df_final['DISTANCE_UNSUPPRESSED'])/2
	df_final['FARE'] = (df_final['FARE_SUPPRESSED'] + df_final['FARE_UNSUPPRESSED'])/2
	df_final['TIP'] = (df_final['TIP_SUPPRESSED'] + df_final['TIP_UNSUPPRESSED'])/2
	df_final['FARE'] = (df_final['FARE_SUPPRESSED'] + df_final['FARE_UNSUPPRESSED'])/2
	df_final['TAX_AND_SURGE'] = (df_final['TAX_AND_SURGE_SUPPRESSED'] + df_final['TAX_AND_SURGE_UNSUPPRESSED'])/2
	df_final['TOTAL_COST'] = (df_final['TOTAL_COST_SUPPRESSED'] + df_final['TOTAL_COST_UNSUPPRESSED'])/2
	df_final['TRIPS_POOLED'] = df_final['TRIPS_POOLED_SUPPRESSED'] + df_final['TRIPS_POOLED_UNSUPPRESSED']



	print('Attaching trips to the empty matrix!')
	empty = pd.read_csv(empty_matrix_filepath)
	empty.DESTINATION = empty.DESTINATION.astype(float)
	empty.ORIGIN = empty.ORIGIN.astype(float)
    
	#have to groupby here because the empty matrix has a TOD column that duplicates the data 5 times.. this gets rid of the duplicates
	empty = empty.groupby(by = ['ORIGIN', 'DESTINATION', 'YEAR', 'MONTH'], as_index = False).mean()
    
	rh_final = empty.merge(df_final, how = 'left', left_on = ['ORIGIN', 'DESTINATION', 'YEAR', 'MONTH'], right_on = ['Pickup Census Tract', 'Dropoff Census Tract', 'YEAR', 'MONTH'])
	empty = pd.DataFrame()
	df_final = pd.DataFrame()
	#rh_final = rh_final.fillna(0)
	
	print('Writing out the file!')
	rh_final.to_csv(final_outfile)
	
	