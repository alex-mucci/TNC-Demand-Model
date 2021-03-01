import pandas as pd
import numpy as np


def format_chi_rh_data(tods, years, agg, chi_rh_h5_filepath, suppressed_trips_filepath, empty_matrix_filepath, raw_outfile, no_suppressed_outfile, final_outfile):
	df_all = pd.DataFrame()
	df_raw = pd.DataFrame()

	for year in years:
		print('Working on year ' + str(year))
		for tod in tods:
			print('Working on tod ' + str(tod))
			df = pd.read_hdf(chi_rh_h5_filepath, where = 'YEAR == ' + str(year), key = 'Weekday_' + str(tod))
			print(df['Trip Seconds'].max())
			print(df['Trip Miles'].max())

			df = df.dropna(subset = ['Pickup Census Tract', 'Dropoff Census Tract'])
			df['DAY'] = df['Trip Start Timestamp'].dt.day
			df['PRIVATE_TRIPS'] = np.where(df['Shared Trip Authorized'] == False, 1, 0)
			df['SHARED_TRIPS'] = np.where(df['Shared Trip Authorized'] == True, 1, 0)  

			## the filters are higher than the longest trip because the trip could be pooled
			# filter out the trips that are longer than 50 miles becuase the longest possible trip is 35 miles going from O'Hare airport to south east corner
			df2 = df[df['Trip Miles'] <= 50]

			# filter out the trips that are longer than 2 hours becuase the longest possible trip is 1 hours going from O'Hare airport to south east corner
			df2 = df2[df2['Trip Seconds'] <= 7200]

			print(df2['Trip Seconds'].max())
			print(df2['Trip Miles'].max())

			#census tract XXX replaces the trips assigned to census tract XXX because they are likely misasigned. Census tract XXX contains
			#contains O'Hare airport and census tract is adjacent. The trips assigned to census tract XXX are likely trips from the airport.
			df2.loc[df2['Pickup Census Tract'] == 17031770700, 'Pickup Census Tract'] = 17031980000
			df2.loc[df2['Dropoff Census Tract'] == 17031770700, 'Dropoff Census Tract'] = 17031980000


			#trips to census tract XXX are likely misasigned but it is not clear which census tract they should be assigned to.
			#there is only one trip, so it is droped
			df2 = df2[df2['Pickup Census Tract'] != 17031810502]
			df2 = df2[df2['Dropoff Census Tract'] != 17031810502]

			df2 = df2.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR', 'MONTH','DAY'], as_index =False).agg(agg)
			df2 = df2.groupby(by= ['Pickup Census Tract','Dropoff Census Tract','YEAR','MONTH'], as_index =False).mean()

			df['TOD'] = tod
			df2['TOD'] = tod

			df_raw = df_raw.append(df)
			df_all = df_all.append(df2)
			
	print('Writting out the raw files!')
	df_raw.to_csv(raw_outfile)
	df_all.to_csv(no_suppressed_outfile)
    
	df_raw = pd.DataFrame()

	print('Adding in the Suppressed Trips!')

	sup_trips = pd.read_csv(suppressed_trips_filepath)

	df_final = df_all.merge(sup_trips[['GEOID_PICKUP', 'GEOID_DROPOFF', 'SCALED_SUP_PRIVATE_TRIPS', 'SCALED_SUP_SHARED_TRIPS','YEAR', 'MONTH', 'TOD']], how = 'left', left_on = ['Pickup Census Tract','Dropoff Census Tract', 'YEAR', 'MONTH', 'TOD'],right_on = ['GEOID_PICKUP', 'GEOID_DROPOFF', 'YEAR', 'MONTH', 'TOD'])
	df_all = pd.DataFrame()
    
	sup_trips = pd.DataFrame()
	df_final['SHARED_TRIPS'] = df_final['SHARED_TRIPS'] + df_final['SCALED_SUP_SHARED_TRIPS']
	df_final['PRIVATE_TRIPS'] = df_final['PRIVATE_TRIPS'] + df_final['SCALED_SUP_PRIVATE_TRIPS']
	df_final['ALL_TRIPS'] = df_final['PRIVATE_TRIPS'] + df_final['SHARED_TRIPS']
	
	print('Attaching trips to the empty matrix!')
	empty = pd.read_csv(empty_matrix_filepath)
	empty.DESTINATION = empty.DESTINATION.astype(float)
	empty.ORIGIN = empty.ORIGIN.astype(float)
	rh_final = empty.merge(df_final, how = 'left', left_on = ['ORIGIN', 'DESTINATION', 'YEAR', 'MONTH','TOD'], right_on = ['Pickup Census Tract', 'Dropoff Census Tract', 'YEAR', 'MONTH', 'TOD'])
	rh_final = rh_final.fillna(0)
	
	print('Writing out the file!')
	rh_final.to_csv(final_outfile)
	
	