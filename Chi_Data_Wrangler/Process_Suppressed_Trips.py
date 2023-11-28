import pandas as pd
import numpy as np
import sys 
import datetime
import os
import matplotlib as plt
import numexpr as ne

pd.options.mode.chained_assignment = None 

# def assign_suppressed_trips(row, df, od, day, hour, minute):

    # df_select = df.loc[(df["DAY"] == day)&(df['HOUR'] == hour)&(df['MINUTE'] == minute)]
    
    # if ((row['GEOID_PICKUP'] in df_select['Pickup Census Tract'].unique()) and (row['GEOID_DROPOFF'] in df_select['Dropoff Census Tract'].unique())):
        # drop_flag = True
    # else:
        # drop_flag = False
        
        
    # return drop_flag
    
def proccess_suppressed_trips(conflation_path, h5_path, outfile, years, agg, tods):    
    conflation = pd.read_csv(conflation_path)
    conflation['COMMUNITY_AREA'] = conflation.area_num_1.astype(float)
    #conflation['GEOID'] = conflation.GEOID.astype(float)

    store = pd.HDFStore(h5_path)
    df9 = pd.DataFrame()

    for year in years:  
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        else:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]

        print('Working on year ' + str(year))
        df8 = pd.DataFrame()

        for month in months:
            print('Working on month ' + str(month))
            df7 = pd.DataFrame()

            for tod in tods:
                print('Working on tod ' + str(tod))
                df6 = pd.DataFrame()

       
                df = store.select(where = ['YEAR == ' + str(year), 'MONTH == ' + str(month)], key = 'Weekday_' + str(tod))

                df['DAY'] = df['Trip Start Timestamp'].dt.day
                df['MINUTE'] = df['Trip Start Timestamp'].dt.minute

                print('Filtering the Data!')

                print(str(len(df[(~np.isnan(df['Pickup Community Area']))&(np.isnan(df['Pickup Census Tract']))&(~np.isnan(df['Dropoff Community Area'])) & (np.isnan(df['Dropoff Census Tract']))])) +  ' Trip Records with Suppressed Origin and Destination out of ' + str(len(df)) )
                print(str(len(df[((np.isnan(df['Pickup Community Area']))&(~np.isnan(df['Pickup Census Tract'])))|((np.isnan(df['Dropoff Community Area'])) & (np.isnan(df['Dropoff Census Tract'])))])) +  ' Trip Records Outside of Chicago but within cook county out of ' + str(len(df)) )
                print(str(len(df[((~np.isnan(df['Pickup Community Area']))&(~np.isnan(df['Pickup Census Tract'])))&((~np.isnan(df['Dropoff Community Area'])) & (~np.isnan(df['Dropoff Census Tract'])))])) +  ' Trip Records with both trip ends within Chicago out of ' + str(len(df)) )
                print(str(len(df[(np.isnan(df['Pickup Community Area']))|(np.isnan(df['Dropoff Community Area']))])) +  ' Trip Records with one trip end outside of Cook county out of ' + str(len(df)) )

                #select out the trips that have community area data and are missing census tract data 
                df = df[~np.isnan(df['Pickup Community Area'])&(~np.isnan(df['Dropoff Community Area']))]
                df2 = df[(np.isnan(df['Pickup Census Tract']))|(np.isnan(df['Dropoff Census Tract']))]
                df2['SUP_PRIVATE_TRIPS'] = np.where(df2['Shared Trip Authorized'] == False, 1, 0)
                df2['SUP_SHARED_TRIPS'] = np.where(df2['Shared Trip Authorized'] == True, 1, 0)

                avg_weekday = (df2.SUP_PRIVATE_TRIPS.sum() + df2.SUP_SHARED_TRIPS.sum())/len(df2.DAY.unique())
                print('There are ' +str(avg_weekday) + ' Weekday Trips that are Suppressed!')

                #aggregate to a 15 minute window because that is what the privacy masking allows...aggregating suppressed (df2) and unsuppressed trips (df)
                # I do a first aggregation for the unsuppressed trips because I only care about keeping the Pickup Cenus Tracts and Dropoff Census Tracts
                df = df.groupby(by = ['Pickup Census Tract', 'Dropoff Census Tract', 'YEAR','MONTH','DAY','HOUR','MINUTE'], as_index = False).first()
                df2 = df2.groupby(by = ['Pickup Community Area', 'Dropoff Community Area', 'YEAR','MONTH','DAY','HOUR','MINUTE'], as_index = False).agg(agg)
                
                
                
                #make a column to iterate through
                df2['OD_PAIRS'] = df2['Pickup Community Area'].astype(str) + '_' + df2['Dropoff Community Area'].astype(str)
                
                #conflate the community area to the census tract centroids that fall within it
                df2 = df2.merge(conflation[['GEOID','COMMUNITY_AREA']], how = 'left', left_on = 'Pickup Community Area' , right_on = 'COMMUNITY_AREA')
                df2 = df2.merge(conflation[['GEOID','COMMUNITY_AREA']], how = 'left', left_on = 'Dropoff Community Area' , right_on = 'COMMUNITY_AREA', suffixes = ('_PICKUP','_DROPOFF'))
                
                
                #drop the census tract pairs that have unsuppressed trips because they do not need to be assigned any unsuppressed trips
                od_drop = df2.merge(df,how = 'inner', left_on = ['YEAR','MONTH','DAY','HOUR','MINUTE','GEOID_PICKUP','GEOID_DROPOFF'], right_on = ['YEAR','MONTH','DAY','HOUR','MINUTE','Pickup Census Tract','Dropoff Census Tract'])
                
                od_drop['OD_PAIRS'] = od_drop['GEOID_PICKUP'].astype(str) +'_' + od_drop['GEOID_DROPOFF'].astype(str)
                #df2['OD'] = df2['GEOID_PICKUP'].astype(str) +'_' + df2['GEOID_DROPOFF'].astype(str)

                df2 = df2[~df2['OD_PAIRS'].isin(od_drop['OD_PAIRS'])]


                #iterate through each of the suppressed Community Area pairings
                print('Working on assigning suppressed trips!')
                
                df = pd.DataFrame()
                od_drop = pd.DataFrame()


                for day in df2.DAY.unique():
                    df5 = pd.DataFrame()
                    print('Working on Day ' + str(day))
                    #od_trips = od_trips.query("DAY == " + str(day))
                    #od_trips = od_trips[od_trips["DAY"] == day]
                    df_select1 = df2.loc[df2["DAY"] == day]

                    for hour in df_select1.HOUR.unique():
                        df4 = pd.DataFrame()

                        #od_trips = od_trips.query("HOUR == " + str(hour))
                        #od_trips = od_trips[od_trips["HOUR"] == hour]
                        df_select2 = df_select1.loc[df_select1["HOUR"] == hour]
    
                        for minute in df_select2.MINUTE.unique():
                            df3 = pd.DataFrame()
                            df_select3 = df_select2.loc[df_select2["MINUTE"] == minute]
                            #od_trips = od_trips.query("MINUTE == " + str(minute))
                            #od_trips = od_trips[od_trips["MINUTE"] == minute]
                            
                            for od in df_select3['OD_PAIRS'].unique():
                                od_trips = df_select3.loc[df_select3["OD_PAIRS"] == od]
                                od_trips['SCALAR'] = np.random.dirichlet(np.ones(len(od_trips)))
                                #od_trips['SUP_PRIVATE_TRIPS'] = od_trips['SUP_PRIVATE_TRIPS']*od_trips['SCALAR']
                                #od_trips['SUP_SHARED_TRIPS'] = od_trips['SUP_SHARED_TRIPS']*od_trips['SCALAR']
                                od_trip = od_trips[od_trips['SCALAR'] == od_trips['SCALAR'].max()]
                                #od_trip['SUP_PRIVATE_TRIPS'] = od_trips['SUP_PRIVATE_TRIPS'].sum()
                                #od_trip['SUP_SHARED_TRIPS'] = od_trips['SUP_SHARED_TRIPS'].sum()
                                
                                df3 = df3.append(od_trip)
                    
                            df4 = df4.append(df3)
                    
                        df5 = df5.append(df4)
                    
                    #aggregating here to reduce the size of the table as the script moves along
                    df5 = df5.groupby(by = ['GEOID_PICKUP', 'GEOID_DROPOFF', 'YEAR','MONTH','DAY'], as_index = False).agg(agg)

                    df6 = df6.append(df5)
                    
                df6 = df6.groupby(by = ['GEOID_PICKUP', 'GEOID_DROPOFF', 'YEAR','MONTH','DAY'], as_index = False).agg(agg)

                df7 = df7.append(df6)
            
            df7 = df7.groupby(by = ['GEOID_PICKUP', 'GEOID_DROPOFF', 'YEAR','MONTH'], as_index = False).agg(agg)

            df8 = df8.append(df7)
            
        df9 = df9.append(df8)



    #grouped = df6[['SCALED_SUP_PRIVATE_TRIPS','SCALED_SUP_SHARED_TRIPS', 'GEOID_PICKUP','GEOID_DROPOFF', 'MONTH','YEAR','TOD','Pickup Community Area', 'Dropoff Community Area']].groupby(by = ['GEOID_PICKUP', 'GEOID_DROPOFF','MONTH','YEAR','TOD'], as_index = False).agg(agg)        
    grouped = df9
    df9.to_csv(outfile)
    
