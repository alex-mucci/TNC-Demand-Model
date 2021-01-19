import pandas as pd
import numpy as np
import sys 
import datetime
import os
import matplotlib as plt


def proccess_suppressed_trips(conflation_path, h5_path, outfile, years, agg, tods):    
    conflation = pd.read_csv(conflation_path)
    conflation['COMMUNITY_AREA'] = conflation.area_num_1.astype(float)
    store = pd.HDFStore(h5_path)

    for year in years:  
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        else:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]

        print('Working on year ' + str(year))
        df5 = pd.DataFrame()

        for month in months:
            print('Working on month ' + str(month))
            df4 = pd.DataFrame()

            for tod in tods:
                print('Working on tod ' + str(tod))

                df3 = pd.DataFrame()
                df2 = pd.DataFrame()
                df = pd.DataFrame()
                df = store.select(where = ['YEAR == ' + str(year)], key = 'Weekday_' + str(tod))

                print('Filtering the Data!')
                df = df[df['MONTH'] == month]
                df['DAY'] = df['Trip Start Timestamp'].dt.day

                print(str(len(df[(~np.isnan(df['Pickup Community Area']))&(np.isnan(df['Pickup Census Tract']))&(~np.isnan(df['Dropoff Community Area'])) & (np.isnan(df['Dropoff Census Tract']))])) +  ' Trip Records with Suppressed Origin and Destination out of ' + str(len(df)) )
                print(str(len(df[((np.isnan(df['Pickup Community Area']))&(~np.isnan(df['Pickup Census Tract'])))|((np.isnan(df['Dropoff Community Area'])) & (np.isnan(df['Dropoff Census Tract'])))])) +  ' Trip Records Outside of Chicago but within cook county out of ' + str(len(df)) )
                print(str(len(df[((~np.isnan(df['Pickup Community Area']))&(~np.isnan(df['Pickup Census Tract'])))&((~np.isnan(df['Dropoff Community Area'])) & (~np.isnan(df['Dropoff Census Tract'])))])) +  ' Trip Records with both trip ends within Chicago out of ' + str(len(df)) )
                print(str(len(df[(np.isnan(df['Pickup Community Area']))|(np.isnan(df['Dropoff Community Area']))])) +  ' Trip Records with one trip end outside of Cook county out of ' + str(len(df)) )


                #select out the trips that have community area data and are missing census tract data 
                df['SUP_PRIVATE_TRIPS'] = np.where(df['Shared Trip Authorized'] == False, 1, 0)
                df['SUP_SHARED_TRIPS'] = np.where(df['Shared Trip Authorized'] == True, 1, 0)
                df = df[(np.isnan(df['Pickup Census Tract']))|(np.isnan(df['Dropoff Census Tract']))]
                df = df[~np.isnan(df['Pickup Community Area'])&(~np.isnan(df['Dropoff Community Area']))]


                df = df.groupby(by = ['Pickup Community Area', 'Dropoff Community Area', 'YEAR','MONTH','DAY'], as_index = False).sum()
                df = df.groupby(by = ['Pickup Community Area', 'Dropoff Community Area','YEAR','MONTH'], as_index = False).mean()

                print('There are ' +str(df.SUP_PRIVATE_TRIPS.sum() + df.SUP_SHARED_TRIPS.sum()) + ' Average Weekday Trips that are Suppressed!')

                #make a column to iterate through
                df['OD_PAIRS'] = df['Pickup Community Area'].astype(str) + '_' + df['Dropoff Community Area'].astype(str)

                #iterate through each of the suppressed Community Areas
                print('Working on assigning suppressed trips!')
                for od in df['OD_PAIRS'].unique():

                    #select out the trips that are originating from the given community area
                    od_trips = df[df['OD_PAIRS'] == od]

                    #conflate the community area to the census tract centroids that fall within it
                    df2 = od_trips[['SUP_PRIVATE_TRIPS', 'SUP_SHARED_TRIPS', 'Pickup Community Area', 'Dropoff Community Area']].merge(conflation[['GEOID','COMMUNITY_AREA']], how = 'left', left_on = 'Pickup Community Area' , right_on = 'COMMUNITY_AREA')
                    df2 = df2.merge(conflation[['GEOID','area_num_1']], how = 'left', left_on = 'Dropoff Community Area' , right_on = 'area_num_1', suffixes = ('_PICKUP','_DROPOFF'))

                    df2['SCALAR'] = np.random.dirichlet(np.ones(len(df2)))
                    df2['SCALED_SUP_PRIVATE_TRIPS'] = df2['SUP_PRIVATE_TRIPS']*df2['SCALAR']
                    df2['SCALED_SUP_SHARED_TRIPS'] = df2['SUP_SHARED_TRIPS']*df2['SCALAR']
                    df3 = df3.append(df2)
                df3['TOD'] = tod       

                df4 = df3.append(df3)
            df4['MONTH'] = month


            df5 = df4.append(df4)
        df5['YEAR'] = year    

        df6 = df5.append(df5)


    grouped = df6[['SCALED_SUP_PRIVATE_TRIPS','SCALED_SUP_SHARED_TRIPS', 'GEOID_PICKUP','GEOID_DROPOFF', 'MONTH','YEAR','Pickup Community Area', 'Dropoff Community Area']].groupby(by = ['GEOID_PICKUP', 'GEOID_DROPOFF','MONTH','YEAR'], as_index = False).agg(agg)        

    grouped.to_csv(outfile)
    
