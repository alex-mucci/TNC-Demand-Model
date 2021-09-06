import pandas as pd
import geopandas as gp
import numpy as np
import glob

def agg_otp_transit_times(tods, years, otp_transit_input_folders, output_folder):

    df4 = pd.DataFrame()
            
    for year in years:
        df3 = pd.DataFrame()
        print('Working on year ' + str(year))
    
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        elif year == 2019:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]
        else:
            print('Bad Year!')
            
        for month in months:
            df2 = pd.DataFrame()
            #set the date variable to read the correct OTP transit travel times for the year-month-tod
            if (year == 2019) & (month in [2,3,4]):
                date = '20190206'
            elif (year == 2019) & (month in [5,6,7]):
                date = '20190605'
            elif (year == 2019) & (month in [8,9,10]):
                date = '20190904'
            elif (year == 2019) & (month in [11,12]):
                date = '20191204'
            elif (year == 2020) & (month in [1]):
                date = '20191204'
            elif (year == 2019) & (month in [1]):
                date = '20181205'
            elif (year == 2020) & (month in [2]):
                date = '20200205'
            elif year == 2018:
                date = '20181205'
            else:
                print('Bad month and year combination!')
                
            print('Working on month ' + str(month))
            for tod in tods: 
                print('Working on TOD ' + str(tod))
                path = otp_transit_input_folders + date + '/' + str(tod)  # use your path
                all_files = glob.glob(path + "/*.csv")

                for filename in all_files:
                    #print('Working on file ' + filename)
                    df = pd.read_csv(filename, index_col=None, header=0)
                    df2 = df2.append(df)
                    
                df2['TOD'] = tod
                df2 = df2.groupby(by = ['origin', 'destination', 'TOD'], as_index = False).mean()
                
            df2['MONTH'] = month
            df3 = df3.append(df2)
           
        df3['YEAR'] = year
        df4 = df4.append(df3)
    
    df4.to_csv(output_folder + 'OTP_Transit_Travel_Times.csv')
        
    return df4

