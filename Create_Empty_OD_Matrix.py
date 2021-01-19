import pandas as pd
import geopandas as gp


def create_empty_od_matrix(tracts_shapefile_path, years, tods, outfile):
	df = pd.DataFrame()
    df3 = pd.DataFrame()
    
    for year in years:  
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        else:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]
            
        print('Working on year ' + str(year))
        df4 = pd.DataFrame()
        
        for month in months:
            print('Working on month ' + str(month))
            df3 = pd.DataFrame()
            
            for tod in tods:
                print('Working on tod ' + str(tod))
                df2 = pd.DataFrame()
                print('Working on TOD ' + str(tod))
            
                for tract in tracts.geoid10:
                    df['DESTINATION'] = tracts.geoid10
                    df['ORIGIN'] = tract
                    df2 = df2.append(df)
                    
                df2['TOD'] = tod
                df3 = df3.append(df2)
                
            df3['MONTH'] = month   
            df4 = df4.append(df3)
            
        df4['YEAR'] = year
        df5 = df5.append(df4)
    
    df5.to_csv(outfile)

            
