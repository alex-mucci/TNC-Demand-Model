import pandas as pd 
import numpy as np

def set_avg_veh_per_hhlds(row):
    if row['TOTAL_HHLDS']  == 0:
        veh = 0
        
    else:
        veh = (row['HHLDS_0_VEH']*0 + row['HHLDS_1_VEH']*1 + row['HHLDS_2_VEH']*2 + row['HHLDS_3P_VEH']*3)/row['TOTAL_HHLDS']
        
    return veh

def process_acs_data(tods, years, folder_path, acs_tables, output_folder):
	
    
    df5 = pd.DataFrame()
    
    for acs_table in acs_tables:
        print('Working on ACS Table ' + acs_table)
        df4 = pd.DataFrame()
        
        if acs_table == 'DP05':
            keep = ['DP05_0001E', 'DP05_0002E', 'DP05_0003E', 'DP05_0005E', 'DP05_0006E', 'DP05_0007E','DP05_0008E', 'DP05_0009E', 'DP05_0010E', 'DP05_0011E', 'DP05_0012E', 'DP05_0013E', 'DP05_0014E', 'DP05_0015E', 'DP05_0016E', 'DP05_0017E', 'DP05_0018E', 'CENSUS_TRACT'] 
            rename = ['TOTAL_POP', 'TOTAL_MALE', 'TOTAL_FEMALE', 'AGE_5U', 'AGE_5_9', 'AGE_10_14', 'AGE_15_19', 'AGE_20_24', 'AGE_25_34', 'AGE_35_44', 'AGE_45_54', 'AGE_55_59', 'AGE_60_64', 'AGE_65_74', 'AGE_75_84', 'AGE_85P', 'MEDIAN_AGE', 'CENSUS_TRACT']
            
        elif acs_table == 'S1101':
            keep = ['S1101_C01_001E', 'S1101_C01_002E', 'S1101_C01_004E', 'CENSUS_TRACT']
            rename = ['TOTAL_HHLDS', 'AVG_HHLD_SIZE', 'AVG_FAMILY_SIZE', 'CENSUS_TRACT']
            
        elif acs_table == 'S1501':
            keep = ['S1501_C01_001E', 'S1501_C01_002E', 'S1501_C01_003E', 'S1501_C01_004E', 'S1501_C01_005E', 'S1501_C01_006E', 'S1501_C01_007E', 'S1501_C01_008E', 'S1501_C01_009E', 'S1501_C01_010E', 'S1501_C01_011E', 'S1501_C01_012E', 'S1501_C01_013E', 'S1501_C01_014E', 'S1501_C01_015E','CENSUS_TRACT']
            rename = ['TOTAL_POP_18_24', 'TOTAL_POP_18_24_NO_HIGH_SCHOOL', 'TOTAL_POP_18_24_HIGH_SCHOOL', 'TOTAL_POP_18_24_SOME_COLLEGE', 'TOTAL_POP_18_24_SOME_BACHELORS_HIGHER', 'TOTAL_POP_25P', 'TOTAL_POP_25P_LESS_THAN_9TH', 'TOTAL_POP_25P_9TH_TO_12TH', 'TOTAL_POP_25P_HIGH_SCHOOL', 'TOTAL_POP_25P_SOME_COLLEGE', 'TOTAL_POP_25P_ASSOCIATES', 'TOTAL_POP_25P_BACHELORS', 'TOTAL_POP_25P_GRADUATE', 'TOTAL_POP_25P_HIGH_SCHOOL_HIGHER', 'TOTAL_POP_25P_BACHELORS_HIGHER','CENSUS_TRACT']
        
        elif acs_table == 'S1901':
            keep = ['S1901_C01_002E', 'S1901_C01_003E', 'S1901_C01_004E', 'S1901_C01_005E', 'S1901_C01_006E', 'S1901_C01_007E', 'S1901_C01_008E', 'S1901_C01_009E', 'S1901_C01_010E', 'S1901_C01_011E', 'S1901_C01_012E', 'S1901_C01_013E','CENSUS_TRACT']
            rename = ['TOTAL_HHLDS_LESS_10K', 'TOTAL_HHLDS_10K_15K', 'TOTAL_HHLDS_15K_25K', 'TOTAL_HHLDS_25K_35K', 'TOTAL_HHLDS_35K_50K', 'TOTAL_HHLDS_50K_75K', 'TOTAL_HHLDS_75K_100K', 'TOTAL_HHLDS_100K_150K', 'TOTAL_HHLDS_150K_200K', 'TOTAL_HHLDS_200KP', 'HHLDS_MEDIAN_INCOME', 'HHLDS_MEAN_INCOME','CENSUS_TRACT']
        
        elif acs_table == 'DP04':
            keep = ['DP04_0057E', 'DP04_0058E', 'DP04_0059E', 'DP04_0060E', 'DP04_0061E', 'CENSUS_TRACT']
            rename = ['TOTAL_HHLDS', 'HHLDS_0_VEH', 'HHLDS_1_VEH', 'HHLDS_2_VEH', 'HHLDS_3P_VEH', 'CENSUS_TRACT']
        
        else:
            print('Bad ACS Table!')
        
        for year in years:
            print('Working on year ' + str(year))
            df3 = pd.DataFrame()

            if year == 2018:
                months = [11,12]
            elif year == 2020:
                months = [1,2]
            else:
                months = [1,2,3,4,5,6,7,8,9,10,11,12]
                
                
            for month in months:
                print('Working on month ' + str(month))
                df2 = pd.DataFrame()

                for tod in tods:
                    print('Working on TOD ' + str(tod))
                    df = pd.read_csv(folder_path + str(year) + '/' + acs_table + '/' + acs_table + '.csv')
                    df[['TRACT', 'COUNTY', 'STATE']] = df.NAME.str.split(',', expand=True)
                    df = df[df['COUNTY'] == ' Cook County']
                    df['CENSUS_TRACT'] = df.apply(lambda row: row['GEO_ID'][9:], axis = 1)
                    df = df[keep]
                    df.columns = rename
                   
                    df['TOD'] = tod
                    df2 = df2.append(df)
                
                df2['MONTH'] = month
                df3 = df3.append(df2)

            df3['YEAR'] = year
            df4 = df4.append(df3)
        #df4 = df4.set_index(['YEAR', 'MONTH','TOD', 'CENSUS_TRACT'])
        df5 = df5.append(df4)
        df5 = df5.groupby(by = ['YEAR', 'MONTH','TOD', 'CENSUS_TRACT'], as_index = False).first()

    df5['HHLDS_0_VEH'] = df5.HHLDS_0_VEH.astype(float)
    df5['HHLDS_1_VEH'] = df5.HHLDS_1_VEH.astype(float)
    df5['HHLDS_2_VEH'] = df5.HHLDS_2_VEH.astype(float)
    df5['HHLDS_3P_VEH'] = df5.HHLDS_3P_VEH.astype(float)
    df5['TOTAL_HHLDS'] = df5.TOTAL_HHLDS.astype(float)
    
    df5['AVG_VEH_PER_HHLD'] = df5.apply(lambda row: set_avg_veh_per_hhlds(row), axis = 1)
    
    variables = df5.columns.drop(['YEAR', 'MONTH','TOD', 'CENSUS_TRACT'])
    for var in variables:    
        df5[var] = df5[var].replace('-','0')
        df5[var] = df5[var].replace('250,000+','250000')
        df5[var] = df5[var].astype(float)
    
    df5.to_csv(output_folder + 'Chicago_ACS_Data.csv')
      
    return df5