import pandas as pd
import geopandas as gp
import numpy as np


def process_QCEW_Data(years, input_folder_path, output_folder_path):
	qcew_final = pd.DataFrame()
	cew_final['month'] = [1,2,3,4,5,6,7,8,9,10,11,12]
	qcew_final['EMP'] = np.nan
	
	for year in years:
    print('Working on year ' + str(year))
    qcew_final['year'] = year

    if year != 2020:
        qcew = pd.read_csv(input_folder_path + '/' + str(year) + '.q1-q4.by_area/' + str(year) + '.q1-q4 17031 Cook County, Illinois.csv')

        qcew = qcew[0:4][['year','qtr','month1_emplvl','month2_emplvl','month3_emplvl']]

        month1 = qcew[qcew['qtr'] == 1].month1_emplvl
        month2 = qcew[qcew['qtr'] == 1].month2_emplvl
        month3 = qcew[qcew['qtr'] == 1].month3_emplvl
        month4 = qcew[qcew['qtr'] == 2].month1_emplvl
        month5 = qcew[qcew['qtr'] == 2].month2_emplvl
        month6 = qcew[qcew['qtr'] == 2].month3_emplvl
        month7 = qcew[qcew['qtr'] == 3].month1_emplvl
        month8 = qcew[qcew['qtr'] == 3].month2_emplvl
        month9 = qcew[qcew['qtr'] == 3].month3_emplvl
        month10 = qcew[qcew['qtr'] == 4].month1_emplvl
        month11 = qcew[qcew['qtr'] == 4].month2_emplvl
        month12 = qcew[qcew['qtr'] == 4].month3_emplvl

        qcew_final.loc[0,'EMP'] = month1.loc[0]
        qcew_final.loc[1,'EMP'] = month2.loc[0]
        qcew_final.loc[2,'EMP'] = month3.loc[0]
        qcew_final.loc[3,'EMP'] = month4.loc[1]
        qcew_final.loc[4,'EMP'] = month5.loc[1]
        qcew_final.loc[5,'EMP'] = month6.loc[1]
        qcew_final.loc[6,'EMP'] = month7.loc[2]
        qcew_final.loc[7,'EMP'] = month8.loc[2]
        qcew_final.loc[8,'EMP'] = month9.loc[2]
        qcew_final.loc[9,'EMP'] = month10.loc[3]
        qcew_final.loc[10,'EMP'] = month11.loc[3]
        qcew_final.loc[11,'EMP'] = month12.loc[3]
        
        
    else:
        qcew = pd.read_csv(input_folder_path '/' + str(year) + '.q1-q2.by_area/' + str(year) + '.q1-q2 17031 Cook County, Illinois.csv')

        
        qcew = qcew[0:4][['year','qtr','month1_emplvl','month2_emplvl','month3_emplvl']]

        month1 = qcew[qcew['qtr'] == 1].month1_emplvl
        month2 = qcew[qcew['qtr'] == 1].month2_emplvl
        month3 = qcew[qcew['qtr'] == 1].month3_emplvl
        month4 = qcew[qcew['qtr'] == 2].month1_emplvl
        month5 = qcew[qcew['qtr'] == 2].month2_emplvl
        month6 = qcew[qcew['qtr'] == 2].month3_emplvl

        qcew_final.loc[0,'EMP'] = month1.loc[0]
        qcew_final.loc[1,'EMP'] = month2.loc[0]
        qcew_final.loc[2,'EMP'] = month3.loc[0]
        qcew_final.loc[3,'EMP'] = month4.loc[1]
        qcew_final.loc[4,'EMP'] = month5.loc[1]
        qcew_final.loc[5,'EMP'] = month6.loc[1]

    qcew_final.to_csv(output_folder_path + '/' + str(year) + '_PROCESSED_QCEW.csv')
    
    
   
def scale_LEHD_data(qcew_input_folder_path, lehd_input_folder_path, census_blocks_path, years, agg, wac_output_path, rac_output_path, od_output_path):
    print('Scaling the LEHD Data Using the QCEW Data!')
    qcew18 = pd.read_csv(qcew_input_folder_path + '2018_PROCESSED_QCEW.csv')
    qcew19 = pd.read_csv(qcew_input_folder_path + '/2018_PROCESSED_QCEW.csv')
    qcew20 = pd.read_csv(qcew_input_folder_path + '/2018_PROCESSED_QCEW.csv')
    
    scalar_18 = qcew18.EMP.mean()

    qcew18['SCALAR'] = qcew18['EMP'] / scalar_18
    qcew19['SCALAR'] = qcew19['EMP'] / scalar_18
    qcew20['SCALAR'] = qcew20['EMP'] / scalar_18
    
    qcew = qcew18.append(qcew19)
    qcew = qcew.append(qcew20)
    
    wac = pd.read_csv(lehd_input_folder_path + '/Illinois/il_wac_S000_JT00_2018.csv')
    rac = pd.read_csv(lehd_input_folder_path + '/Illinois/il_rac_S000_JT00_2018.csv')
    
    
    blocks = gp.read_file(census_blocks_path)
    cook_blocks = blocks[blocks['COUNTYFP10'] == '031']
    
    wac['w_geocode'] = wac['w_geocode'].astype(str)
    rac['h_geocode'] = rac['h_geocode'].astype(str)
    od['h_geocode'] = od['h_geocode'].astype(str)
    od['w_geocode'] = od['w_geocode'].astype(str)
    
    wac_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(wac, how = 'inner', left_on = 'GEOID10', right_on = 'w_geocode')
    rac_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(rac, how = 'inner', left_on = 'GEOID10', right_on = 'h_geocode')
    od_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(od, how = 'inner', left_on = 'GEOID10', right_on = 'h_geocode')
    od_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(od_geo, how = 'inner', left_on = 'GEOID10', right_on = 'w_geocode', suffixes =('','_DESTINATION'))
    
    od_geo_grouped = od_geo.groupby(by = ['TRACTCE10', 'TRACTCE10_DESTINATION'], as_index = False).agg(agg)
    wac_geo_grouped = wac_geo.groupby(by = 'TRACTCE10', as_index = False).sum()
    rac_geo_grouped = rac_geo.groupby(by = 'TRACTCE10', as_index = False).sum()


    rac_final = pd.DataFrame()
    wac_final = pd.DataFrame()
    scaled_cols = ['C000','CA01' ,'CA02' , 'CA03', 'CE01' , 'CE02', 'CE03', 'CNS18']

    for year in years:
        print('Working on year ' + str(year))
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        else:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]

        for month in months:
            print('Working on month ' + str(month))
            df2 = pd.DataFrame()
            df3 = pd.DataFrame()
        
            for col in scaled_cols: 

                scalar = qcew[(qcew['year'] == year)&(qcew['month'] == month)]['SCALAR']

                df2['YEAR'] = year
                df2['MONTH'] = month
                          
                df3['YEAR'] = year
                df3['MONTH'] = month
                
                df2['TRACTCE10'] = wac_geo_grouped['TRACTCE10']
                df3['TRACTCE10'] = rac_geo_grouped['TRACTCE10']
                
                df2[col] = wac_geo_grouped[col] * scalar.values[0]
                df3[col] = rac_geo_grouped[col] * scalar.values[0]
                

            rac_final = rac_final.append(df3)
            wac_final = wac_final.append(df2)
            
    wac_final.to_csv(wac_output_path)
    rac_final.to_csv(rac_output_path)
    
    
    print('Scaling the LEHD OD Data!')
    od = pd.read_csv(lehd_input_folder_path + '/Illinois/il_od_main_JT00_2018.csv')
    
    od_scaled_cols = ['S000', 'SA01', 'SA02', 'SA03', 'SE01', 'SE02', 'SE03']
    od_final = pd.DataFrame()

    for year in years:
        print('Working on year ' + str(year))
        if year == 2018:
            months = [11,12]
        elif year == 2020:
            months = [1,2]
        else:
            months = [1,2,3,4,5,6,7,8,9,10,11,12]

        for month in months:
            print('Working on month ' + str(month))
            df2 = pd.DataFrame()
            scalar = qcew[(qcew['year'] == year)&(qcew['month'] == month)]['SCALAR']

            for col in od_scaled_cols: 
                for col in od_scaled_cols:
                    df2['YEAR'] = year
                    df2['MONTH'] = month
                    df2['TRACTCE10_PICKUP'] = od_geo_grouped['TRACTCE10'] 
                    df2['TRACTCE10_DEST'] = od_geo_grouped['TRACTCE10_DESTINATION'] 
                    df2[col] = od_geo_grouped[col] * scalar.values[0]

                    
            od_final = od_final.append(df2)
    
    od_final.to_csv(od_output_path)
                          
