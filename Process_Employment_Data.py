import pandas as pd
import geopandas as gp
import numpy as np


def set_OD_industry_code(row):
    if row['industry_code'] in ['11','21','23','1013']:
        industry = 'SI01'
    elif row['industry_code'] in ['22','42','44-45','48-49']:
        industry = 'SI02'

    elif row['industry_code'] in ['51','52','56','54','55','56','61','62','71','72','81','92']:
        industry = 'SI03'

    else:
        industry = np.nan
    return industry
    
def set_RAC_WAC_industry_code(row):
    if row['industry_code'] == '11':
        industry = 'CNS01'
    elif row['industry_code'] == '21':
        industry = 'CNS02'

    elif row['industry_code'] == '22':
        industry = 'CNS03'
        
    elif row['industry_code'] == '23':
        industry = 'CNS04'
        
    elif row['industry_code'] == '1013':
        industry = 'CNS05'

    elif row['industry_code'] == '42':
        industry = 'CNS06'

    elif row['industry_code'] == '44-45':
        industry = 'CNS07'

    elif row['industry_code'] == '48-49':
        industry = 'CNS08'
        
    elif row['industry_code'] == '51':
        industry = 'CNS09'
        
    elif row['industry_code'] == '52':
        industry = 'CNS10'

    elif row['industry_code'] == '53':
        industry = 'CNS11'

    elif row['industry_code'] == '54':
        industry = 'CNS12'

    elif row['industry_code'] == '55':
        industry = 'CNS13'

    elif row['industry_code'] == '56':
        industry = 'CNS14'
        
    elif row['industry_code'] == '61':
        industry = 'CNS15'

    elif row['industry_code'] == '62':
        industry = 'CNS16'

    elif row['industry_code'] == '71':
        industry = 'CNS17'
        
    elif row['industry_code'] == '72':
        industry = 'CNS18'
        
    elif row['industry_code'] == '81':
        industry = 'CNS19'
        
    elif row['industry_code'] == '92':
        industry = 'CNS20'
        
    else:
        industry = np.nan
    return industry
    
    
def process_QCEW_Data(years, qcew_folder_path ):
    
    for year in years:
        print('Working on year ' + str(year))
        qcew_final_od = pd.DataFrame()
        qcew_final_od['year'] = year
        
        qcew_final_rac_wac = pd.DataFrame()
        qcew_final_rac_wac['year'] = year
        
        if year != 2020:
            qcew_final_od['month'] = [1,2,3,4,5,6,7,8,9,10,11,12]
            qcew_final_rac_wac['month'] = [1,2,3,4,5,6,7,8,9,10,11,12]

            qcew = pd.read_csv(qcew_folder_path + '/' + str(year) + '.q1-q4.by_area/' + str(year) + '.q1-q4 17031 Cook County, Illinois.csv')
            qcew['OD_INDUSTRY'] = qcew.apply(lambda row:set_OD_industry_code(row), axis = 1)
            qcew['RAC_WAC_INDUSTRY'] = qcew.apply(lambda row:set_RAC_WAC_industry_code(row), axis = 1)
            
            qcew_od = qcew[['year','qtr','OD_INDUSTRY','month1_emplvl','month2_emplvl','month3_emplvl']].groupby(by = ['year','qtr','OD_INDUSTRY'], as_index = False).sum()
            
            qcew_rac_wac = qcew[['year','qtr','RAC_WAC_INDUSTRY','month1_emplvl','month2_emplvl','month3_emplvl']].groupby(by = ['year','qtr','RAC_WAC_INDUSTRY'], as_index = False).sum()
            
            print('Processing the QCEW data for LEHD OD Data!')
            industries = qcew_od.OD_INDUSTRY.unique()
            for industry in industries:
                qcew_od_selected = qcew_od[qcew_od['OD_INDUSTRY'] == industry]

                month1 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month1_emplvl
                month2 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month2_emplvl
                month3 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month3_emplvl
                month4 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month1_emplvl
                month5 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month2_emplvl
                month6 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month3_emplvl
                month7 = qcew_od_selected[qcew_od_selected['qtr'] == 3].month1_emplvl
                month8 = qcew_od_selected[qcew_od_selected['qtr'] == 3].month2_emplvl
                month9 = qcew_od_selected[qcew_od_selected['qtr'] == 3].month3_emplvl
                month10 = qcew_od_selected[qcew_od_selected['qtr'] == 4].month1_emplvl
                month11 = qcew_od_selected[qcew_od_selected['qtr'] == 4].month2_emplvl
                month12 = qcew_od_selected[qcew_od_selected['qtr'] == 4].month3_emplvl

                qcew_final_od.loc[0,industry] = month1.iloc[0]
                qcew_final_od.loc[1,industry] = month2.iloc[0]
                qcew_final_od.loc[2,industry] = month3.iloc[0]
                qcew_final_od.loc[3,industry] = month4.iloc[0]
                qcew_final_od.loc[4,industry] = month5.iloc[0]
                qcew_final_od.loc[5,industry] = month6.iloc[0]
                qcew_final_od.loc[6,industry] = month7.iloc[0]
                qcew_final_od.loc[7,industry] = month8.iloc[0]
                qcew_final_od.loc[8,industry] = month9.iloc[0]
                qcew_final_od.loc[9,industry] = month10.iloc[0]
                qcew_final_od.loc[10,industry] = month11.iloc[0]
                qcew_final_od.loc[11,industry] = month12.iloc[0]
            
            
            
            print('Processing the QCEW data for LEHD RAC/WAC Data!')
            industries = qcew_rac_wac['RAC_WAC_INDUSTRY'].unique()
            for industry in industries:
                qcew_rac_wac_selected = qcew_rac_wac[qcew_rac_wac['RAC_WAC_INDUSTRY'] == industry]
                month1 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month1_emplvl
                month2 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month2_emplvl
                month3 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month3_emplvl
                month4 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month1_emplvl
                month5 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month2_emplvl
                month6 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month3_emplvl
                month7 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 3].month1_emplvl
                month8 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 3].month2_emplvl
                month9 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 3].month3_emplvl
                month10 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 4].month1_emplvl
                month11 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 4].month2_emplvl
                month12 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 4].month3_emplvl

                qcew_final_rac_wac.loc[0,industry] = month1.iloc[0]
                qcew_final_rac_wac.loc[1,industry] = month2.iloc[0]
                qcew_final_rac_wac.loc[2,industry] = month3.iloc[0]
                qcew_final_rac_wac.loc[3,industry] = month4.iloc[0]
                qcew_final_rac_wac.loc[4,industry] = month5.iloc[0]
                qcew_final_rac_wac.loc[5,industry] = month6.iloc[0]
                qcew_final_rac_wac.loc[6,industry] = month7.iloc[0]
                qcew_final_rac_wac.loc[7,industry] = month8.iloc[0]
                qcew_final_rac_wac.loc[8,industry] = month9.iloc[0]
                qcew_final_rac_wac.loc[9,industry] = month10.iloc[0]
                qcew_final_rac_wac.loc[10,industry] = month11.iloc[0]
                qcew_final_rac_wac.loc[11,industry] = month12.iloc[0]
            
            
        else:
        
            qcew_final_od['month'] = [1,2,3,4,5,6]
            qcew_final_rac_wac['month'] = [1,2,3,4,5,6]

            qcew = pd.read_csv(qcew_folder_path + '/' + str(year) + '.q1-q2.by_area/' + str(year) + '.q1-q2 17031 Cook County, Illinois.csv')
            qcew['OD_INDUSTRY'] = qcew.apply(lambda row:set_OD_industry_code(row), axis = 1)
            qcew['RAC_WAC_INDUSTRY'] = qcew.apply(lambda row:set_RAC_WAC_industry_code(row), axis = 1)

            
            qcew_od = qcew[['year','qtr','OD_INDUSTRY','month1_emplvl','month2_emplvl','month3_emplvl']].groupby(by = ['year','qtr','OD_INDUSTRY'], as_index = False).sum()
            qcew_rac_wac = qcew[['year','qtr','RAC_WAC_INDUSTRY','month1_emplvl','month2_emplvl','month3_emplvl']].groupby(by = ['year','qtr','RAC_WAC_INDUSTRY'], as_index = False).sum()
            
            print('Processing the QCEW data for LEHD OD Data!')
            industries = qcew_od.OD_INDUSTRY.unique()
            for industry in industries:
                qcew_od_selected = qcew_od[qcew_od['OD_INDUSTRY'] == industry]

                month1 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month1_emplvl
                month2 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month2_emplvl
                month3 = qcew_od_selected[qcew_od_selected['qtr'] == 1].month3_emplvl
                month4 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month1_emplvl
                month5 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month2_emplvl
                month6 = qcew_od_selected[qcew_od_selected['qtr'] == 2].month3_emplvl

                qcew_final_od.loc[0,industry] = month1.iloc[0]
                qcew_final_od.loc[1,industry] = month2.iloc[0]
                qcew_final_od.loc[2,industry] = month3.iloc[0]
                qcew_final_od.loc[3,industry] = month4.iloc[0]
                qcew_final_od.loc[4,industry] = month5.iloc[0]
                qcew_final_od.loc[5,industry] = month6.iloc[0]

            print('Processing the QCEW data for LEHD RAC/WAC Data!')
            industries = qcew_rac_wac.RAC_WAC_INDUSTRY.unique()
            for industry in industries:
                qcew_rac_wac_selected = qcew_rac_wac[qcew_rac_wac['RAC_WAC_INDUSTRY'] == industry]

                month1 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month1_emplvl
                month2 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month2_emplvl
                month3 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 1].month3_emplvl
                month4 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month1_emplvl
                month5 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month2_emplvl
                month6 = qcew_rac_wac_selected[qcew_rac_wac_selected['qtr'] == 2].month3_emplvl

                qcew_final_rac_wac.loc[0,industry] = month1.iloc[0]
                qcew_final_rac_wac.loc[1,industry] = month2.iloc[0]
                qcew_final_rac_wac.loc[2,industry] = month3.iloc[0]
                qcew_final_rac_wac.loc[3,industry] = month4.iloc[0]
                qcew_final_rac_wac.loc[4,industry] = month5.iloc[0]
                qcew_final_rac_wac.loc[5,industry] = month6.iloc[0]
        
        qcew_final_od['year'] = year
        qcew_final_rac_wac['year'] = year

        qcew_final_od.to_csv(qcew_folder_path + '/' + str(year) + '_PROCESSED_QCEW_OD.csv')
        qcew_final_rac_wac.to_csv(qcew_folder_path + '/' + str(year) + '_PROCESSED_QCEW_RAC_WAC.csv')

    
   
def scale_LEHD_data(qcew_input_folder_path, lehd_input_folder_path, census_blocks_path, years, output_folder_path):
    print('Scaling the LEHD RAC/WAC Data Using the QCEW Data!')
    qcew18 = pd.read_csv(qcew_input_folder_path + '/2018_PROCESSED_QCEW_RAC_WAC.csv')
    qcew19 = pd.read_csv(qcew_input_folder_path + '/2019_PROCESSED_QCEW_RAC_WAC.csv')
    qcew20 = pd.read_csv(qcew_input_folder_path + '/2020_PROCESSED_QCEW_RAC_WAC.csv')
    
    qcew = qcew18.append(qcew19)
    qcew = qcew.append(qcew20)
    
    wac = pd.read_csv(lehd_input_folder_path + '/Illinois/il_wac_S000_JT00_2018.csv')
    rac = pd.read_csv(lehd_input_folder_path + '/Illinois/il_rac_S000_JT00_2018.csv')
    
    blocks = gp.read_file(census_blocks_path)
    cook_blocks = blocks[blocks['COUNTYFP10'] == '031']
    
    wac['w_geocode'] = wac['w_geocode'].astype(str)
    rac['h_geocode'] = rac['h_geocode'].astype(str)

    wac_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(wac, how = 'inner', left_on = 'GEOID10', right_on = 'w_geocode')
    rac_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(rac, how = 'inner', left_on = 'GEOID10', right_on = 'h_geocode')

    wac_geo_grouped = wac_geo.groupby(by = 'TRACTCE10', as_index = False).sum()
    rac_geo_grouped = rac_geo.groupby(by = 'TRACTCE10', as_index = False).sum()


    rac_final = pd.DataFrame()
    wac_final = pd.DataFrame()
    scaled_cols = ['CNS01', 'CNS02', 'CNS03', 'CNS04', 'CNS05', 'CNS06', 'CNS07', 'CNS08', 'CNS09', 'CNS10', 'CNS11', 'CNS12', 'CNS13', 'CNS14', 'CNS15', 'CNS16', 'CNS17', 'CNS18', 'CNS19', 'CNS20']

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
                print('Working on column ' + col)
                scalar_qcew = qcew[(qcew['year'] == year)&(qcew['month'] == month)][col]
                rac_scalar = scalar_qcew/rac_geo_grouped[col].sum()
                wac_scalar = scalar_qcew/wac_geo_grouped[col].sum()
                df2['YEAR'] = year
                df2['MONTH'] = month
                          
                df3['YEAR'] = year
                df3['MONTH'] = month
                
                df2['TRACTCE10'] = wac_geo_grouped['TRACTCE10']
                df3['TRACTCE10'] = rac_geo_grouped['TRACTCE10']
                df2[col] = wac_geo_grouped[col].apply(lambda x: x * wac_scalar)
                df3[col] = rac_geo_grouped[col].apply(lambda x: x * rac_scalar)

            rac_final = rac_final.append(df3)
            wac_final = wac_final.append(df2)
            
    wac_final.to_csv(output_folder_path + 'CHI_WAC.csv')
    rac_final.to_csv(output_folder_path + 'CHI_RAC.csv')
    
    
    print('Scaling the LEHD OD Data!')
    od = pd.read_csv(lehd_input_folder_path + '/Illinois/il_od_main_JT00_2018.csv')
    
    qcew18 = pd.read_csv(qcew_input_folder_path + '/2018_PROCESSED_QCEW_OD.csv')
    qcew19 = pd.read_csv(qcew_input_folder_path + '/2019_PROCESSED_QCEW_OD.csv')
    qcew20 = pd.read_csv(qcew_input_folder_path + '/2020_PROCESSED_QCEW_OD.csv')
    
    qcew = qcew18.append(qcew19)
    qcew = qcew.append(qcew20)
    
    od['h_geocode'] = od['h_geocode'].astype(str)
    od['w_geocode'] = od['w_geocode'].astype(str)
    
    od_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(od, how = 'inner', left_on = 'GEOID10', right_on = 'h_geocode')
    od_geo = cook_blocks[['geometry','GEOID10','TRACTCE10']].merge(od_geo, how = 'inner', left_on = 'GEOID10', right_on = 'w_geocode', suffixes =('','_DESTINATION'))
    
    od_geo_grouped = od_geo.groupby(by = ['TRACTCE10', 'TRACTCE10_DESTINATION'], as_index = False).sum()
    
    od_scaled_cols = ['SI01', 'SI02', 'SI03']
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

            for col in od_scaled_cols: 
                print('Working on column ' + col)
                scalar_qcew = qcew[(qcew['year'] == year)&(qcew['month'] == month)][col]
                scalar = scalar_qcew/od_geo_grouped[col].sum()
                
                df2['YEAR'] = year
                df2['MONTH'] = month
                df2['TRACTCE10_PICKUP'] = od_geo_grouped['TRACTCE10'] 
                df2['TRACTCE10_DEST'] = od_geo_grouped['TRACTCE10_DESTINATION'] 
                df2[col] = od_geo_grouped[col].apply(lambda x: x * scalar)

                    
            od_final = od_final.append(df2)
    
    od_final.to_csv(output_folder_path + 'CHI_LEHD_OD.csv')
                          
