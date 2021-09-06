import pandas as pd
import geopandas as gp

def find_mean(column):
    column = pd.to_numeric(column)
    output = column.mean()
    
    return output

def find_min(column):
    column = pd.to_numeric(column)
    output = column.min()
    
    return output

def find_max(column):
    column = pd.to_numeric(column)
    output = column.max()
    
    return output
    
def create_estimation_file_stats(drop_cols, estimation_file, output_folder_path):
    estimation_file.drop(drop_cols, axis = 1, inplace = True)
    estimation_file.replace('-', '0', inplace = True)
    estimation_file.replace('250,000+', '250000', inplace = True)
    df = pd.DataFrame()
    df['Mean'] = estimation_file.apply(lambda column: find_mean(column), axis = 0)
    df['Min'] = estimation_file.apply(lambda column: find_min(column), axis = 0)
    df['Max'] = estimation_file.apply(lambda column: find_max(column), axis = 0)
    df.to_csv(output_folder_path + 'Descriptive Statistics of Chi Estimation File.csv')
    
    
def daily_agg(estimation_file_folder_path):

    acs = pd.read_csv(estimation_file_folder_path + 'Chicago_ACS_Data.csv')
    lehd_od = pd.read_csv(estimation_file_folder_path + 'CHI_LEHD_OD.csv', index_col = 0)
    lehd_rac = pd.read_csv(estimation_file_folder_path + 'CHI_RAC.csv', index_col = 0)
    lehd_wac = pd.read_csv(estimation_file_folder_path + 'CHI_WAC.csv', index_col = 0)
    trans_otp = pd.read_csv(estimation_file_folder_path + 'OTP_Transit_Travel_Times.csv')
    ridehail = pd.read_csv(estimation_file_folder_path + 'Monthly_Ridehail_Trips.csv')
    tax = pd.read_csv(estimation_file_folder_path + 'CHI_TAX.csv', index_col = 0)
    auto_otp = pd.read_csv(estimation_file_folder_path + 'OTP_Auto_Travel_Times.csv')
    
    #aggregate some of the files from TOD to daily
    #ridehail = ridehail.groupby(by = ['YEAR','MONTH','ORIGIN','DESTINATION'], as_index = False).mean()
    acs = acs.groupby(by = ['YEAR','MONTH','CENSUS_TRACT'], as_index = False).mean()
    trans_otp = trans_otp.groupby(by = ['YEAR', 'MONTH','origin','destination'],as_index = False).mean()
    auto_otp = auto_otp.groupby(by = ['origin','destination'],as_index = False).mean()
    
    #drop some of the useless columns
    ridehail = ridehail.drop(['TOD','Unnamed: 0_x','Pickup Community Area','Dropoff Community Area'], axis = 1)
    auto_otp = auto_otp.drop('year', axis = 1)
    acs = acs.drop(['Unnamed: 0','TOD'], axis = 1)
    trans_otp = trans_otp.drop(['Unnamed: 0','year','TOD'], axis = 1)


    est = ridehail.merge(acs, how = 'left', left_on = ['YEAR', 'MONTH', 'ORIGIN'], right_on = ['YEAR', 'MONTH', 'CENSUS_TRACT'], suffixes = ('','_ORIGIN'))
    est = est.merge(acs, how = 'left', left_on = ['YEAR', 'MONTH', 'DESTINATION'], right_on = ['YEAR', 'MONTH', 'CENSUS_TRACT'], suffixes = ('','_DESTINATION'))
    est = est.drop(['CENSUS_TRACT','CENSUS_TRACT_DESTINATION'], axis = 1)

    est['TRACTCE10_PICKUP'] = est.ORIGIN.astype(str)
    est['TRACTCE10_PICKUP'] = est['TRACTCE10_PICKUP'].apply(lambda x : x[5:11])
    est['TRACTCE10_PICKUP'] = est['TRACTCE10_PICKUP'].astype(int)
    est['TRACTCE10_DEST'] = est.DESTINATION.astype(str)
    est['TRACTCE10_DEST'] = est['TRACTCE10_DEST'].apply(lambda x : x[5:11])
    est['TRACTCE10_DEST'] = est['TRACTCE10_DEST'].astype(int)

    est = est.merge(lehd_od, how = 'left', on = ['YEAR', 'MONTH','TRACTCE10_PICKUP', 'TRACTCE10_DEST'], validate = 'one_to_one', suffixes = ('', '_LEHD_OD'))
    
    est['SI01'] = est['SI01'].fillna(0)
    est['SI02'] = est['SI02'].fillna(0)
    est['SI03'] = est['SI03'].fillna(0)

    est = est.merge(lehd_rac, how = 'left', right_on = ['YEAR', 'MONTH','TRACTCE10'], left_on = ['YEAR','MONTH','TRACTCE10_PICKUP'], suffixes = ('', '_LEHD_RAC'))
    est = est.merge(lehd_rac, how = 'left', right_on = ['YEAR', 'MONTH','TRACTCE10'], left_on = ['YEAR','MONTH', 'TRACTCE10_DEST'], suffixes = ('_RAC_ORIGIN', '_RAC_DESTINATION'))

    est = est.merge(lehd_wac, how = 'left', right_on = ['YEAR', 'MONTH', 'TRACTCE10'], left_on = ['YEAR','MONTH', 'TRACTCE10_PICKUP'], suffixes = ('', '_LEHD_WAC'))
    est = est.merge(lehd_wac, how = 'left', right_on = ['YEAR', 'MONTH', 'TRACTCE10'], left_on = ['YEAR','MONTH','TRACTCE10_DEST'], suffixes = ('_WAC_ORIGIN', '_WAC_DESTINATION'))
    
    est = est.drop(['TRACTCE10_PICKUP','TRACTCE10_DEST','TRACTCE10_WAC_ORIGIN','TRACTCE10_WAC_DESTINATION','TRACTCE10_RAC_ORIGIN','TRACTCE10_RAC_DESTINATION'], axis = 1)

    fill_cols = lehd_wac.columns.drop(['YEAR','MONTH','TRACTCE10'])
    
    for col in fill_cols:
        col = col + '_WAC_ORIGIN'
        est[col] = est[col].fillna(0)
    
    for col in fill_cols:
        col = col + '_WAC_DESTINATION'
        est[col] = est[col].fillna(0)
        
    est = est.merge(trans_otp, how = 'left', right_on = ['YEAR', 'MONTH', 'origin','destination'], left_on = ['YEAR','MONTH','ORIGIN','DESTINATION'], validate = 'one_to_one', suffixes = ('', '_TRANS_OTP'))

    est = est.merge(auto_otp, how = 'left', right_on = ['origin','destination'], left_on = ['ORIGIN','DESTINATION'], suffixes = ('', '_AUTO_OTP'))

    est = est.merge(tax, how = 'left', left_on = ['YEAR','MONTH','ORIGIN','DESTINATION'], right_on = ['YEAR','MONTH','ORIGIN','DESTINATION'])
    
    est['TOTAL_JOBS_ORIGIN'] = est['CNS01_WAC_ORIGIN'] +est['CNS02_WAC_ORIGIN']+est['CNS03_WAC_ORIGIN']+est['CNS04_WAC_ORIGIN']+est['CNS05_WAC_ORIGIN']+est['CNS06_WAC_ORIGIN']+est['CNS07_WAC_ORIGIN']+est['CNS08_WAC_ORIGIN']+est['CNS09_WAC_ORIGIN']+est['CNS10_WAC_ORIGIN']+est['CNS11_WAC_ORIGIN']+est['CNS12_WAC_ORIGIN']+est['CNS13_WAC_ORIGIN']+est['CNS14_WAC_ORIGIN']+est['CNS15_WAC_ORIGIN']+est['CNS16_WAC_ORIGIN']+est['CNS17_WAC_ORIGIN']+est['CNS18_WAC_ORIGIN']+est['CNS19_WAC_ORIGIN']+est['CNS20_WAC_ORIGIN']
    est['TOTAL_JOBS_DESTINATION'] = est['CNS01_WAC_DESTINATION'] +est['CNS02_WAC_DESTINATION']+est['CNS03_WAC_DESTINATION']+est['CNS04_WAC_DESTINATION']+est['CNS05_WAC_DESTINATION']+est['CNS06_WAC_DESTINATION']+est['CNS07_WAC_DESTINATION']+est['CNS08_WAC_DESTINATION']+est['CNS09_WAC_DESTINATION']+est['CNS10_WAC_DESTINATION']+est['CNS11_WAC_DESTINATION']+est['CNS12_WAC_DESTINATION']+est['CNS13_WAC_DESTINATION']+est['CNS14_WAC_DESTINATION']+est['CNS15_WAC_DESTINATION']+est['CNS16_WAC_DESTINATION']+est['CNS17_WAC_DESTINATION']+est['CNS18_WAC_DESTINATION']+est['CNS19_WAC_DESTINATION']+est['CNS20_WAC_DESTINATION']
    
    est['TOTAL_WORKERS_ORIGIN'] = est['CNS01_RAC_ORIGIN']+est['CNS02_RAC_ORIGIN']+est['CNS03_RAC_ORIGIN']+est['CNS04_RAC_ORIGIN']+est['CNS05_RAC_ORIGIN']+est['CNS06_RAC_ORIGIN']+est['CNS07_RAC_ORIGIN']+est['CNS08_RAC_ORIGIN']+est['CNS09_RAC_ORIGIN']+est['CNS10_RAC_ORIGIN']+est['CNS11_RAC_ORIGIN']+est['CNS12_RAC_ORIGIN']+est['CNS13_RAC_ORIGIN']+est['CNS14_RAC_ORIGIN']+est['CNS15_RAC_ORIGIN']+est['CNS16_RAC_ORIGIN']+est['CNS17_RAC_ORIGIN']+est['CNS18_RAC_ORIGIN']+est['CNS19_RAC_ORIGIN']+est['CNS20_RAC_ORIGIN']
    est['TOTAL_WORKERS_DESTINATION'] = est['CNS01_RAC_DESTINATION']+est['CNS02_RAC_DESTINATION']+est['CNS03_RAC_DESTINATION']+est['CNS04_RAC_DESTINATION']+est['CNS05_RAC_DESTINATION']+est['CNS06_RAC_DESTINATION']+est['CNS07_RAC_DESTINATION']+est['CNS08_RAC_DESTINATION']+est['CNS09_RAC_DESTINATION']+est['CNS10_RAC_DESTINATION']+est['CNS11_RAC_DESTINATION']+est['CNS12_RAC_DESTINATION']+est['CNS13_RAC_DESTINATION']+est['CNS14_RAC_DESTINATION']+est['CNS15_RAC_DESTINATION']+est['CNS16_RAC_DESTINATION']+est['CNS17_RAC_DESTINATION']+est['CNS18_RAC_DESTINATION']+est['CNS19_RAC_DESTINATION']+est['CNS20_RAC_DESTINATION']
    
    est.to_csv(estimation_file_folder_path + 'Chi_Estimation_File.csv')
    
    
    return est