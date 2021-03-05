#pull in all of the functions required by the script
from Process_Raw_RH_Data import processRawData
from Process_Raw_RH_Data import clean_float_cols
from Process_Suppressed_Trips import proccess_suppressed_trips
from Visualize_Suppressed_Trips import visualize_suppressed_trips
from Create_Empty_OD_Matrix import create_empty_od_matrix
from Create_Census_Tract_Centroids import create_census_tract_centroids
from Create_Conflation_File import create_conflation_file
from Format_Chi_RH_Data import format_chi_rh_data
from Agg_OTP_Transit_Travel_Times import agg_otp_transit_times
from Process_ACS_Data import process_acs_data 
from Process_Employment_Data import process_QCEW_Data 
from Process_Employment_Data import scale_LEHD_data
from Link_Data_Together import daily_agg
from Link_Data_Together import create_estimation_file_stats


import pandas as pd

#Step 1 Constants
INFILE_RAW_CSV = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Transportation_Network_Providers_-_Trips.csv'
H5_OUTFILE = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Chicago_TNC_Trips_20.H5'
CHUNKSIZE = 500000

#Step 2 Constants
OUTFILE_EMPTY_OD_MATRIX = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Empty_Chicago_Matrix.csv'
TODS = [1,2,3,4,5]
YEARS = [2018,2019,2020]

#Step 3 Constants
TRACTS_SHAPEFILE_PATH = 'D:/TNC-Demand-Model/Inputs/Census Shapefiles/Illinois/Chicago Tracts/geo_export_558aad9f-98d8-4dd5-a6b1-c1730155d596.shp'
OUTFILE_CENTROIDS = 'D:/TNC-Demand-Model/otp/points.csv'

COM_AREA_SHAPEFILE_PATH = 'D:/TNC-Demand-Model/Inputs/Chicago Community Areas/geo_export_d8da94d2-4ef1-4ee6-9fff-7bb20d451fe2.shp'
OUTFILE_CONFLATION = 'D:/TNC-Demand-Model/Inputs/Chicago Community Areas/Community_Area_to_Census_Tract.csv'

#Step 4 Constants
##uses the same tracts shapefile, tods, and years constant that step 2 uses
CONFLATION_PATH = 'D:/TNC-Demand-Model/Inputs/Chicago Community Areas/Community_Area_to_Census_Tract.csv'
H5_PATH = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Chicago_TNC_Trips_20.H5'
OUTFILE_SUPPRESSED_TRIPS = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Monthly Suppressed Trips.csv'
AGG_SUPPRESSED_TRIPS = {'SCALED_SUP_PRIVATE_TRIPS':'sum', 'SCALED_SUP_SHARED_TRIPS':'sum', 'Pickup Community Area':'first', 'Dropoff Community Area':'first'}

TRACTS_SHAPEFILE = 'D:/TNC-Demand-Model/Inputs/Census Shapefiles/Illinois/Chicago Tracts/geo_export_558aad9f-98d8-4dd5-a6b1-c1730155d596.shp'
TRACTS_CENTROIDS_FILE_PATH = 'D:/TNC-Demand-Model/otp/points.csv'
OUTFILE_SUPPRESSED_TRIPS_MAP = 'D:/TNC-Demand-Model/Data Exploration/Suppressed Ridehailing Maps/Suppressed_Trips.html'

#Step 5 Constants
##uses the same tracts shapefile, tods, and years constant that step 2 uses
### uses the same H5 filepath as step 4 and the outfiles of step 2 and 4 as infiles
AGG_FORMAT_CHI_RH = { 'Trip Seconds':'mean','Trip Miles':'mean', 'Fare':'mean', 'Tip':'mean', 'Additional Charges':'mean', 'Trip Total':'mean', 'PRIVATE_TRIPS':'sum','SHARED_TRIPS':'sum', 'Trips Pooled':'sum'}
OUTFILE_RAW_CHI_RH = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Raw_Trip_Records_No_Suppressed.csv'
OUTFILE_NO_SUPPRESSED_CHI_RH = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Monthly_Trip_Records_No_Suppressed.csv'
OUTFILE_CHI_RH = 'D:/TNC-Demand-Model/Outputs/2019_Weekday_Monthly_Ridehail_TOD.csv' 

#Step 6 Constants
##uses the same tracts shapefile, tods, and years constant that step 2 uses
OTP_TRANSIT_INPUT_FOLDERS = r'D:/TNC-Demand-Model/Outputs/OTP Travel Times/Transit/'
OUTPUT_FOLDER_PATH = 'D:/TNC-Demand-Model/Outputs/'

#Step 7 Constants
##uses the same tracts shapefile, tods, and years constant that step 2 uses
ACS_INPUT_FOLDER_PATH = 'D:/TNC-Demand-Model/Inputs/ACS/'
ACS_TABLES = ['DP05', 'S1101', 'S1501', 'S1901', 'DP04']

#Step 8 Constants
#uses the same years constant as step 2
#uses the same output folder path constant as step 6
QCEW_FOLDER_PATH = 'D:/TNC-Demand-Model/Inputs/QCEW'
LEHD_INPUT_FOLDER_PATH = 'D:/TNC-Demand-Model/Inputs/LEHD/'
CENSUS_BLOCKS_PATH = 'D:/TNC-Demand-Model/Inputs/Census Shapefiles/Illinois/Blocks/tl_2019_17_tabblock10.shp'
 
 
#Step 9 Constants
#uses the same output folder path constant as step 6
ESTIMATION_FILE_FOLDER_PATH = 'D:/TNC-Demand-Model/Outputs/'
AGG_RH = {'Trip Seconds':'mean', 'Trip Miles':'mean', 'Fare':'mean', 'Tip':'mean',
       'Additional Charges':'mean', 'Trip Total':'mean', 'PRIVATE_TRIPS':'sum', 'SHARED_TRIPS':'sum',
       'SCALED_SUP_PRIVATE_TRIPS':'sum', 'SCALED_SUP_SHARED_TRIPS':'sum', 'ALL_TRIPS':'sum'}
 
#Step 10 Constants
#uses the same output folder path constant as step 6
DROP_COLS = ['ORIGIN',
'DESTINATION',
'MONTH',
'CENSUS_TRACT_DESTINATION','TRACTCE10_PICKUP','TRACTCE10_DEST','TRACTCE10_RAC_ORIGIN','TRACTCE10_RAC_DESTINATION','TRACTCE10_WAC_ORIGIN','TRACTCE10_WAC_DESTINATION']

# main function call
if __name__ == "__main__":
    
    #set the aggregation methods for each data column
    
    print('STEP 1: PROCESSING RAW CHICAGO RH DATA!')
    #create the shared TNC trip h5 table
    #processRawData(INFILE_RAW_CSV, H5_OUTFILE, CHUNKSIZE)
    print('STEP 1 IS COMPLETE!')
    
    print('STEP 2: CREATING EMPTY OD MATRIX OF CHICAGO TRACTS!')
    #create_empty_od_matrix(TRACTS_SHAPEFILE, YEARS, TODS, OUTFILE_EMPTY_OD_MATRIX)
    print('STEP 2 IS COMPLETE!')

    print('STEP 3: FINDING TRACT CENTROIDS AND CREATE CONFLATION FILE FOR SUPPRESSED TRIPS!')
    #create_census_tract_centroids(TRACTS_SHAPEFILE_PATH, OUTFILE_CENTROIDS)
    #create_conflation_file(TRACTS_SHAPEFILE_PATH, COM_AREA_SHAPEFILE_PATH, OUTFILE_CONFLATION)
    print('STEP 3 IS COMPLETE!')
    
    print('STEP 4: PROCESSING AND VISUALIZING SUPPRESSED CHICAGO RH TRIPS!')
    #assign the suppressed trips to a census tract randomly based on which census tracts are within its assigned community area
    #proccess_suppressed_trips(CONFLATION_PATH, H5_PATH, OUTFILE_SUPPRESSED_TRIPS, YEARS, AGG_SUPPRESSED_TRIPS, TODS)
    
    #grouped = pd.read_csv(OUTFILE_SUPPRESSED_TRIPS)
    #visualize_suppressed_trips(grouped, TRACTS_SHAPEFILE, TRACTS_CENTROIDS_FILE_PATH, OUTFILE_SUPPRESSED_TRIPS_MAP)
    print('STEP 4 IS COMPLETE!')
    
    print('STEP 5: FORMATTING MONTHLY AVERAGE WEEKDAY CHICAGO RH DATA!')
    format_chi_rh_data(TODS, YEARS, AGG_FORMAT_CHI_RH, H5_PATH, OUTFILE_SUPPRESSED_TRIPS, OUTFILE_EMPTY_OD_MATRIX, OUTFILE_RAW_CHI_RH, OUTFILE_NO_SUPPRESSED_CHI_RH, OUTFILE_CHI_RH)
    print('STEP 5 IS COMPLETE!')
    
    print('STEP 6: AGGREGATING OTP TRANSIT TRAVEL TIME OUTPUTS!')
    #otp_transit = agg_otp_transit_times(TODS, YEARS, OTP_TRANSIT_INPUT_FOLDERS, OUTPUT_FOLDER_PATH)
    print('STEP 6 IS COMPLETE!')
    
    print('STEP 7: PROCESSING ACS DATA!')
    #acs = process_acs_data(TODS, YEARS, ACS_INPUT_FOLDER_PATH, ACS_TABLES, OUTPUT_FOLDER_PATH)
    print('STEP 7 IS COMPLETE!')
    
    print('STEP 8: PROCESSING EMPLOYMENT DATA!')
    #process_QCEW_Data(YEARS, QCEW_FOLDER_PATH, )
    #scale_LEHD_data(QCEW_FOLDER_PATH, LEHD_INPUT_FOLDER_PATH, CENSUS_BLOCKS_PATH, YEARS, OUTPUT_FOLDER_PATH)
    print('STEP 8 IS COMPLETE!')

    print('STEP 9: LINKING DATA TOGETHER TO CREATE ESTIMATION FILE!')
    est = daily_agg(ESTIMATION_FILE_FOLDER_PATH, AGG_RH)
    print('STEP 9 IS COMPLETE!')
    
    print('STEP 10: CALCULATE STATS FOR ESTIMATION FILE!')
    create_estimation_file_stats(DROP_COLS, est, OUTPUT_FOLDER_PATH)
    est_100 = est.head(100)
    est_100.to_csv('D:/TNC-Demand-Model/Outputs/Chi_Estimation_File (First 100 rows).csv')
    
    print('STEP 10 IS COMPLETE!')
