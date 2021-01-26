#pull in all of the functions required by the script
from Process_Raw_RH_Data import processRawData
from Process_Raw_RH_Data import clean_float_cols
from Process_Suppressed_Trips import proccess_suppressed_trips
from Visualize_Suppressed_Trips import visualize_suppressed_trips
from Create_Empty_OD_Matrix import create_empty_od_matrix
from Create_Census_Tract_Centroids import create_census_tract_centroids
from Create_Conflation_File import create_conflation_file
from Format_Chi_RH_Data import format_chi_rh_data

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
    create_census_tract_centroids(TRACTS_SHAPEFILE_PATH, OUTFILE_CENTROIDS)
    create_conflation_file(TRACTS_SHAPEFILE_PATH, COM_AREA_SHAPEFILE_PATH, OUTFILE_CONFLATION)
    print('STEP 3 IS COMPLETE!')
    
    print('STEP 4: PROCESSING AND VISUALIZING SUPPRESSED CHICAGO RH TRIPS!')
    #assign the suppressed trips to a census tract randomly based on which census tracts are within its assigned community area
    #proccess_suppressed_trips(CONFLATION_PATH, H5_PATH, OUTFILE_SUPPRESSED_TRIPS, YEARS, AGG_SUPPRESSED_TRIPS, TODS)
    
    #grouped = pd.read_csv(OUTFILE_SUPPRESSED_TRIPS)
    #visualize_suppressed_trips(grouped, TRACTS_SHAPEFILE, TRACTS_CENTROIDS_FILE_PATH, OUTFILE_SUPPRESSED_TRIPS_MAP)
    print('STEP 4 IS COMPLETE!')
    
    print('STEP 5: FORMATTING MONTHLY AVERAGE WEEKDAY CHICAGO RH DATA!')
    #format_chi_rh_data(TODS, YEARS, AGG_FORMAT_CHI_RH, H5_PATH, OUTFILE_SUPPRESSED_TRIPS, OUTFILE_EMPTY_OD_MATRIX, OUTFILE_RAW_CHI_RH, OUTFILE_NO_SUPPRESSED_CHI_RH, OUTFILE_CHI_RH)
    print('STEP 5 IS COMPLETE!')
    
    print('STEP 6: AGGREGATING OTP TRANSIT TRAVEL TIME OUTPUTS!')
    
    
    print('STEP 7: PROCESSING ACS DATA!')
    
    
    print('STEP 8: PROCESSING LEHD DATA!')


    print('STEP 9: PROCESSING GTFS DATA!')


    print('STEP 10: LINKING DATA TOGETHER TO CREATE ESTIMATION FILE!')

