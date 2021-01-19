#pull in all of the functions required by the script
from Process_Raw_RH_Data import processRawData
from Process_Raw_RH_Data import clean_float_cols
from Process_Suppressed_Trips import proccess_suppressed_trips
from Visualize_Suppressed_Trips import visualize_suppressed_trips
from Create_Empty_OD_Matrix import create_empty_od_matrix

import pandas as pd

#Step 1 Constants
INFILE_RAW_CSV = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Transportation_Network_Providers_-_Trips.csv'
H5_OUTFILE = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Chicago_TNC_Trips_20.H5'
CHUNKSIZE = 500000

#Step 2 Constants
CONFLATION_PATH = 'D:/TNC-Demand-Model/Inputs/Chicago Community Areas/Community_Area_to_Census_Tract.csv'
H5_PATH = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Chicago_TNC_Trips_20.H5'
OUTFILE_SUPPRESSED_TRIPS = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Monthly Suppressed Trips.csv'
TODS = [1,2,3,4,5]
YEARS = [2018,2019,2020]
AGG_SUPPRESSED_TRIPS = {'SCALED_SUP_PRIVATE_TRIPS':'sum', 'SCALED_SUP_SHARED_TRIPS':'sum', 'Pickup Community Area':'first', 'Dropoff Community Area':'first'}

TRACTS_SHAPEFILE = 'D:/TNC-Demand-Model/Inputs/Census Shapefiles/Chicago Tracts/geo_export_558aad9f-98d8-4dd5-a6b1-c1730155d596.shp'
TRACTS_CENTROIDS_FILE_PATH = 'D:/TNC-Demand-Model/otp/points.csv'
OUTFILE_SUPPRESSED_TRIPS_MAP = 'D:/TNC-Demand-Model/Data Exploration/Suppressed Ridehailing Maps/Suppressed_Trips.html'

#Step 3 Constants
##uses the same tracts shapefile, tods, and years constant that step 2 uses
OUTFILE_EMPTY_OD_MATRIX = 'D:/TNC-Demand-Model/Inputs/Chicago Ride-Hailing/Empty_Chicago_Matrix.csv'




# main function call
if __name__ == "__main__":
    
    #set the aggregation methods for each data column
    
    print('STEP 1: PROCESSING RAW CHICAGO RH DATA!')
    #create the shared TNC trip h5 table
    #processRawData(INFILE_RAW_CSV, H5_OUTFILE, CHUNKSIZE)
    print('STEP 1 IS COMPLETE!')
    
    print('STEP 2: PROCESSING SUPPRESSED CHICAGO RH TRIPS!')
    #assign the suppressed trips to a census tract randomly based on which census tracts are within its assigned community area
    proccess_suppressed_trips(CONFLATION_PATH, H5_PATH, OUTFILE_SUPPRESSED_TRIPS, YEARS, AGG_SUPPRESSED_TRIPS, TODS)
    
    grouped = pd.read_csv(OUTFILE_SUPPRESSED_TRIPS)
    visualize_suppressed_trips(grouped, TRACTS_SHAPEFILE, TRACTS_CENTROIDS_FILE_PATH, OUTFILE_SUPPRESSED_TRIPS_MAP)
    print('STEP 2 IS COMPLETE!')

    print('STEP 3: CREATING EMPTY OD MATRIX OF CHICAGO TRACTS!')
    create_empty_od_matrix(TRACTS_SHAPEFILE, YEARS, TODS, OUTFILE_EMPTY_OD_MATRIX)
    print('STEP 3 IS COMPLETE!')

    print('STEP 4: FINDING TRACT CENTROIDS AND CREATE CONFLATION FILE FOR SUPPRESSED TRIPS!')
## NEED TO MOVE THIS TO STEP 3, THEN STEP 3 TO STEP 2, THEN STEP 2 TO STEP 4

    print('STEP 5: FORMATTING MONTHLY AVERAGE WEEKDAY CHICAGO RH DATA!')
    
    
    print('STEP 6: AGGREGATING OTP TRANSIT TRAVEL TIME OUTPUTS!')
    
    
    print('STEP 7: PROCESSING ACS DATA!')
    
    
    print('STEP 8: PROCESSING LEHD DATA!')


    print('STEP 9: PROCESSING GTFS DATA!')


    print('STEP 10: LINKING DATA TOGETHER TO CREATE ESTIMATION FILE!')

