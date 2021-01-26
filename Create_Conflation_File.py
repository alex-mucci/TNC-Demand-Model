import geopandas as gp
import pandas as pd
import numpy as np


def create_conflation_file(tracts_shapefile_path, com_area_shapefile_path, outfile_path):
	tracts = gp.read_file(tracts_shapefile_path)
	centroids = gp.GeoDataFrame(tracts.centroid)
	centroids['GEOID'] = tracts.geoid10
	centroids.columns = ['geometry','GEOID']
    
	com_area = gp.read_file(com_area_shapefile_path)
	conflate = gp.sjoin(com_area[['area_num_1','geometry']], centroids[['GEOID','geometry']], how = 'left', op = 'contains' )
	conflate = conflate.drop(columns = ['geometry', 'index_right'])
	add_ons = {'area_num_1':[ 53, 75, 43,43,43, 39, 41, 41, 41, 36, 8, 76, 76, 10, 1, 8], 'GEOID':[17031821402, 17031823304, 17031431400, 17031430700, 17031430102, 17031390700, 17031411000, 17031410900, 17031410100, 17031836400,17031081403, 17031770602, 17031770902, 17031810400, 17031010400, 17031081202 ]} 
	add_on_df = pd.DataFrame(add_ons)
	conflate = conflate.append(add_on_df)
	conflate.to_csv(outfile_path)
