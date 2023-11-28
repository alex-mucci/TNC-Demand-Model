import geopandas as gp
import pandas as pd
import numpy as np

def create_census_tract_centroids(tracts_shapefile_path, outfile_path):
	tracts = gp.read_file(tracts_shapefile_path)
	origins = pd.DataFrame()
	origins['X'] = tracts.centroid.x
	origins['Y'] = tracts.centroid.y
	origins['GEOID'] = tracts.geoid10
	
	#had to change the centroid of this census tract because the true centroid is located in the middle of a schol and it messes up 
	#the transit travel time calculation
	origins.loc[origins['GEOID'] == '17031843700', 'X'] = -87.69079
	origins.loc[origins['GEOID'] == '17031843700', 'Y'] = 41.94482
	origins.loc[origins['GEOID'] == '17031540102', 'X'] = -87.60610
	origins.loc[origins['GEOID'] == '17031540102', 'Y'] = 41.65970
	origins.to_csv(outfile_path)
    
