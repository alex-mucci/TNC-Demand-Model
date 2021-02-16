import pandas as pd 
import numpy as np

def process_acs_data(year, folder_path, acs_table, keep, rename):
	df = pd.read_csv(folder_path + str(year) + '/' + acs_table + '/' + acs_table + '.csv')
	df[['TRACT', 'COUNTY', 'STATE']] = df.NAME.str.split(',', expand=True)
	df = df[df['COUNTY'] == ' Cook County']
	df['CENSUS_TRACT'] = df.apply(lambda row: row['GEO_ID'][9:], axis = 1)
	df = df[keep]
	df.columns = rename
	
	return df
	
