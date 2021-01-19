import datetime
import pandas as pd

#define a function that cleans the float columns that have commas
def clean_float_cols(x):
    x = x.replace(',','')
    x = float(x)
    
    return x
	
#define a function that converts the raw Chicago ridee-hailing CSV file into a H5 file
def processRawData(infile, outfile, chunksize):
        """
        Reads data, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - infile name, raw CSV format
        outfile - output file name, h5 format
        outkey = name of table in output h5 file
        """
        
        print(datetime.datetime.now().ctime(), 'Converting raw data in file: ', infile, chunksize)
        
        # set up the reader
        reader = pd.read_csv(infile,  
                         iterator = True, 
                         sep = ',', 
        parse_dates = ['Trip Start Timestamp','Trip End Timestamp'], infer_datetime_format = True, chunksize = chunksize)

        # establish the writer and clear any table with that file name
        store = pd.HDFStore(outfile)

        # iterate through chunk by chunk so the computer doesn't run out of memory
        rowsRead    = 0
        rowsWritten_weekday = 0
        rowsWritten_weekend = 0
        
        for chunk in reader:   

            rowsRead += len(chunk)
            
            #convert columns from a string because it caused problems
            convert_cols = ['Trip Seconds','Trip Miles', 'Pickup Census Tract', 'Dropoff Census Tract','Fare', 
                            'Tip','Additional Charges', 'Trip Total', 'Trips Pooled']
        
            for column in convert_cols:
                chunk[column] = chunk[column].astype(str).apply(lambda x: x.replace(',',''))
                chunk[column] = chunk[column].astype(float)
           
            chunk['YEAR'], chunk['MONTH'], chunk['DOW'], chunk['HOUR'] = chunk['Trip Start Timestamp'].dt.year, chunk['Trip Start Timestamp'].dt.month, chunk['Trip Start Timestamp'].dt.weekday, chunk['Trip Start Timestamp'].dt.hour
            
            chunk_weekday = chunk[chunk['DOW'].isin([0,1,2,3,4])]
            chunk_weekend = chunk[chunk['DOW'].isin([5,6])]

            chunk_weekday_1 = chunk_weekday[chunk_weekday['HOUR'].isin([22,23,24,1,2,3,4,5])]
            chunk_weekday_2 = chunk_weekday[chunk_weekday['HOUR'].isin([6,7,8])]
            chunk_weekday_3 = chunk_weekday[chunk_weekday['HOUR'].isin([9,10,11,12,13,14,15])]
            chunk_weekday_4 = chunk_weekday[chunk_weekday['HOUR'].isin([16,17,18])]
            chunk_weekday_5 = chunk_weekday[chunk_weekday['HOUR'].isin([19,20,21])]

            chunk_weekend_1 = chunk_weekend[chunk_weekend['HOUR'].isin([22,23,24,1,2,3,4,5])]
            chunk_weekend_2 = chunk_weekend[chunk_weekend['HOUR'].isin([6,7,8])]
            chunk_weekend_3 = chunk_weekend[chunk_weekend['HOUR'].isin([9,10,11,12,13,14,15])]
            chunk_weekend_4 = chunk_weekend[chunk_weekend['HOUR'].isin([16,17,18])]
            chunk_weekend_5 = chunk_weekend[chunk_weekend['HOUR'].isin([19,20,21])]

            
            
            # write the data
            store.append('Weekday_1', chunk_weekday_1, data_columns = True)
            store.append('Weekday_2', chunk_weekday_2, data_columns = True)
            store.append('Weekday_3', chunk_weekday_3, data_columns = True)
            store.append('Weekday_4', chunk_weekday_4, data_columns = True)
            store.append('Weekday_5', chunk_weekday_5, data_columns = True)


            store.append('Weekend_1', chunk_weekend_1, data_columns = True)
            store.append('Weekend_2', chunk_weekend_2, data_columns = True)
            store.append('Weekend_3', chunk_weekend_3, data_columns = True)
            store.append('Weekend_4', chunk_weekend_4, data_columns = True)
            store.append('Weekend_5', chunk_weekend_5, data_columns = True)


            rowsWritten_weekday += len(chunk_weekday)
            rowsWritten_weekend += len(chunk_weekend)

            print ('Read %i rows and kept %i rows in weekday TNC table' % (rowsRead, rowsWritten_weekday))
            print ('kept ' + str(rowsWritten_weekend) + ' rows in weekend TNC table')

        store.close()
		
		


