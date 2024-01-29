# todo: python script created from data-upload-green-taxi.ipynb to load taxi+_zone_lookup.csv--> green_taxi_data and green_tripdata_2019-09.csv-->green_taxi_zone_lookup
# todo: import pandas ,create_engine from sqlalchemy to create connection and time to benchmark loadtime
from pickle import TRUE
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    # todo: accept parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_zone = params.table_name_zone
    table_name_green = params.table_name_green
    url_zone = params.url_zone
    url_green = params.url_green
    csv_name_zone = 'taxi_zone_lookup.csv'
    csv_name_green_gz = 'green_trip_data_2019-09.csv.gz'
    csv_name_green = 'green_trip_data_2019-09.csv'
    # todo: download taxi zone csv
    os.system(f"wget {url_zone} -O {csv_name_zone}")
    # todo: download green taxi trip csv.gz
    os.system(f"wget {url_green} -O {csv_name_green_gz}")
    # todo: unzip the gunzip file
    os.system(f"gunzip {csv_name_green_gz}")
    
    
    # todo: create connection to postgres server and DB ny_taxi
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    # todo: read taxi+_zone_lookup.csv in df_zone
    df_zone = pd.read_csv(csv_name_zone)
    # todo: Create table green_taxi_zone_lookup in postgres db ny_taxi
    df_zone.head(n=0).to_sql(name=table_name_zone, con=engine, if_exists='replace')
    # todo: load table green_taxi_zone_lookup in postgres db ny_taxi with df_zone
    df_zone.to_sql(name=table_name_zone, con=engine, if_exists='append')

    # todo: read green_tripdata_2019-09.csv in df_green
    df_green = pd.read_csv(csv_name_green, nrows=100)
    # todo: Create table green_taxi_data in postgres db ny_taxi
    df_green.head(n=0).to_sql(name=table_name_green, con=engine, if_exists='replace')
    # todo: read csv green_tripdata_2019-09.csv -->df iterator df_green_iter in chunksize of 100000 records
    df_green_iter = pd.read_csv(csv_name_green, iterator=True, chunksize=100000)

    # todo:define constant for while loop
    LOAD = True
    # todo:define constant for load time
    TOTAL_LOAD_TIME = 0
    while LOAD == True:
        try:
            # todo: start time for bench marking
            t_start = time()
            # todo: reading next iterator in df
            df_green = next(df_green_iter)
            # todo formatting timestamp fields
            df_green.lpep_pickup_datetime = pd.to_datetime(df_green.lpep_pickup_datetime)
            df_green.lpep_dropoff_datetime = pd.to_datetime(df_green.lpep_dropoff_datetime)
            # todo: append data from df to postgresdb.table ny_taxi.yellow_taxi_data
            df_green.to_sql(name=table_name_green, con=engine, if_exists='append')
            # todo: end time for bench marking
            t_end = time()
            # todo: print data load time for each chunk
            print(f"Another chunk of 100000 record insterted in :{(t_end - t_start):.3f} seconds")
            # todo: increase total load time for current chunk
            TOTAL_LOAD_TIME = TOTAL_LOAD_TIME + (t_end - t_start)
        except Exception as error_msg:
            print(f"Data loading completed with error_msg:{error_msg}")
            # todo: set LOAD false when there is no more chunk of data in df iterator
            LOAD = False
    print(f"Loading complete in:{TOTAL_LOAD_TIME:.3f} seconds")
    
if __name__ == '__main__':
    # todo: Use argument parser to accept command line input
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')
    # todo: Add user,password,host name,port,db name,table name,url for csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', type=int, help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name_zone', help='table name for Taxi Zone lookup data in postgres')
    parser.add_argument('--table_name_green', help='table name for Green Taxi trip data in postgres')
    parser.add_argument('--url_zone', help='url for taxi zone csv to be loaded')
    parser.add_argument('--url_green', help='url for green taxi trip csv to be loaded')

    args = parser.parse_args()
    # print(args.accumulate(args.integers))
    main(args)



