#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine

# load environment variables
from dotenv import load_dotenv


def get_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    if url.endswith('.csv.gz'):
        df = pd.read_csv(csv_name, iterator=True, chunksize=100000,compression='gzip')
    else:
        csv_name = 'output.csv'
        df = pd.read_csv(csv_name)

    return df


def insert_taxi_data(engine, table_name):
    green_trip_data_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
    df_iter = get_data(green_trip_data_url)
    flag = True
    for index, df in enumerate(df_iter):
        print(df.head(3))
        print(df.shape)
        print(df.columns)
        print(df.info())
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        if flag:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            flag = False
        else:
            df.to_sql(name=table_name, con=engine, if_exists='append')
        print(f'Chunk {index + 1} loaded')

def insert_taxi_zone(engine, table_name):
    taxi_zone_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    df = get_data(taxi_zone_url)
    print(df.head())
    print(df.shape)
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    print(f'Taxi zone data loaded')

def check_current_tables(engine):
    query = """
    SELECT tablename	
    FROM pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog' AND 
        schemaname != 'information_schema';
    """

    df= pd.read_sql(query, con=engine)

    return df['tablename'].to_list()

def main():
    load_dotenv()
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    host = os.environ.get('POSTGRES_HOST')
    port = os.environ.get('POSTGRES_PORT')
    db = os.environ.get('POSTGRES_DB')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print('connection is succsefuly.')

    tables = check_current_tables(engine)
    if 'taxi_zone' not in tables:
        insert_taxi_zone(table_name='taxi_zone', engine=engine)
    else:
        print('Taxi zone data already loaded')

    if 'green_trip_data' not in tables:
        insert_taxi_data(engine, 'green_trip_data')
    else:
        print('Taxi data already loaded')

if __name__ == '__main__':
    main()