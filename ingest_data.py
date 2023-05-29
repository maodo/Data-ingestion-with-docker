#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    parquet_name = 'output.parquet'
    url = params.url

    # Download parquet file
    os.system(f'wget {url} -O {parquet_name}')
    
    df = pd.read_parquet(parquet_name,engine='fastparquet')
    print(df.head(10))
    # Database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.url
    engine.connect()
    # Print the corresponding sql schema
    print(pd.io.sql.get_schema(df, name = "yellow_taxi_data",con=engine))

    # Load the column names to our database
    df.head(0).to_sql(name=table_name, con=engine,if_exists='replace')
    # We will only populate with the first 1000 rows
    df.head(1000).to_sql(name=table_name,con=engine,if_exists='append')

    # Let's test our database with some basic queries
    query = """
    SELECT * FROM pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog' AND schemaname !='information_schema'
    """
    result = pd.read_sql(query,con=engine)
    print(result)

    query = """
    SELECT * FROM yellow_taxi_data LIMIT 10;
    """
    yellow_taxi_10 = pd.read_sql(query,con=engine)
    print(yellow_taxi_10)

if __name__ == '__main__':
    # We will use python argparse library to hide de Env vars
    parser = argparse.ArgumentParser(
                        prog='DataIngestion',
                        description='Ingest parquet data to Postgres with Docker')
    # We will parse user, password, host, port, database name, table name and parquet url
    parser.add_argument('--user',help='user name for postgres')           # positional argument
    parser.add_argument('--password', help='password for postgres') # positional argument
    parser.add_argument('--host',help='host for postgres') # positional argument
    parser.add_argument('--port',help='port for postgres dbs access')
    parser.add_argument('--db',help='database name for postgres') # positional argument
    parser.add_argument('--table_name',help='table name for postgres') # positional argument
    parser.add_argument('--url',help='url of the parquet file') # positional argument

    args = parser.parse_args()
    main(args)


# python ingest_data.py \
#     --user=root \
#     --password=root \
#     --host=localhost \
#     --port=5433 \
#     --db=ny_taxi \
#     --table_name=yellow_taxi_data \
#     --url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'