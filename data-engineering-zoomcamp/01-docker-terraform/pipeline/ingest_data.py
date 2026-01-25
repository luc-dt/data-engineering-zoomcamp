#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine 
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(url:str, engine, target_table:str, chunksize: int = 100000,) -> pd.DataFrame:
    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
    first_chunk = next(df_iter)
    first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
    print(f"Table {target_table} created")

    first_chunk.to_sql(name=target_table, con=engine, if_exists="append")
    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
        print(f"Inserted chunk: {len(df_chunk)}")
    
    print(f"done ingesting to {target_table}")


@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default=5432, show_default=True, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--url', default=None, help='URL or path to CSV file (overrides year/month)')
@click.option('--year', default=None, type=int, help='Year for NYC taxi data')
@click.option('--month', default=None, type=int, help='Month for NYC taxi data')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV chunksize')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, year, month, chunksize, target_table):
    # Build URL from year/month if not provided
    if url is None:
        if year is None or month is None:
            raise ValueError("Either --url or both --year and --month must be provided")
        url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
        url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    ingest_data(url=url, engine=engine, target_table=target_table, chunksize=chunksize)

if __name__ == '__main__':
    main()

















