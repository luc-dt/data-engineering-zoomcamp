#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine 
from tqdm.auto import tqdm
import click


def ingest_data(url: str, engine, target_table:str):
    # Determine how to read the file based on its extension
    if url.endswith('.csv'):
        df = pd.read_csv(url)
    elif url.endswith('.parquet'):
        df = pd.read_parquet(url)
    else:
        raise ValueError("Unsupported file format. Please use .csv or .parquet")
    
    # This one line creates the table structure AND inserts all rows
    df.to_sql(name=target_table, 
            con=engine, 
            if_exists="replace", 
            index=False,
            chunksize=1000, # Limits how many rows are in one 'batch' sent to SQL
            method="multi"   # Groups multiple rows into a single INSERT statement
            )
    print(f"done ingesting to {target_table}")
    
@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default=5432, show_default=True, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--url', required=True, help='URL or path to the .csv or .parquet file')
@click.option('--target-table', required=True, help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, target_table):
    # 1. Create the connection string
    conn_url = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    # 2. Create the engine
    engine = create_engine(conn_url)
    # 3. Run the ingestion logic we perfected yesterday
    print(f"Starting ingestion for {url}...")
    ingest_data(url=url, engine=engine, target_table=target_table)
    print("Mission accomplished!")

if __name__ == '__main__':
    main()




