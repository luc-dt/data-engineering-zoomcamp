"""@bruin
name: ingestion.trips             # Schema = ingestion, table name = trips
type: python                      # This asset runs Python code
image: python:3.11                # Run in Python 3.11 environment
connection: duckdb-default        # Save results to DuckDB

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "Pickup location ID"
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location ID"
  - name: fare_amount
    type: float
    description: "Base fare in USD"
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: "Payment type identifier"
  - name: taxi_type
    type: string
    description: "yellow or green"
    checks:
      - name: not_null

@bruin"""

import os
import io
import json
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
def materialize():
    # Read Bruin runtime variables
    start_date = os.environ["BRUIN_START_DATE"]        # e.g. "2025-01-01"
    end_date = os.environ["BRUIN_END_DATE"]            # e.g. "2025-02-01"
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    start = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_dfs = []
    current = start

    while current <= end:
        for taxi_type in taxi_types:
            month_str = current.strftime("%Y-%m")
            url = f"{base_url}/{taxi_type}_tripdata_{month_str}.parquet"
            print(f"Fetching: {url}")

            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                df = pd.read_parquet(io.BytesIO(response.content))

                # Normalize column names between yellow and green taxis
                col_map = {
                    "tpep_pickup_datetime":  "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    "lpep_pickup_datetime":  "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                    "PULocationID":          "pickup_location_id",
                    "DOLocationID":          "dropoff_location_id",
                    "fare_amount":           "fare_amount",
                    "payment_type":          "payment_type",
                }
                df = df.rename(columns=col_map)
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()  # lineage/debugging

                # Keep only the columns we need
                keep = [
                    "pickup_datetime", "dropoff_datetime",
                    "pickup_location_id", "dropoff_location_id",
                    "fare_amount", "payment_type",
                    "taxi_type", "extracted_at"
                ]
                df = df[[c for c in keep if c in df.columns]]
                all_dfs.append(df)
                print(f"  -> Loaded {len(df):,} rows")
            else:
                print(f"  -> Skipping {taxi_type}_tripdata_{month_str}.parquet: HTTP {response.status_code}")

        current += relativedelta(months=1)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total rows: {len(final_df):,}")
        return final_df

    print("No data fetched for the given date range.")
    return pd.DataFrame()