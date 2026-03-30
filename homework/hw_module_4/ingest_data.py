"""
DE Zoomcamp Module 4 — Data Ingestion Script
Downloads NYC Taxi data from DataTalksClub static files and loads into DuckDB.

Data loaded:
  - Yellow taxi: 2019 + 2020 (all months)
  - Green taxi:  2019 + 2020 (all months)
  - FHV taxi:    2019 only (all months)

Run from: d:/data-engineering-zoomcamp/homework/hw_module_4/taxi_rides_ny/
"""
import duckdb
import requests
import sys
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DB_PATH = "taxi_rides_ny.duckdb"


def download_and_convert(taxi_type: str, years: list, months=range(1, 13)):
    """Download CSV.gz files and convert to Parquet format."""
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in months:
            parquet_filepath = data_dir / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

            if parquet_filepath.exists():
                print(f"  ✓ Skip {parquet_filepath.name} (exists)")
                continue

            csv_gz = data_dir / f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            url = f"{BASE_URL}/{taxi_type}/{csv_gz.name}"

            print(f"  ↓ Downloading {csv_gz.name} ...", end=" ", flush=True)
            try:
                r = requests.get(url, stream=True, timeout=120)
                r.raise_for_status()
                with open(csv_gz, "wb") as f:
                    for chunk in r.iter_content(65536):
                        f.write(chunk)
            except requests.exceptions.HTTPError as e:
                print(f"\n  ⚠ HTTP {e.response.status_code} — skipping (file may not exist)")
                if csv_gz.exists():
                    csv_gz.unlink()
                continue

            print("→ Converting to Parquet ...", end=" ", flush=True)
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_csv_auto('{csv_gz}')) "
                f"TO '{parquet_filepath}' (FORMAT PARQUET)"
            )
            con.close()

            csv_gz.unlink()  # remove csv.gz to save space
            print(f"✔ {parquet_filepath.name}")


def load_to_duckdb():
    """Create prod schema and load Parquet files as tables."""
    print(f"\n=== Loading tables into DuckDB: {DB_PATH} ===")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    tables = {
        "yellow_tripdata": "data/yellow/*.parquet",
        "green_tripdata":  "data/green/*.parquet",
        "fhv_tripdata":    "data/fhv/*.parquet",
    }

    for table_name, glob_path in tables.items():
        parquet_files = list(Path(".").glob(glob_path))
        if not parquet_files:
            print(f"  ⚠ No parquet files found for {table_name}, skipping")
            continue

        print(f"  Loading prod.{table_name} ({len(parquet_files)} files) ...", end=" ", flush=True)
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{table_name} AS
            SELECT * FROM read_parquet('{glob_path}', union_by_name=true)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM prod.{table_name}").fetchone()[0]
        print(f"✔  {count:,} rows")

    con.close()


def verify():
    """Print row counts to confirm data is loaded."""
    print(f"\n=== Verification ===")
    con = duckdb.connect(DB_PATH, read_only=True)
    for table in ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM prod.{table}").fetchone()[0]
            print(f"  prod.{table}: {count:,} rows")
        except Exception as e:
            print(f"  prod.{table}: ERROR — {e}")
    con.close()


if __name__ == "__main__":
    # Ensure we're running from inside taxi_rides_ny/
    if not Path("dbt_project.yml").exists():
        print("❌ ERROR: Run this script from inside the taxi_rides_ny/ directory!")
        print("   cd d:\\data-engineering-zoomcamp\\homework\\hw_module_4\\taxi_rides_ny")
        print("   python ..\\ingest_data.py")
        sys.exit(1)

    print("=" * 60)
    print("  DE Zoomcamp Module 4 — NYC Taxi Data Ingestion")
    print("=" * 60)

    print("\n[1/3] Downloading Yellow taxi data (2019–2020)...")
    download_and_convert("yellow", [2019, 2020])

    print("\n[2/3] Downloading Green taxi data (2019–2020)...")
    download_and_convert("green", [2019, 2020])

    print("\n[3/3] Downloading FHV taxi data (2019 only)...")
    download_and_convert("fhv", [2019])

    load_to_duckdb()
    verify()

    print("\n✅ All done! Now run: dbt build --target prod")
