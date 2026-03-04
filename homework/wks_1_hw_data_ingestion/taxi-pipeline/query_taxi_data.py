import duckdb
import sys

print("Starting query script...")

try:
    # Connect to the database
    con = duckdb.connect('d:/wks_1_hw_data_ingestion/taxi-pipeline/taxi_pipeline.duckdb', read_only=True)
    print("Connection successful.")

    # Query 1: Start and end date
    start_end_date = con.execute("SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM taxi_data.rides;").fetchone()
    print(f"Start date: {start_end_date[0]}")
    print(f"End date: {start_end_date[1]}")

    # Query 2: Proportion of trips paid with credit card
    credit_card_proportion = con.execute("SELECT CAST(SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) FROM taxi_data.rides;").fetchone()[0]
    print(f"Proportion of trips paid with credit card: {credit_card_proportion:.2%}")

    # Query 3: Total amount of tips generated
    total_tips = con.execute("SELECT SUM(tip_amt) FROM taxi_data.rides;").fetchone()[0]
    print(f"Total amount of tips: {total_tips:,.2f}")

except Exception as e:
    print(f"An error occurred: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    # Close the connection
    if 'con' in locals() and con:
        con.close()
        print("Connection closed.")

print("Script finished.")
