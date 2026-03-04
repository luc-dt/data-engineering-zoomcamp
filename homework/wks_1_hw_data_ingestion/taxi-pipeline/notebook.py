import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import marimo as mo
    import pandas as pd

    conn = duckdb.connect("taxi_pipeline.duckdb")
    return conn, mo


@app.cell
def _(conn, mo):
    df = conn.sql("SELECT * FROM taxi_data.rides LIMIT 10").df()
    mo.ui.table(df)
    return


@app.cell
def _(conn, mo):
    # Question_1: What are the start date and end date of the dataset?
    result_1 = conn.sql("""
        SELECT 
            MIN(trip_pickup_date_time) as start_date,
            MAX(trip_pickup_date_time) as end_date
        FROM taxi_data.rides
    """).df()
    mo.ui.table(result_1)
    return


@app.cell
def _(conn, mo):
    # Question_2: What porportion of trips are paid with credit card?
    result_2 = conn.sql("""
        SELECT 
            ROUND(COUNT(*) FILTER (WHERE payment_type='Credit') * 100.0 / COUNT(*), 2) AS credit_card_pct
        FROM taxi_data.rides
    """).df()
    mo.ui.table(result_2)
    return


@app.cell
def _(conn, mo):
    # Question_3: What is the total amount of money generated in tips?
    result_3 = conn.sql("""
        SELECT 
            ROUND(SUM(tip_amt), 2) AS total_tips
        FROM taxi_data.rides
    """).df()
    mo.ui.table(result_3)
    return


@app.cell
def _(conn):
    conn.sql("DESCRIBE taxi_data.rides").df()
    return


if __name__ == "__main__":
    app.run()
