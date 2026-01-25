# Senior Data Engineer Interview Q&A
## Data Pipeline Project - NYC Taxi Data Ingestion

---

## 1. Error Handling & Robustness

### Q: What happens if the CSV download fails halfway through?
**A:** Currently, the script would crash with an HTTPError. 
I would add:
- Try/except for download failures
- Retry logic (retry 3 times before failing)
- Log the error so we know what went wrong
- Maybe save downloaded file to disk first, then process it

```python
import time
from urllib.error import URLError

def download_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            return pd.read_csv(url, ...)
        except URLError as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # exponential backoff
            else:
                raise
```

---

### Q: What if database connection drops during ingestion?
**A:** We'd lose data and have partial table created.
I would add:
- Transaction management (commit only after each chunk succeeds)
- Connection pooling to detect disconnects early
- Rollback if something fails
- Idempotent inserts (don't duplicate if re-run)

```python
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def ingest_data_with_transactions(url, engine, target_table, chunksize):
    try:
        df_iter = pd.read_csv(url, iterator=True, chunksize=chunksize)
        first_chunk = next(df_iter)
        first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
        
        with engine.begin() as connection:
            for df_chunk in df_iter:
                try:
                    df_chunk.to_sql(name=target_table, con=connection, if_exists="append")
                except SQLAlchemyError as e:
                    logging.error(f"Failed to insert chunk: {e}")
                    raise
    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        raise
```

---

### Q: What if a row has invalid data?
**A:** Pandas would throw an error, stopping entire job.
I would add:
- Data validation before insert (check dtypes match)
- Error handling per chunk (skip bad rows, log them)
- Or use skiprows/error_bad_lines in pandas
- Create a "rejected_rows" table to investigate later

```python
def ingest_data_with_validation(url, engine, target_table, chunksize):
    invalid_rows = []
    
    for df_chunk in pd.read_csv(url, iterator=True, chunksize=chunksize):
        # Validate data
        for col, dtype_expected in dtype.items():
            try:
                df_chunk[col].astype(dtype_expected)
            except ValueError as e:
                invalid_rows.append(df_chunk)
                continue
        
        # Insert valid rows
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
    
    # Log invalid rows
    if invalid_rows:
        pd.concat(invalid_rows).to_sql('rejected_rows', con=engine, if_exists='append')
        logging.warning(f"Rejected {len(invalid_rows)} rows")
```

---

## 2. Data Quality & Monitoring

### Q: How do you verify data was ingested correctly?
**A:** After ingestion, I would:
- Count rows: SELECT COUNT(*) and compare with expected
- Check NULL values: are they expected?
- Validate date ranges (tpep_pickup_datetime should be in 2021)
- Compare checksums/hashes before/after

```python
def validate_ingestion(engine, target_table, expected_row_count):
    with engine.connect() as conn:
        # Check row count
        result = conn.execute(f"SELECT COUNT(*) FROM {target_table}")
        actual_count = result.scalar()
        assert actual_count == expected_row_count, f"Row count mismatch: {actual_count} vs {expected_row_count}"
        
        # Check NULL values
        result = conn.execute(f"SELECT COUNT(*) FROM {target_table} WHERE trip_distance IS NULL")
        null_count = result.scalar()
        logging.info(f"NULL values in trip_distance: {null_count}")
        
        # Validate date range
        result = conn.execute(f"SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM {target_table}")
        min_date, max_date = result.fetchone()
        logging.info(f"Date range: {min_date} to {max_date}")
```

---

### Q: How do you track ingestion performance?
**A:** Add logging/metrics:
- Record start time, end time
- Calculate rows_per_second
- Log to a metrics table
- Alert if takes > expected time

```python
import time
from datetime import datetime

def ingest_data_with_metrics(url, engine, target_table, chunksize):
    start_time = time.time()
    start_dt = datetime.now()
    total_rows = 0
    
    for df_chunk in pd.read_csv(url, iterator=True, chunksize=chunksize):
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
        total_rows += len(df_chunk)
    
    elapsed = time.time() - start_time
    rows_per_sec = total_rows / elapsed
    
    # Log metrics
    logging.info(f"Ingestion completed: {total_rows} rows in {elapsed:.2f}s ({rows_per_sec:.0f} rows/sec)")
    
    # Store in metrics table
    metrics = pd.DataFrame({
        'table_name': [target_table],
        'start_time': [start_dt],
        'end_time': [datetime.now()],
        'total_rows': [total_rows],
        'duration_seconds': [elapsed]
    })
    metrics.to_sql('ingestion_metrics', con=engine, if_exists='append', index=False)
```

---

## 3. Scalability

### Q: What if you need to ingest 1 billion rows?
**A:** Current chunking would still work, but:
- 100k chunks might be too large (memory issue)
- Reduce chunk size to 10k or 50k
- Consider parallel processing (ingest multiple months at once)
- Use database bulk insert tools (faster than pandas to_sql)
- Compress data before transfer

**Solution approach:**
```python
# 1. Reduce chunk size for large datasets
@click.option('--chunksize', default=50000, type=int)  # down from 100k

# 2. Use bulk insert (much faster)
# Option: Use COPY instead of INSERT
# Option: Use sqlalchemy.dialects.postgresql for bulk_insert_mappings

# 3. For truly massive datasets, consider:
# - Apache Spark instead of pandas
# - Distributed processing (multiple workers)
```

---

### Q: What if you need multiple months in parallel?
**A:** Instead of sequential:
```python
# Sequential (current)
for month in [1, 2, 3, ...]:
    ingest(year=2021, month=month)

# Parallel processing
from multiprocessing import Pool

months = [(2021, m) for m in range(1, 13)]
with Pool(processes=4) as pool:
    pool.starmap(ingest, months)

# Or use job queues
# - Airflow: DAG with parallel tasks
# - Celery: distributed task queue
# - AWS Glue: serverless ETL
```

---

## 4. Data Pipeline

### Q: What if same data is ingested twice?
**A:** Would create duplicates! I would:
- Add PRIMARY KEY or UNIQUE constraint on (year, month, trip_id)
- Use INSERT ... ON CONFLICT DO NOTHING (Postgres)
- Or check if data already exists before inserting
- Idempotent design (safe to re-run)

```python
# Create unique constraint (first time setup)
with engine.connect() as conn:
    conn.execute(f"""
        ALTER TABLE {target_table} 
        ADD CONSTRAINT unique_trip 
        UNIQUE(trip_id, vendor_id)
    """)

# Use ON CONFLICT in Postgres (idempotent)
insert_query = f"""
    INSERT INTO {target_table} (columns...)
    VALUES (%s, %s, ...)
    ON CONFLICT (trip_id, vendor_id) DO NOTHING
"""

# Or check before insert
def insert_if_not_exists(df_chunk, engine, target_table):
    trip_ids = df_chunk['trip_id'].unique()
    with engine.connect() as conn:
        existing = conn.execute(f"SELECT trip_id FROM {target_table} WHERE trip_id IN ({','.join(map(str, trip_ids))})")
        existing_ids = set(row[0] for row in existing)
    
    df_new = df_chunk[~df_chunk['trip_id'].isin(existing_ids)]
    df_new.to_sql(name=target_table, con=engine, if_exists="append")
```

---

### Q: Incremental vs full refresh?
**A:** 

**Currently doing full refresh** (if_exists='replace'):
```python
first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
```

**For incremental (recommended for large tables):**
```python
# Delete old data for this month, then insert new
with engine.begin() as conn:
    conn.execute(f"DELETE FROM {target_table} WHERE year={year} AND month={month}")
    
# Then insert all chunks
for df_chunk in df_iter:
    df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
```

**For full refresh (keep as is for small datasets):**
- Simple logic
- Less error-prone
- Good for datasets < 100GB

**For incremental (better for production):**
- Only updates changed data
- Much faster for large tables
- Maintains history if needed
```

---

## 5. Deployment & Testing

### Q: How would you test this before production?
**A:** 
```python
# 1. Unit tests
import pytest

def test_ingest_small_data():
    engine = create_test_db()
    small_csv_url = "file:///test_data/sample.csv"
    ingest_data(small_csv_url, engine, 'test_table', chunksize=1000)
    
    with engine.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM test_table").scalar()
    assert count == 100

# 2. Integration test
def test_full_pipeline():
    engine = create_test_db()
    ingest_data(..., engine, 'test_table', ...)
    validate_ingestion(engine, 'test_table', expected_count=1369765)

# 3. Performance test
def test_ingestion_performance():
    start = time.time()
    ingest_data(..., engine, 'test_table', chunksize=100000)
    elapsed = time.time() - start
    assert elapsed < 300  # Should complete in < 5 minutes

# Run tests
# pytest test_ingest_data.py -v
```

---

### Q: Parameter validation?
**A:** Currently no validation. I would add:

```python
@click.command()
@click.option('--pg-user', default='root', show_default=True)
@click.option('--pg-pass', default='root', show_default=True)
@click.option('--pg-host', default='localhost', show_default=True)
@click.option('--pg-port', default=5432, show_default=True, type=click.IntRange(1, 65535))
@click.option('--pg-db', default='ny_taxi', show_default=True)
@click.option('--year', default=2021, show_default=True, type=click.IntRange(2000, 2100))
@click.option('--month', default=1, show_default=True, type=click.IntRange(1, 12))
@click.option('--chunksize', default=100000, show_default=True, type=click.IntRange(1, 1000000))
@click.option('--target-table', default='yellow_taxi_data', required=True)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    # Additional validation
    if not target_table or not target_table.replace('_', '').isalnum():
        raise ValueError("Invalid table name")
    
    # Check if database is accessible
    try:
        engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
        engine.execute("SELECT 1")
    except Exception as e:
        raise click.ClickException(f"Cannot connect to database: {e}")
    
    main(...)
```

---

## 6. Production Readiness

### Q: Where are logs stored?
**A:** Currently prints to console (lost after script ends).
I would add:

```python
import logging
import sys
from logging.handlers import RotatingFileHandler

def setup_logging(log_file='ingest.log'):
    """Configure logging to file and console"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # File handler (with rotation)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Use in main
def main(...):
    logger = setup_logging(f'ingest_{year}_{month:02d}.log')
    logger.info(f"Starting ingestion for {year}-{month:02d}")
    try:
        ingest_data(...)
        logger.info("Ingestion completed successfully")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise
```

**Alternative: Log to database**
```python
# Store logs in database for analysis
def log_to_database(engine, level, message):
    df = pd.DataFrame({
        'timestamp': [datetime.now()],
        'level': [level],
        'message': [message]
    })
    df.to_sql('logs', con=engine, if_exists='append', index=False)
```

---

### Q: Rollback strategy if data looks wrong?
**A:** 

```python
def ingest_with_validation_and_rollback(url, engine, target_table, year, month):
    """Ingest data with validation. Rollback if validation fails."""
    
    # Step 1: Load to temporary table
    temp_table = f"{target_table}_temp"
    df_full = pd.read_csv(url)
    df_full.to_sql(temp_table, con=engine, if_exists='replace', index=False)
    
    # Step 2: Run validation queries
    with engine.connect() as conn:
        try:
            # Check row count
            count = conn.execute(f"SELECT COUNT(*) FROM {temp_table}").scalar()
            assert count > 0, "No rows loaded"
            
            # Check data quality
            null_trip_ids = conn.execute(f"SELECT COUNT(*) FROM {temp_table} WHERE trip_id IS NULL").scalar()
            assert null_trip_ids == 0, f"Found {null_trip_ids} NULL trip_ids"
            
            # Check date range
            min_date = conn.execute(f"SELECT MIN(tpep_pickup_datetime) FROM {temp_table}").scalar()
            assert min_date.year == year and min_date.month == month
            
            # Step 3: If validation passes, move to real table
            conn.execute(f"DELETE FROM {target_table} WHERE year={year} AND month={month}")
            conn.execute(f"INSERT INTO {target_table} SELECT * FROM {temp_table}")
            logging.info("Data validation passed. Data committed.")
            
        except AssertionError as e:
            logging.error(f"Validation failed: {e}. Rolling back...")
            conn.execute(f"DROP TABLE {temp_table}")
            raise
        finally:
            # Clean up temp table
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
```

**Backup strategy:**
```python
# Keep previous month as backup
def create_backup_before_insert(engine, target_table, year, month):
    backup_table = f"{target_table}_backup_{year}_{month:02d}"
    with engine.connect() as conn:
        conn.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {target_table}")
        logging.info(f"Backup created: {backup_table}")
```

---

### Q: How to schedule automatically?
**A:** 

**Option 1: Cron job (Linux)**
```bash
# Run daily at 2 AM
0 2 * * * /usr/bin/python /app/ingest_data.py --year=2021 --month=1

# With logging
0 2 * * * /usr/bin/python /app/ingest_data.py --year=2021 --month=1 >> /var/log/ingest.log 2>&1
```

**Option 2: Apache Airflow (recommended)**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'taxi_data_ingestion',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2021, 1, 1),
)

def ingest_task(**context):
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    ingest_data(year=year, month=month, ...)

task = PythonOperator(
    task_id='ingest_taxi_data',
    python_callable=ingest_task,
    dag=dag,
)
```

**Option 3: Docker + Kubernetes CronJob**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: taxi-ingest
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: ingest
            image: taxi_ingest:v001
            args: ["--year=2021", "--month=1"]
          restartPolicy: OnFailure
```

**Option 4: Cloud Functions (AWS Lambda/GCP)**
```python
# Google Cloud Function
def ingest_taxi_data(request):
    year = request.args.get('year', 2021)
    month = request.args.get('month', 1)
    ingest_data(year=year, month=month, ...)
    return 'Ingestion completed'

# Schedule with Cloud Scheduler
# gcloud scheduler jobs create pubsub ingest-job \
#     --schedule="0 2 * * *" \
#     --topic=ingest-trigger
```

---

## Summary

**Key Takeaways for Data Engineers:**
1. **Always handle errors** - Network, database, data quality issues will happen
2. **Log everything** - Debugging production issues requires detailed logs
3. **Validate data** - Don't assume input data is clean
4. **Make pipelines idempotent** - Safe to re-run without duplicates
5. **Monitor & alert** - Know when things go wrong
6. **Scale gradually** - Test with small data first, then larger volumes
7. **Use proper scheduling** - Automated, reliable pipelines (Airflow, not cron)
8. **Document your assumptions** - Future you will thank you

---

## Interview Tips

**Do say:**
- "I haven't implemented that yet, but I would..."
- "Here's my approach to solve this..."
- "I would add tests and monitoring..."
- "What's most important to your team?"

**Don't say:**
- "I don't know" (without explaining what you'd do)
- "It just works" (without understanding why)
- "That's not my responsibility" (show ownership)

**Ask clarifying questions:**
- "What's the SLA for ingestion? (How fast must it complete?)"
- "How do we handle schema changes?"
- "Who gets alerted if something fails?"
- "How long must we keep historical data?"

---

Good luck with your interviews! 🚀
