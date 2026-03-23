# Module 7: Real-Time Streaming Pipeline with Redpanda and PyFlink

**Course**: DataTalksClub Data Engineering Zoomcamp 2026
**Dataset**: Green Taxi Trip Data — October 2025
**Stack**: Python 3.12 · Redpanda v25.3.9 · Apache Flink 2.2 (PyFlink) · PostgreSQL 18 · Docker Compose

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture and Tool Decisions](#2-architecture-and-tool-decisions)
3. [Project Structure](#3-project-structure)
4. [Infrastructure Setup](#4-infrastructure-setup)
5. [Phase 1 — Basic Pipeline (Q1–Q3)](#5-phase-1--basic-pipeline-q1q3)
6. [Phase 2 — Stream Processing with Flink (Q4–Q6)](#6-phase-2--stream-processing-with-flink-q4q6)
7. [Key Findings](#7-key-findings)
8. [Lessons Learned and Debugging Journal](#8-lessons-learned-and-debugging-journal)
9. [Commands Cheat Sheet](#9-commands-cheat-sheet)

---

## 1. Project Overview

### The Business Problem

A taxi operations team needs real-time visibility into fleet activity. Batch processing cannot answer operational questions fast enough:

- **"Which pickup zones are busiest right now?"** — useful for rebalancing drivers while the surge is still happening
- **"Is there a sustained demand streak at a specific location?"** — useful for surge pricing decisions
- **"Which hours generate the most tip revenue?"** — useful for fleet financial planning

Traditional batch pipelines process data overnight. By the time results are available, the operational window has closed. This project builds a streaming pipeline that answers these questions within seconds of events occurring.

### What We Built

```
Green Taxi Parquet File (October 2025)
              │
              ▼
    Python Producer (producer.ipynb)
    reads parquet, converts types, publishes JSON to Kafka
              │
              ▼
    Redpanda (Kafka-compatible broker)
    stores 73,000+ events durably, enables replay
              │
    ┌─────────┴──────────────────────┐
    │                                │
    ▼                                ▼
Python Consumer                Apache Flink
(consumer_db.ipynb)            (q4, q5, q6 jobs)
validates data                 aggregates into
writes raw rows                time windows
to PostgreSQL                  writes results
    │                          to PostgreSQL
    └─────────────┬────────────────────┘
                  ▼
             PostgreSQL
        stores results for analysts
```

The pipeline produces three analytical outputs:

- **Q4**: Trip counts per pickup zone per 5-minute window
- **Q5**: Session-based activity streaks per pickup zone
- **Q6**: Total tip revenue per hour across all zones

---

## 2. Architecture and Tool Decisions

Every tool was chosen for a specific reason. Understanding why a tool was selected is as important as knowing how to use it.

### Docker and Docker Compose

**Problem it solves:** "It works on my machine" — the most common source of friction in data engineering.

The `docker-compose.yml` packages five services with pinned versions and exact configurations. Any machine with Docker can run the full stack with one command. No manual installation, no version mismatches.

| Service | Image | Port | Role |
|---|---|---|---|
| `redpanda` | redpandadata/redpanda:v25.3.9 | 9092, 29092 | Message broker |
| `console` | redpandadata/console:latest | 8082 | Redpanda web UI |
| `jobmanager` | pyflink-workshop (custom) | 8081 | Flink coordinator |
| `taskmanager` | pyflink-workshop (custom) | — | Flink worker |
| `postgres` | postgres:18 | 5432 | Result storage |

### Redpanda

**Problem it solves:** Durable, replayable event storage between the producer and Flink.

Chosen over Apache Kafka because:
- Written in C++ — no JVM overhead, starts in seconds on a laptop
- No ZooKeeper dependency — one container instead of two
- Identical Kafka wire protocol — `kafka-python` and Flink's Kafka connector work unchanged

The key property: **replayability**. Events are written to an append-only log. If Flink crashes, it does not lose data — it rewinds to its last checkpoint offset and replays exactly what it missed.

Two listener addresses are configured for different network contexts:

```yaml
--advertise-kafka-addr
  PLAINTEXT://redpanda:29092   # for containers (Flink uses this)
  OUTSIDE://localhost:9092     # for your laptop (producer/consumer use this)
```

This is why Flink DDL always uses `redpanda:29092` — never `localhost:9092`. Inside Docker, `localhost` refers to the Flink container itself, not Redpanda.

### Apache Flink (PyFlink 2.2)

**Problem it solves:** Stateful, time-aware aggregation that a plain Python consumer cannot do reliably.

| Requirement | Plain Python Consumer | Apache Flink |
|---|---|---|
| Group events into hourly buckets | Build manually | `TUMBLE()` built-in |
| Handle late-arriving events | Build manually | Watermarks built-in |
| Survive crash without data loss | Build manually | Checkpointing built-in |
| Process in parallel | Build manually | Task slots built-in |
| Correct already-written results | Build manually | Upserts built-in |

A custom Docker image (`Dockerfile.flink`) extends the official Flink image with Python 3.12, PyFlink 2.2, and three connector JARs (Kafka, JDBC, PostgreSQL driver). The `flink-config.yaml` increases JVM metaspace to 512m specifically because PyFlink's Python-Java bridge requires significantly more metaspace than a pure Java Flink job.

### PostgreSQL 18

**Problem it solves:** A queryable, durable destination that analysts can access with standard SQL.

PostgreSQL is the serving layer. Flink has already done all transformation work upstream. PostgreSQL receives clean, pre-aggregated rows ready for dashboards and reporting tools.

---

## 3. Project Structure

```
hw_module_7/
└── workshop/
    │
    ├── notebooks/                       # Jupyter notebooks (run on your laptop)
    │   ├── producer.ipynb               # Q2: reads parquet, publishes to Redpanda
    │   ├── consumer_db.ipynb            # Q3: reads from Redpanda, writes to PostgreSQL
    │   └── analysis.ipynb               # Q4-Q6: query and verify results
    │
    ├── src/                             # Python source code
    │   ├── __init__.py
    │   ├── models.py                    # GreenRide dataclass, serializer, deserializer
    │   └── job/                         # Flink job files (mounted into container at /opt/src/job/)
    │       ├── q4_tumbling_window.py    # Q4: 5-min tumbling window per zone
    │       ├── q5_session_window.py     # Q5: session window per zone
    │       └── q6_largest_tip.py       # Q6: 1-hour tumbling window, tip aggregation
    │
    ├── docker-compose.yml               # All five services defined here
    ├── Dockerfile.flink                 # Custom Flink image: Python + PyFlink + JARs
    ├── flink-config.yaml               # Flink cluster config (metaspace fix for PyFlink)
    ├── pyproject.flink.toml            # PyFlink dependency (apache-flink==2.2.0)
    ├── pyproject.toml                   # Host Python dependencies
    └── README.md                        # This file
```

### Key Design Decisions in the Structure

**Why are notebooks separate from job files?**

Notebooks run on your laptop and connect via `localhost`. Flink job files run inside Docker containers and connect via Docker DNS service names. They live in different execution contexts and must not be mixed.

```
notebooks/producer.ipynb       → bootstrap_servers = ['localhost:9092']
src/job/q4_tumbling_window.py  → 'properties.bootstrap.servers' = 'redpanda:29092'
```

**Why is `src/` mounted as a Docker volume?**

The `docker-compose.yml` mounts `./src/:/opt/src` into both Flink containers. Editing a job file on your laptop is immediately visible inside the container — no image rebuild needed during development.

**Why does `models.py` live in `src/` instead of `notebooks/`?**

`models.py` is shared between the producer and consumer notebooks. Each notebook adds `src/` to `sys.path` to import it:

```python
project_root = os.path.dirname(os.getcwd())
sys.path.insert(0, project_root)
from src.models import GreenRide, ride_deserializer
```

---

## 4. Infrastructure Setup

### The Correct Startup Order

```
Step 1 — Create src/job/ BEFORE Docker starts
───────────────────────────────────────────────
mkdir -p src/job

WHY: Docker volume mounts create directories as root if they
     do not already exist. Your editor then cannot write files
     into it. Always create the directory yourself first.


Step 2 — Build and start all services
───────────────────────────────────────
docker compose up --build -d

WHY: --build rebuilds the custom pyflink-workshop image
     (adds Python 3.12, PyFlink 2.2, connector JARs).
     First build takes 3-5 minutes.
     -d runs detached, terminal is not blocked.


Step 3 — Verify all five services are healthy
──────────────────────────────────────────────
docker compose ps

Expected: redpanda, console, jobmanager, taskmanager,
          postgres all showing "Up"

If any show "Exiting":
→ Docker needs more memory (3 GB minimum)
→ Increase: Docker Desktop → Settings → Resources


Step 4 — Verify Flink cluster is ready
────────────────────────────────────────
Open http://localhost:8081
Expected: 1 Task Manager, 15 available slots, 0 running jobs


Step 5 — Verify PostgreSQL accepts connections
───────────────────────────────────────────────
docker compose exec -T postgres psql \
    -U postgres -d postgres -c "\conninfo"
Expected: connection confirmation, no errors
```

### The Two-Process Flink Architecture

```
Job Manager ("The Brain")           Task Manager ("The Muscle")
─────────────────────────           ──────────────────────────
Receives job submissions            Executes data processing
Schedules tasks across slots        Holds operator state in memory
Coordinates checkpoints             15 task slots available
Serves Web UI on port 8081          Reports metrics to Job Manager
Memory: 1600m                       Memory: 1728m + 512m metaspace
```

The 512m JVM metaspace in `flink-config.yaml` is specifically required for PyFlink — the Python-Java bridge consumes significantly more metaspace than a pure Java Flink job.

---

## 5. Phase 1 — Basic Pipeline (Q1–Q3)

### Purpose of This Phase

Before adding Flink complexity, we first prove the basic pipeline works end to end:

```
Producer → Kafka → Python Consumer → PostgreSQL
```

This validates that data publishing, reading, and database writes all work correctly. Any problem found here is far easier to diagnose than the same problem buried inside a Flink streaming job. This phase also produces the raw dataset in PostgreSQL that answers Q3.

---

### Question 1: Redpanda Version

**Strategy:** Confirm the Redpanda container is healthy and the `rpk` CLI is accessible.

```bash
docker exec -it workshop-redpanda-1 rpk version
```

**Result:** `v25.3.9`

---

### Question 2: Sending Data to Redpanda

**File:** `notebooks/producer.ipynb`

**Strategy:** Read the green taxi parquet file, transform each row into a `GreenRide` dataclass defined in `src/models.py`, serialize to JSON bytes, and publish to the `green-trips` Kafka topic.

**Why datetimes are stored as strings in `models.py`:**

```python
@dataclass
class GreenRide:
    lpep_pickup_datetime: str    # string, not datetime
    lpep_dropoff_datetime: str   # JSON cannot serialize Python datetime natively
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float
```

Storing as ISO-8601 strings (`"2025-10-01 14:23:05"`) avoids a custom JSON encoder and is trivially parsed by Flink's `TO_TIMESTAMP` function on the consumer side.

**Why NaN guards are required in `green_ride_from_row()`:**

```python
def get_int(val, default=0):
    return int(val) if pd.notna(val) else default
```

The parquet file contains NaN in `passenger_count` and other numeric columns. Python's `int(NaN)` raises a `ValueError` that crashes the producer loop. Every integer and float field requires a null guard before type conversion.

**Result:** ~73,000 events sent in approximately **60 seconds**.

---

### Question 3: Consumer — Trip Distance

**File:** `notebooks/consumer_db.ipynb`

**Strategy:** Read all events from the `green-trips` topic (from `earliest` offset), write them to PostgreSQL, then count qualifying trips.

**Two key implementation decisions:**

```python
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',       # read all historical data from offset 0
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=5000            # stop automatically after 5s of silence
)
```

`consumer_timeout_ms=5000` — without this, the consumer loop runs forever waiting for new messages. For a finite dataset, a 5-second timeout makes the notebook cell complete automatically.

**Batch commit pattern for performance:**

```python
batch_size = 10000
if count % batch_size == 0:
    conn.commit()
```

Committing every single row is extremely slow — each commit is a PostgreSQL round trip. Batching every 10,000 rows reduces total ingestion time from hours to minutes.

**Result:** **8,506 trips** have `trip_distance > 5.0`.

---

## 6. Phase 2 — Stream Processing with Flink (Q4–Q6)

### Transition from Phase 1

Flink reads from Kafka, not from PostgreSQL. The Phase 1 table (`green_trips`) is left intact. New dedicated tables are created for each Flink question.

### Common Configuration Across All Three Jobs

**Event time — always pickup, never dropoff:**

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

`lpep_pickup_datetime` is used because pickup is when demand occurs. Using dropoff time would assign trips to the wrong time window — a trip starting at 14:55 and ending at 15:08 would be counted in the 15:00 hour instead of the 14:00 hour.

**Parallelism = 1:**

```python
env.set_parallelism(1)
```

The `green-trips` topic has one partition. With parallelism > 1, idle workers receive no events and never advance the watermark — windows never close, PostgreSQL stays empty.

**Offset = earliest:**

```python
'scan.startup.mode' = 'earliest-offset',
'properties.auto.offset.reset' = 'earliest'
```

All October 2025 data is already in Kafka from Phase 1. With `latest-offset`, Flink waits for new events that never arrive.

**Unique consumer group per job:**

```python
'properties.group.id' = 'q4-group-fresh'   # Q4
'properties.group.id' = 'q5-group-v3'      # Q5
'properties.group.id' = 'q6-group-fresh'   # Q6
```

Each job tracks its own Kafka read offset independently. All three jobs can read the full dataset from offset 0 without interfering with each other.

---

### Question 4: Tumbling Window — Pickup Location

**File:** `src/job/q4_tumbling_window.py`

**Business question:** "Every 5 minutes, which pickup zones have the most activity?"

**What a tumbling window is:**

```
Time →
│← 5 min →│← 5 min →│← 5 min →│← 5 min →│
│ Window 1 │ Window 2 │ Window 3 │ Window 4 │
  fixed      fixed      fixed      fixed
  no overlap, no gaps, every event in exactly one window
```

**PostgreSQL table (create before submitting):**

```sql
CREATE TABLE IF NOT EXISTS trips_per_zone_5min (
    window_start   TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

The `PRIMARY KEY` enables upsert mode in the JDBC connector. Without it, late events insert duplicate rows instead of correcting existing ones — trip counts become permanently inflated with no error message.

**The aggregation query:**

```sql
INSERT INTO trips_per_zone_5min
SELECT
    window_start,
    PULocationID AS pulocationid,
    COUNT(*) AS num_trips
FROM TABLE(
    TUMBLE(
        TABLE green_trips,
        DESCRIPTOR(event_timestamp),
        INTERVAL '5' MINUTE
    )
)
GROUP BY window_start, PULocationID
```

**Submit and verify:**

```bash
docker compose exec -T jobmanager ./bin/flink run \
    -py /opt/src/job/q4_tumbling_window.py

# Wait until count stabilizes
docker compose exec -T postgres psql -U postgres -d postgres \
    -c "SELECT COUNT(*) FROM trips_per_zone_5min;"

# Query the answer
docker compose exec -T postgres psql -U postgres -d postgres -c "
    SELECT PULocationID, num_trips
    FROM trips_per_zone_5min
    ORDER BY num_trips DESC LIMIT 3;"
```

**Result:** **PULocationID 74** (Jackson Heights, Queens) — **30 trips** in a single 5-minute window.

---

### Question 5: Session Window — Longest Streak

**File:** `src/job/q5_session_window.py`

**Business question:** "Which pickup zone had the longest unbroken burst of continuous demand?"

**What a session window is:**

Unlike tumbling windows, session windows have no fixed size. They open when the first event arrives and close only when there is a silence gap longer than the configured timeout.

```
Trips for zone 74:
●  ●  ●  ●                    ●  ●  ●
              gap > 5 minutes
└── Session 1 ──┘              └── Session 2 ──┘
  (4 trips, closes              (3 trips, closes
   after 5-min silence)          after 5-min silence)
```

**Critical decision — `PARTITION BY`:**

```sql
SESSION(
    TABLE green_trips PARTITION BY PULocationID,
    DESCRIPTOR(event_timestamp),
    INTERVAL '5' MINUTE
)
```

Without `PARTITION BY PULocationID`, Flink computes one global session across all zones. Any trip from any zone resets the silence timer — the result is one session spanning 16+ hours with hundreds of trips, which is completely wrong.

With `PARTITION BY`, each zone has its own independent silence timer.

**Sink table with `window_end`:**

Session windows need `window_end` in the PRIMARY KEY because two sessions for the same zone could share a `window_start` but differ in duration:

```sql
CREATE TABLE IF NOT EXISTS trips_per_zone_sessions (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
```

**Answer query:**

```sql
SELECT pulocationid, num_trips, window_start, window_end
FROM trips_per_zone_sessions
ORDER BY num_trips DESC
LIMIT 1;
```

**Result:** **PULocationID 74** — **81 trips** in the longest single session.

---

### Question 6: Tumbling Window — Largest Tip

**File:** `src/job/q6_largest_tip.py`

**Business question:** "Which hour of the entire month generated the most tip revenue across all zones?"

**Key difference from Q4:**

Q4 groups by `(window_start, PULocationID)` — one row per zone per window.

Q6 groups only by `(window_start, window_end)` — one row per hour globally. No location dimension. Total tips summed across the entire fleet.

```sql
INSERT INTO total_tips_per_hour
SELECT
    window_start,
    window_end,
    SUM(tip_amount) AS total_tips
FROM TABLE(
    TUMBLE(
        TABLE green_trips,
        DESCRIPTOR(event_timestamp),
        INTERVAL '1' HOUR
    )
)
GROUP BY window_start, window_end
```

**PostgreSQL table:**

```sql
CREATE TABLE IF NOT EXISTS total_tips_per_hour (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    total_tips     DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);
```

**Answer query:**

```sql
SELECT window_start, window_end, ROUND(total_tips::numeric, 2) AS total_tips
FROM total_tips_per_hour
ORDER BY total_tips DESC
LIMIT 5;
```

**Result:** **2025-10-30 16:00:00** — **$507.10** in total tips.

---

## 7. Key Findings

| Question | Answer | Detail |
|---|---|---|
| Q1 — Redpanda version | v25.3.9 | — |
| Q2 — Dataset send time | ~60 seconds | ~73,000 events to Kafka |
| Q3 — Trips with distance > 5km | 8,506 trips | filtered from PostgreSQL |
| Q4 — Busiest zone (5-min window) | PULocationID 74 | 30 trips in one window |
| Q5 — Longest activity session | PULocationID 74 | 81 trips, unbroken session |
| Q6 — Peak tip revenue hour | 2025-10-30 16:00:00 | $507.10 total |

**Notable pattern:** PULocationID 74 (Jackson Heights, Queens) dominates both Q4 and Q5 — the busiest zone in any given 5-minute window and the zone with the longest sustained activity streak. This is consistent with it being one of NYC's highest-density residential and commercial areas.

---

## 8. Lessons Learned and Debugging Journal

### The Three-Part Correctness Guarantee

Exactly-once processing requires all three components. Remove any one and results become silently wrong:

```
Checkpointing     +    Watermark     +    PRIMARY KEY (upsert)
─────────────          ─────────          ──────────────────
Crash recovery         Window             Late event
no data loss           trigger            correction
no duplicates          (when to emit)     (update, not duplicate)
on restart
```

### Bugs Encountered and Fixed

**Bug 1 — Wrong bootstrap server port (Q4)**

```
Symptom:   Job shows RUNNING in Flink UI, zero rows in PostgreSQL, no errors
Root cause: 'properties.bootstrap.servers' = 'redpanda:29002'  ← missing a 9
Fix:        'properties.bootstrap.servers' = 'redpanda:29092'
Lesson:    Wrong port is a silent failure. Check bootstrap.servers
           first whenever Flink runs but produces zero rows.
```

**Bug 2 — Wrong interval syntax (Q4)**

```
Symptom:   Job fails at runtime with SQL parse error
Root cause: INTERVAL '5' MINUTES   ← plural, invalid Flink SQL
Fix:        INTERVAL '5' MINUTE    ← singular only
Lesson:    Flink SQL interval syntax is strict. Always use singular.
```

**Bug 3 — Two jobs writing to the same table (Q4)**

```
Symptom:   Results appear but counts are doubled
Root cause: First job was not canceled before resubmitting.
            Both jobs read from offset 0, wrote to same table.
Fix:        Cancel old job in Flink UI → TRUNCATE TABLE → resubmit once.
Rule:       Always verify Running Jobs = 1 before trusting any results.
```

**Bug 4 — Missing PARTITION BY in session window (Q5)**

```
Symptom:   One row returned with 388 trips spanning 16 hours
Root cause: SESSION() without PARTITION BY computes one global session.
            Any trip from any zone resets the silence timer permanently.
Fix:        SESSION(TABLE green_trips PARTITION BY PULocationID, ...)
Lesson:    SESSION windows always require PARTITION BY.
           Without it, the result is always one massive wrong session.
```

**Bug 5 — Windows CRLF line endings blocking job submission (Q5)**

```
Symptom:   NoSuchFileException when submitting — file exists but Flink
           cannot copy it to its temp directory
Root cause: File saved with Windows CRLF line endings.
Fix:        Copy file directly into container bypassing the volume:
            docker cp src/job/q5_session_window.py \
                workshop-jobmanager-1:/tmp/q5_session_window.py
Lesson:    On Windows, CRLF line endings cause silent failures
           in Linux container file operations.
```

### Debugging Checklist — Zero Rows in PostgreSQL

```
1. Is the job RUNNING in Flink UI?
   → If FAILED: Exceptions tab shows root cause

2. Are Records Received > 0 in operator metrics?
   → If 0: bootstrap.servers port is wrong (29002 vs 29092)

3. Are Records Sent = 0 on the sink operator?
   → Data is arriving but windows have not closed yet
   → Check: parallelism = 1 (matches 1-partition topic)
   → Check: scan.startup.mode = earliest-offset
   → Wait longer — session windows take more time than tumbling

4. Does the PostgreSQL table exist?
   → Flink NEVER creates tables automatically
   → Create the table before submitting the job

5. Does the table have a PRIMARY KEY?
   → Without it, late events insert duplicate rows silently
   → Drop and recreate the table with PRIMARY KEY
```

---

## 9. Commands Cheat Sheet

### Infrastructure

```bash
# Start the full stack
docker compose up --build -d

# Check all service statuses
docker compose ps

# Stop without losing PostgreSQL data
docker compose down

# Full reset — permanently deletes all PostgreSQL data
docker compose down -v

# View Task Manager logs for processing issues
docker compose logs taskmanager --tail 50
```

### Redpanda Operations

```bash
# Check Redpanda version (Q1)
docker exec -it workshop-redpanda-1 rpk version

# Create the green-trips topic
docker exec -it workshop-redpanda-1 rpk topic create green-trips

# Verify messages arrived (first 10 from beginning)
docker compose exec -T redpanda rpk topic consume green-trips \
    -n 10 --offset earliest

# Delete topic (if producer ran twice — prevents duplicate counts)
docker exec -it workshop-redpanda-1 rpk topic delete green-trips

# Check consumer group lag
docker compose exec redpanda rpk group describe flink-consumer-group
```

### Flink Job Management

```bash
# Submit Q4
docker compose exec -T jobmanager ./bin/flink run \
    -py /opt/src/job/q4_tumbling_window.py

# Submit Q5
docker compose exec -T jobmanager ./bin/flink run \
    -py /opt/src/job/q5_session_window.py

# Submit Q6
docker compose exec -T jobmanager ./bin/flink run \
    -py /opt/src/job/q6_largest_tip.py

# List all running jobs (get job IDs)
docker compose exec jobmanager ./bin/flink list

# Cancel a specific job
docker compose exec jobmanager ./bin/flink cancel <JOB_ID>

# Windows CRLF workaround — copy file directly into container
docker cp src/job/q5_session_window.py \
    workshop-jobmanager-1:/tmp/q5_session_window.py
```

### PostgreSQL Operations

```bash
# Open interactive SQL shell
docker compose exec postgres psql -U postgres -d postgres

# Run a one-line query
docker compose exec -T postgres psql -U postgres -d postgres \
    -c "SELECT COUNT(*) FROM trips_per_zone_5min;"

# Truncate a result table (clean slate before resubmitting)
docker compose exec -T postgres psql -U postgres -d postgres \
    -c "TRUNCATE TABLE trips_per_zone_5min;"

# List all tables
docker compose exec -T postgres psql -U postgres -d postgres \
    -c "\dt"
```

### PostgreSQL Table Creation Scripts

```sql
-- Q4 table
CREATE TABLE IF NOT EXISTS trips_per_zone_5min (
    window_start   TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

-- Q5 table
CREATE TABLE IF NOT EXISTS trips_per_zone_sessions (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

-- Q6 table
CREATE TABLE IF NOT EXISTS total_tips_per_hour (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    total_tips     DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);
```

### Answer Queries

```sql
-- Q3: Trips with distance > 5km
SELECT COUNT(*) FROM green_trips WHERE trip_distance > 5.0;

-- Q4: Zone with most trips in a 5-minute window
SELECT PULocationID, num_trips
FROM trips_per_zone_5min
ORDER BY num_trips DESC LIMIT 3;

-- Q5: Zone with the longest session (most trips)
SELECT pulocationid, num_trips, window_start, window_end
FROM trips_per_zone_sessions
ORDER BY num_trips DESC LIMIT 1;

-- Q6: Hour with highest total tip amount
SELECT window_start, window_end,
       ROUND(total_tips::numeric, 2) AS total_tips
FROM total_tips_per_hour
ORDER BY total_tips DESC LIMIT 5;
```

---

*DataTalksClub Data Engineering Zoomcamp 2026 — Module 7*
*Stack: Python 3.12 · PyFlink 2.2 · Redpanda v25.3.9 · PostgreSQL 18 · Docker Compose*
