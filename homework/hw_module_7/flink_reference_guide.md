# Apache Flink — Complete Reference Guide
**Based on DataTalksClub Data Engineering Zoomcamp — Module 7**
*PyFlink 2.2 · Redpanda · PostgreSQL · Docker Compose*

---

## Table of Contents

1. [Why Flink? The Four Problems Python Cannot Solve](#1-why-flink)
2. [The Flink Image and Services](#2-the-flink-image-and-services)
3. [The Pass-Through Job — The Base Template](#3-the-pass-through-job)
4. [Offsets — Earliest vs Latest](#4-offsets--earliest-vs-latest)
5. [Aggregation with Tumbling Windows](#5-aggregation-with-tumbling-windows)
6. [Watermarks — When Does a Window Close?](#6-watermarks)
7. [Late Events and Upserts](#7-late-events-and-upserts)
8. [Window Types — Tumbling, Sliding, Session](#8-window-types)
9. [The Three-Part Correctness Guarantee](#9-the-three-part-correctness-guarantee)
10. [Job Submission](#10-job-submission)
11. [Cleanup — down vs down -v](#11-cleanup)
12. [Common Gotchas Checklist](#12-common-gotchas-checklist)
13. [Quick Reference — SQL Patterns](#13-quick-reference--sql-patterns)

---

## 1. Why Flink?

### The Four Problems a Plain Python Consumer Cannot Solve

A Python Kafka consumer works for simple read-and-write. But it breaks down fast when real requirements arrive.

---

**Problem 1 — Windowed Aggregation**

A business question like "how many trips per pickup zone per hour?" requires grouping events across time. With Python you'd have to build windowing logic yourself — tracking buckets in a dictionary, deciding when a window is "done," handling late arrivals. Weeks of engineering work, easy to get wrong.

**Flink's answer:**
```sql
TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
```
One line. All of that handled.

---

**Problem 2 — Crash Recovery**

Your consumer is at row 50,000 and the machine dies. When it restarts:
```
auto_offset_reset = 'earliest' → reprocess from row 1 → DUPLICATES
auto_offset_reset = 'latest'   → skip to newest         → DATA LOSS
```
Doing it correctly yourself requires manual offset commits, retry logic, and careful ordering.

**Flink's answer:** Checkpointing. Every N seconds, Flink snapshots its state (Kafka offset + open windows + any in-flight data) to disk. On restart it rewinds to that snapshot automatically. Zero code from you.

---

**Problem 3 — Parallelism**

One Python process reads one stream. To scale, you need multiple consumers, partition assignment, and rebalancing on failure — essentially building a distributed system from scratch.

**Flink's answer:**
```python
env.set_parallelism(3)
```
One line. Flink's Job Manager assigns partitions to Task Manager slots automatically.

---

**Problem 4 — Multiple Sinks**

Today: PostgreSQL. Tomorrow: S3, Elasticsearch, Redis. With Python, each destination needs its own connector code, batching, retries, and connection pooling.

**Flink's answer:** Sinks are declared as DDL tables. Changing destination means changing a `WITH` clause:
```sql
'connector' = 'jdbc'        -- PostgreSQL today
'connector' = 'kafka'       -- Kafka topic tomorrow
'connector' = 'filesystem'  -- S3 next week
```

---

### The Mental Model

```
Plain Python Consumer          Apache Flink
─────────────────────          ────────────
You manage offsets        →    Checkpointing handles it
You implement windows     →    TUMBLE/HOP/SESSION built-in
You manage parallelism    →    Task slots + Job Manager
You write sink connectors →    DDL WITH clauses
You handle late data      →    Watermarks + upserts
```

Python consumer = a script. Flink = a runtime that runs your logic continuously, durably, and in parallel.

---

## 2. The Flink Image and Services

### Why a Custom Docker Image?

Official Flink is Java only. Before it can run PyFlink jobs you must add three things:

```
Official Flink image
        │
        ├── + Python 3.12
        ├── + PyFlink package (Python API that talks to Flink's Java core)
        └── + Connector JARs
                ├── flink-connector-kafka
                ├── flink-connector-jdbc
                └── postgresql driver
```

`Dockerfile.flink` does this. The result is the `pyflink-workshop` image.

---

### The Two-Process Architecture

Flink is always two processes — never one:

```
┌────────────────────────────────────────────┐
│              Flink Cluster                 │
│                                            │
│   ┌──────────────┐    ┌────────────────┐   │
│   │  Job Manager │    │  Task Manager  │   │
│   │  "The Brain" │───▶│  "The Muscle"  │   │
│   │              │    │                │   │
│   │ - Schedules  │    │ - Runs your    │   │
│   │   tasks      │    │   actual code  │   │
│   │ - Coordinates│    │ - Holds state  │   │
│   │   checkpoints│    │ - Has slots    │   │
│   │ - Serves UI  │    │                │   │
│   └──────────────┘    └────────────────┘   │
│        :8081                               │
└────────────────────────────────────────────┘
```

Restaurant analogy:
- **Job Manager** = the manager. Takes orders (job submission), coordinates the kitchen, tracks progress.
- **Task Manager** = the chef. Actually cooks (processes data). Never talks to customers.

---

### Key docker-compose Settings Explained

**Job Manager**

```yaml
build:
  context: .
  dockerfile: ./Dockerfile.flink
image: pyflink-workshop      # name the built image for Task Manager to reuse
pull_policy: never           # don't look on Docker Hub — it doesn't exist there
ports:
  - "8081:8081"              # Web UI
volumes:
  - ./src/:/opt/src          # your code mounted live — no rebuild needed on changes
environment:
  FLINK_PROPERTIES: |
    jobmanager.rpc.address: jobmanager   # how workers find the brain (Docker DNS)
    jobmanager.memory.process.size: 1600m
```

**Task Manager**

```yaml
image: pyflink-workshop      # reuses the image built by Job Manager — no rebuild
pull_policy: never
depends_on:
  - jobmanager               # start container after Job Manager
command: taskmanager --taskmanager.registration.timeout 5 min
environment:
  FLINK_PROPERTIES: |
    jobmanager.rpc.address: jobmanager
    taskmanager.memory.process.size: 1728m
    taskmanager.numberOfTaskSlots: 15   # how many parallel operators this worker can run
    parallelism.default: 3
```

**Task slots explained:**
```
Task Manager (15 slots)
├── Slot 1  ← Job A, operator 1
├── Slot 2  ← Job A, operator 2
├── Slot 3  ← Job A, operator 3
├── Slots 4-15 ← idle (available for other jobs)
```
With parallelism=3, one job uses 3 slots. With 15 slots you can run 5 such jobs simultaneously.

**The `mkdir -p src/job` trap — always do this BEFORE `docker compose up`:**
```
Scenario A — You create src/ first (correct):
  Owner = your user → you can write files freely ✓

Scenario B — Docker creates src/ first (trap):
  Owner = root → PermissionError when saving job files ✗
```
Fix if it happened: `sudo chown -R $(whoami) src/`

---

### Verifying the Stack is Healthy

```bash
docker compose ps                     # all 4 services show "Up"
docker exec -it workshop-redpanda-1 rpk version   # Redpanda responds
# Open http://localhost:8081          # Flink UI shows 1 TM, 15 slots
docker compose exec -T postgres psql -U postgres -d postgres -c "\conninfo"
```

---

## 3. The Pass-Through Job

### What It Is

The simplest possible Flink job — read from Kafka, write to PostgreSQL, no transformation:

```
Kafka → Flink → PostgreSQL
```

Its purpose is to prove all three layers are connected before adding complexity. Think of it as testing a pipe with water before running oil through it.

---

### The Three-Function Structure

Every PyFlink job follows this skeleton:

```python
# WHERE does data come from?
def create_events_source_kafka(t_env): ...

# WHERE does data go?
def create_processed_events_sink_postgres(t_env): ...

# WHAT do we do with it?
def log_processing(): ...
```

---

### Function 1 — The Source (Kafka)

```python
source_ddl = """
    CREATE TABLE events (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance DOUBLE,
        total_amount DOUBLE,
        tpep_pickup_datetime BIGINT
    ) WITH (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = 'redpanda:29092',  -- NOT localhost
        'topic' = 'rides',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    );
"""
```

**Critical points:**
- Column names must exactly match your JSON field names — mismatch returns nulls silently
- `redpanda:29092` not `localhost:9092` — inside Docker, `localhost` means Flink itself
- This is a virtual table declaration — no data moves yet

```
Your laptop          Docker network
───────────          ──────────────
localhost:9092  ≠   redpanda:29092
(for producer)      (for Flink job)
```

---

### Function 2 — The Sink (PostgreSQL)

```python
sink_ddl = """
    CREATE TABLE processed_events (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance DOUBLE,
        total_amount DOUBLE,
        pickup_datetime TIMESTAMP        -- different type from source
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'processed_events',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    );
"""
```

No psycopg2, no INSERT statements. Just declare the table and Flink handles all writes.

Note: no PRIMARY KEY here. Pass-through is append-only. PRIMARY KEY comes when you have windows and late events (see Section 5).

---

### Function 3 — The Execution

```python
def log_processing():
    # 1. Create the runtime
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)   # snapshot every 10 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Register virtual tables (no data moves yet)
    source_table = create_events_source_kafka(t_env)
    postgres_sink = create_processed_events_sink_postgres(t_env)

    # 3. The pipeline — this is where the job actually starts
    t_env.execute_sql(f"""
        INSERT INTO {postgres_sink}
        SELECT
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount,
            TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3) AS pickup_datetime
        FROM {source_table}
    """).wait()   # .wait() keeps the job running
```

`TO_TIMESTAMP_LTZ(col, 3)` converts epoch milliseconds (BIGINT) to TIMESTAMP. The `3` = millisecond precision.

`.wait()` is essential — without it the Python script exits immediately and the job stops.

---

## 4. Offsets — Earliest vs Latest

### What an Offset Is

Kafka stores messages in an append-only numbered log:

```
Topic: green-trips
┌────────┬───────────────────────────────────────┐
│ Offset │ Message                               │
├────────┼───────────────────────────────────────┤
│   0    │ {PULocationID: 74, total_amount:12.50}│
│   1    │ {PULocationID: 75, total_amount: 8.00}│
│  ...   │ ...                                   │
│ 73,421 │ {PULocationID: 75, total_amount:11.00}│ ← latest
└────────┴───────────────────────────────────────┘
```

When a consumer connects, Kafka asks: "Where do you want to start reading from?" That is the offset setting.

---

### The Two Starting Points

```
Offset:   0    1    2    3  .....  73,421
          │                    │        │
          └── "earliest"       │        └── "latest"
                               └── also latest
```

**`earliest-offset`** — start from offset 0, read everything ever written to this topic.

**`latest-offset`** — start from current end, only read new messages arriving after the job starts.

---

### Why This Matters for Windows

Your green taxi data has October 2025 timestamps. Today is March 2026.

```
latest-offset → Flink starts reading from "now" (March 2026)
              → Waits for new messages
              → No new October 2025 messages arrive
              → Windows never fill up
              → Zero rows in PostgreSQL
              → You think Flink is broken ✗

earliest-offset → Flink rewinds to offset 0
               → Processes all October 2025 events
               → Windows close with real data
               → Results appear in PostgreSQL ✓
```

For homework Q4–Q6: always use `earliest-offset`.

---

### The Duplicate Problem

If you ran the producer twice:
```
First run:   offsets 0 → 73,421   (73,422 messages)
Second run:  offsets 73,422 → 146,843  (same data again)
```

With `earliest-offset`, Flink reads all 146,844 messages — every trip counted twice, every answer wrong.

**Fix:**
```bash
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
docker exec -it workshop-redpanda-1 rpk topic create green-trips
# Run producer exactly once
```

---

### Offset Tracking Is Per Consumer Group

Each Flink job submission gets its own consumer group. Q4, Q5, Q6 jobs each read independently from offset 0 without interfering with each other.

---

### Checkpoint vs Offset Setting Interaction

```
Job running, checkpointed at offset 50,000, then crashes:

RESTART same job  → Flink reads checkpoint → resumes from 50,001
                    offset setting is IGNORED ✓

CANCEL + new job  → No checkpoint, brand new job instance
                    offset setting NOW applies
                    earliest → reprocess from 0
                    latest   → skip everything before now
```

In production: always restart a failed job rather than cancel and resubmit.

---

### Decision Table

| | `earliest-offset` | `latest-offset` |
|---|---|---|
| Starts from | Offset 0 | Current end |
| Sees historical data | ✅ Yes | ❌ No |
| Use case | Backfill, homework, testing | Production live feed |
| Risk | Duplicates if producer ran twice | Missing historical data |
| Homework Q4–Q6 | ✅ Use this | ❌ Windows never close |

---

## 5. Aggregation with Tumbling Windows

### What a Tumbling Window Is

```
Time →
────────────────────────────────────────────────────────
00:00        01:00        02:00        03:00
  │            │            │            │
  ├── Window 1 ┤├── Window 2 ┤├── Window 3 ┤
  │  1 hour   ││  1 hour   ││  1 hour   │
  │ trips: 47 ││ trips: 63 ││ trips: 31 │
  └───────────┘└───────────┘└───────────┘
```

Properties:
- **Fixed size** — always exactly 1 hour (or whatever you set)
- **Non-overlapping** — windows never share events
- **Every event belongs to exactly one window**
- **No gaps** — immediately after Window 1 closes, Window 2 starts

The `[14:00, 15:00)` notation means: includes 14:00:00, excludes 15:00:00. An event at exactly 15:00:00 goes into the next window.

---

### Pre-create the PostgreSQL Table First

Flink will not create tables automatically. Always run this before submitting:

```sql
CREATE TABLE processed_events_aggregated (
    window_start   TIMESTAMP,
    PULocationID   INTEGER,
    num_trips      BIGINT,
    total_revenue  DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)   -- critical for upserts
);
```

The composite PRIMARY KEY means one row per `(time bucket, pickup zone)`. Without it, late events create duplicate rows with wrong totals.

---

### The Source Table — Two New Lines vs Pass-Through

```sql
CREATE TABLE events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime BIGINT,

    -- NEW: computed column converting epoch ms → timestamp
    event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),

    -- NEW: watermark — tells Flink when to close windows
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND

) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'topic' = 'rides',
    'scan.startup.mode' = 'earliest-offset',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);
```

`event_timestamp` does not exist in the Kafka message — Flink computes it on the fly from each row.

---

### The Sink Table — PRIMARY KEY Added

```sql
CREATE TABLE processed_events_aggregated (
    window_start  TIMESTAMP(3),
    PULocationID  INT,
    num_trips     BIGINT,
    total_revenue DOUBLE,
    PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
    -- NOT ENFORCED = Flink uses PK as upsert signal only
    -- PostgreSQL enforces actual uniqueness
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'processed_events_aggregated',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
);
```

---

### The Aggregation Query

```sql
INSERT INTO processed_events_aggregated
SELECT
    window_start,
    PULocationID,
    COUNT(*) AS num_trips,
    SUM(total_amount) AS total_revenue
FROM TABLE(
    TUMBLE(
        TABLE events,
        DESCRIPTOR(event_timestamp),   -- must reference the WATERMARK column
        INTERVAL '1' HOUR              -- window size
    )
)
GROUP BY window_start, PULocationID;
```

After `TUMBLE(...)`, two virtual columns become available: `window_start` and `window_end`.

**`env.set_parallelism(1)` for your homework** — your `green-trips` topic has 1 partition. With parallelism > 1, idle workers block the watermark from advancing. Windows never close. Zero results.

---

### Lifecycle of One Window

```
1. Events for zone 74 arrive with timestamps in 14:xx range
   → Flink assigns them all to window [14:00, 15:00)
   → Accumulates in memory: count=3, sum=56.25

2. An event with timestamp 15:00:06 arrives
   → Watermark = 15:00:06 - 5s = 15:00:01
   → 15:00:01 > 15:00:00 (window end)
   → Window [14:00, 15:00) CLOSES

3. Flink writes to PostgreSQL:
   INSERT (window_start=14:00, PULocationID=74, num_trips=3, total_revenue=56.25)

4. Late event arrives: ts=14:58:33 (arrived after window closed)
   → PRIMARY KEY triggers UPDATE instead of INSERT
   UPDATE SET num_trips=4, total_revenue=75.25
   WHERE window_start='14:00' AND PULocationID=74
```

---

### Pickup vs Dropoff — Which Timestamp to Use?

Always use **pickup datetime** as event time. Here is why:

```
Trip: starts 14:55, ends 15:08

Using pickup (14:55) → assigned to window [14:00, 15:00) ✓
  → correctly counted in the hour demand occurred

Using dropoff (15:08) → assigned to window [15:00, 16:00) ✗
  → counted in the wrong hour
  → surge pricing reacts 13 minutes too late
```

Pickup = when demand occurred. Pickup = known immediately (no waiting for trip to end). Pickup is always the right choice for operational analytics.

---

## 6. Watermarks

### What a Watermark Is

The watermark is the trigger that tells Flink when a window is safe to close and emit results.

```
Watermark = max_seen_event_timestamp - patience_interval
```

In the DDL:
```sql
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Flink tracks the highest event timestamp it has seen. The watermark trails 5 seconds behind. When the watermark passes a window's end time, that window closes.

---

### Watermark Advancing Step by Step

```
Event arrives: ts=14:00:08  → watermark = 14:00:03   (window still open)
Event arrives: ts=14:00:45  → watermark = 14:00:40   (window still open)
Event arrives: ts=15:00:06  → watermark = 15:00:01
                                          ↑
                                          This just passed 15:00:00
                                          → Window [14:00, 15:00) CLOSES
                                          → Result emitted to PostgreSQL
```

---

### Why Patience Exists

Without patience (0 seconds), any event arriving even 1 millisecond late would be dropped. The 5-second patience says: "I'll wait 5 seconds of event time for stragglers before I declare a window finished."

Real-world events arrive out of order constantly — network jitter, phone reconnecting after tunnel, server queuing. A small patience window accommodates this without sacrificing too much latency.

**Trade-off:**
```
Larger patience → more late events included → longer wait before seeing results
Smaller patience → faster results → more late events dropped
```

---

## 7. Late Events and Upserts

### Two Types of Lateness

**Type 1 — Late but within watermark patience:**
```
Window end:     15:00:00
Patience:       5 seconds
Event ts:       14:59:58, arrives at 15:00:03

Watermark = 15:00:03 - 5s = 14:59:58
Window NOT yet closed (watermark < 15:00:00)
→ Event included in window before it closes ✓
→ No upsert needed
```

**Type 2 — Late beyond patience:**
```
Window end:     15:00:00
Patience:       5 seconds
Event ts:       14:58:33, arrives at 15:00:41

Watermark has already passed 15:00:00
Window already CLOSED and result already written
→ Flink must REVISE the already-written result
→ Upsert (PRIMARY KEY) handles this
```

---

### What Happens Without PRIMARY KEY

```
Window closes → INSERT (window=14:00, PU=74, trips=12, revenue=187.50)
Late event   → INSERT (window=14:00, PU=74, trips=13, revenue=206.50)

PostgreSQL has TWO rows for the same window:
  trips=12 (stale)
  trips=13 (revision)

SELECT SUM(num_trips) → returns 25 instead of 13 ✗ WRONG
```

---

### What Happens With PRIMARY KEY

```
Window closes → INSERT (window=14:00, PU=74, trips=12, revenue=187.50)
Late event   → UPDATE SET trips=13, revenue=206.50
               WHERE window_start='14:00' AND PULocationID=74

PostgreSQL has ONE row:
  trips=13 ✓ CORRECT
```

---

## 8. Window Types

### Tumbling Windows

```
│← 1 hour →│← 1 hour →│← 1 hour →│
│  Window 1 │  Window 2 │  Window 3 │
```

- Fixed size, no overlap, no gaps
- Every event belongs to exactly one window
- SQL function: `TUMBLE()`

```sql
TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
```

**Use when:** Regular reporting (trips per hour, daily revenue summaries, any fixed periodic aggregation).

**Homework:** Q4 (5-minute), Q6 (1-hour)

---

### Sliding Windows

```
│←────── 1 hour ──────→│
         │←────── 1 hour ──────→│
                  │←────── 1 hour ──────→│
         ←15min→
```

- Fixed size, overlapping
- One event can belong to multiple windows
- SQL function: `HOP()`

```sql
HOP(TABLE events, DESCRIPTOR(event_timestamp),
    INTERVAL '15' MINUTE,   -- slide (how often a new window starts)
    INTERVAL '1' HOUR)      -- window size
```

**Use when:** Peak detection, moving averages, surge pricing signals — finding the highest demand concentration regardless of where it falls on the clock.

**Why overlap matters:** A surge from 14:30–15:30 would be split across two tumbling windows and the true peak missed. Sliding windows check every possible 1-hour combination.

**Homework:** Not required in Q4–Q6, but good to understand.

---

### Session Windows

```
Events:  ●●●●    (silence > 5 min)    ●●●●●●●    (silence > 5 min)   ●●
          └── Session 1 ──┘            └──── Session 2 ────┘           └─ S3
```

- Dynamic size — no fixed duration
- Window stays open as long as events keep arriving
- Closes after a gap of inactivity longer than the timeout
- SQL function: `SESSION()`

```sql
SESSION(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
```

**Use when:** User/driver session analysis, burst detection, behavioral clustering — any scenario where "activity clusters" matter more than fixed time boundaries.

**Homework:** Q5 — find the PULocationID with the most trips in a single session.

---

### Comparison Table

| | Tumbling | Sliding | Session |
|---|---|---|---|
| Window size | Fixed | Fixed | Dynamic |
| Overlap | None | Yes | None |
| Trigger | Time boundary | Time boundary | Inactivity gap |
| Event belongs to | 1 window | Multiple windows | 1 window |
| SQL function | `TUMBLE()` | `HOP()` | `SESSION()` |
| Best for | Periodic reports | Peak/moving avg | Behavioral clusters |
| Homework | Q4, Q6 | — | Q5 |

---

## 9. The Three-Part Correctness Guarantee

This is the most important concept. Exactly-once correctness requires ALL THREE:

```
┌──────────────────────────────────────────────────────────┐
│                                                          │
│   Checkpointing    +    Watermark    +    PRIMARY KEY    │
│   ─────────────         ─────────         ──────────     │
│   Crash recovery        Window            Late event     │
│   No data loss          trigger           safety net     │
│   No duplicates         (when to          (upsert on     │
│   on restart            emit results)     revision)      │
│                                                          │
│   Remove ANY ONE → correctness breaks SILENTLY           │
└──────────────────────────────────────────────────────────┘
```

| Missing piece | What breaks |
|---|---|
| No checkpointing | Crash causes data loss or double-counting |
| No watermark | Windows never close, nothing written to PostgreSQL |
| No PRIMARY KEY | Late events create duplicate rows, inflated totals |

How each is configured in the job:

```python
env.enable_checkpointing(10_000)     # checkpointing ← in Python

# watermark ← in source DDL
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND

# PRIMARY KEY ← in sink DDL
PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
```

---

## 10. Job Submission

### The Submit Command

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/aggregation_job.py \
    --pyFiles /opt/src \
    -d
```

| Flag | Purpose |
|---|---|
| `-py /opt/src/job/aggregation_job.py` | Python entry point (path inside container) |
| `--pyFiles /opt/src` | Bundle entire src/ directory — required so `from models import ...` works inside Task Manager |
| `-d` | Detached — return job ID immediately |

**Without `--pyFiles`:**
```
ModuleNotFoundError: No module named 'models'
```
The Task Manager runs in a separate process and cannot see your local Python files unless they are explicitly bundled.

---

### Verify the Job is Running

Open **http://localhost:8081**:
- Running Jobs: job with status `RUNNING`
- Checkpoints tab: count incrementing every ~10 seconds, status `COMPLETED`

If status is `FAILED`, click the job → Exceptions tab → read the root cause.

---

### Jobs Do Not Persist Across Restarts

Every `docker compose down` removes the containers. Flink jobs are gone. On restart:

```bash
docker compose up -d
# Flink UI shows: Running Jobs = 0
# Resubmit every job you need
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/your_job.py \
    --pyFiles /opt/src -d
```

---

## 11. Cleanup

### Two Commands, Very Different Consequences

```bash
# Soft stop — KEEP PostgreSQL data volume
docker compose down

# Hard stop — PERMANENTLY DELETE PostgreSQL data volume
docker compose down -v
```

**What survives each:**

| | `down` | `down -v` |
|---|---|---|
| Containers | Removed | Removed |
| PostgreSQL data | ✅ Survives | ❌ Gone forever |
| Your .py files | ✅ Survives | ✅ Survives |
| Flink jobs | ❌ Must resubmit | ❌ Must resubmit |
| PostgreSQL tables | ✅ Intact | ❌ Must recreate |

**When to use which:**

```
Want to pause and resume later?    → docker compose down
Something went wrong, fresh start? → docker compose down -v
Homework complete, free up disk?   → docker compose down -v
```

**Restart after `docker compose down`:**
```bash
docker compose up -d
# Resubmit Flink jobs
# Run producer if needed
# PostgreSQL data still intact ✓
```

**Restart after `docker compose down -v`:**
```bash
docker compose up --build -d
# Recreate PostgreSQL table (Step E)
# Run producer exactly once
# Resubmit Flink job
```

---

## 12. Common Gotchas Checklist

| Symptom | Root Cause | Fix |
|---|---|---|
| Job starts but processes 0 events | `localhost:9092` used inside Flink | Use `redpanda:29092` in DDL |
| `ModuleNotFoundError: No module named 'models'` | Missing `--pyFiles` flag | Add `--pyFiles /opt/src` to submit command |
| Windows never appear in PostgreSQL | `latest-offset` with no new data | Change to `earliest-offset` |
| Windows never appear in PostgreSQL | Parallelism > partition count | Set `env.set_parallelism(1)` for 1-partition topic |
| Duplicate rows in result table | Missing PRIMARY KEY on sink | Drop and recreate table with PRIMARY KEY |
| `PermissionError` writing to src/ | Docker created src/ as root | `sudo chown -R $(whoami) src/` |
| PyFlink import errors in container | Python 3.13 on host | Pin to 3.12 in `.python-version` |
| Wrong trip counts (doubled) | Producer ran twice | Delete topic, recreate, run producer once |
| Flink job shows `FAILED` immediately | Stale image or missing JAR | `docker compose up --build -d` |
| Container shows `Exiting` | Out of memory (~3GB needed) | Increase Docker Desktop memory limit |

---

## 13. Quick Reference — SQL Patterns

### Source Table — String Timestamp (Green Taxi)

```sql
CREATE TABLE green_trips_source (
    lpep_pickup_datetime  VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID          INT,
    DOLocationID          INT,
    passenger_count       INT,
    trip_distance         DOUBLE,
    tip_amount            DOUBLE,
    total_amount          DOUBLE,
    -- computed column from string timestamp
    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'properties.bootstrap.servers'  = 'redpanda:29092',
    'topic'                         = 'green-trips',
    'scan.startup.mode'             = 'earliest-offset',
    'properties.auto.offset.reset'  = 'earliest',
    'format'                        = 'json'
);
```

### Source Table — Epoch Milliseconds (Yellow Taxi)

```sql
event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

### Tumbling Window Query

```sql
INSERT INTO result_table
SELECT
    window_start,
    PULocationID,
    COUNT(*) AS num_trips,
    SUM(total_amount) AS total_revenue
FROM TABLE(
    TUMBLE(TABLE source_table, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
)
GROUP BY window_start, PULocationID;
```

### Sliding Window Query

```sql
FROM TABLE(
    HOP(TABLE source_table, DESCRIPTOR(event_timestamp),
        INTERVAL '15' MINUTE,   -- slide
        INTERVAL '1' HOUR)      -- size
)
```

### Session Window Query

```sql
FROM TABLE(
    SESSION(TABLE source_table, DESCRIPTOR(event_timestamp),
            INTERVAL '5' MINUTE)   -- gap
)
GROUP BY window_start, PULocationID;
```

### Sink Table with Upsert

```sql
CREATE TABLE result_table (
    window_start  TIMESTAMP(3),
    PULocationID  INT,
    num_trips     BIGINT,
    total_revenue DOUBLE,
    PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'result_table',
    'username'   = 'postgres',
    'password'   = 'postgres',
    'driver'     = 'org.postgresql.Driver'
);
```

### Job Boilerplate

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)   # match your topic's partition count

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # register source and sink tables
    # ...

    t_env.execute_sql("INSERT INTO sink SELECT ... FROM source").wait()

if __name__ == '__main__':
    main()
```

### Submit Command

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/your_job.py \
    --pyFiles /opt/src \
    -d
```

---

*Reference guide compiled from DataTalksClub Data Engineering Zoomcamp — Module 7 Workshop and Homework.*
*Stack: Python 3.12 · PyFlink 2.2 · Redpanda · PostgreSQL · Docker Compose*
