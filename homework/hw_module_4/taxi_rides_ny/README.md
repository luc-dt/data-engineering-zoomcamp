# Module 4 Homework: Analytics Engineering with dbt
## DataTalks.Club Data Engineering Zoomcamp 2026

---

## 🚀 What I Built

Just finished Module 4 - Analytics Engineering with dbt. Learned how to:

✅ Build transformation models with dbt  
✅ Create staging, intermediate, and fact tables  
✅ Write tests to ensure data quality  
✅ Understand lineage and model dependencies  
✅ Analyze revenue patterns across NYC zones  

Transforming raw data into analytics-ready models - the T in ELT!

---

## 📋 Table of Contents

1. [Project Purpose](#1-project-purpose)
2. [Stack and Tools](#2-stack-and-tools)
3. [Project Structure](#3-project-structure)
4. [Architecture and Data Flow](#4-architecture-and-data-flow)
5. [Setup and Workflow](#5-setup-and-workflow)
6. [Model Descriptions](#6-model-descriptions)
7. [Tests](#7-tests)
8. [Key Concepts Learned](#8-key-concepts-learned)
9. [Homework Answers](#9-homework-answers)
10. [Bugs Fixed](#10-bugs-fixed)

---

## 1. Project Purpose

### What Problem Does This Solve?

Raw NYC taxi trip data arrives in the warehouse as:
- Messy column names (`VendorID`, `lpep_pickup_datetime`)
- No unique trip identifiers
- Duplicate records from vendor systems
- Two separate tables (green + yellow) with slightly different schemas
- Integer codes instead of human-readable values (`payment_type = 1`)
- No documentation, no tests, no version control on transformations

After dbt transforms it, analysts get:
- Clean `snake_case` column names
- One stable unique `trip_id` per trip
- Zero duplicates
- Green + yellow unified in one fact table
- Human-readable payment descriptions
- Pre-aggregated monthly revenue by zone — ready for dashboards

### The ELT Pattern

```
Extract          Load              Transform
(Kestra)    →   (BigQuery/DuckDB)  →  (dbt — this project)
                raw data               clean, tested tables
```

dbt only does the **T** in ELT. It never touches ingestion or scheduling.

---

## 2. Stack and Tools

| Component | Tool | Purpose |
|---|---|---|
| Transformation | dbt Core 1.11.7 | SQL model management |
| Dev warehouse | DuckDB (local) | Free, fast local development |
| Prod warehouse | BigQuery (GCP) | Production queries + homework |
| Cross-db functions | dbt_utils 1.3.3 | Surrogate key generation |
| Code generation | codegen 0.14.0 | Auto-generate schema YAML |
| Version control | Git | Track all SQL changes |

### Why Two Warehouses?

```
--target dev   → DuckDB on laptop  → FREE, 1-month data slice
--target prod  → BigQuery on GCP   → Costs money, full 2019-2020 data
```

**Rule: Always develop on dev, only run prod for final answers.**

---

## 3. Project Structure

```
taxi_rides_ny/
│
├── dbt_project.yml              ← project config, materialization defaults
├── packages.yml                 ← dbt_utils + codegen dependencies
├── package-lock.yml             ← locked package versions
│
├── seeds/
│   ├── taxi_zone_lookup.csv     ← 265 NYC taxi zones (static reference)
│   └── payment_type_lookup.csv  ← 7 payment type codes
│
├── macros/
│   ├── get_trip_duration_minutes.sql  ← cross-db DATEDIFF wrapper
│   ├── get_vendor_data.sql            ← vendor_id → vendor name mapping
│   └── safe_cast.sql                  ← cross-db safe casting
│
├── models/
│   ├── staging/
│   │   ├── sources.yml              ← raw data source declarations
│   │   ├── schema.yml               ← staging tests + docs
│   │   ├── stg_green_tripdata.sql   ← clean green taxi data
│   │   ├── stg_yellow_tripdata.sql  ← clean yellow taxi data
│   │   └── stg_fhv_tripdata.sql     ← clean FHV trip data (Q6)
│   │
│   ├── intermediate/
│   │   ├── schema.yml
│   │   ├── int_trips_unioned.sql    ← green + yellow combined
│   │   └── int_trips.sql            ← deduped + enriched trips
│   │
│   └── marts/
│       ├── schema.yml
│       ├── dim_zones.sql            ← zone dimension table
│       ├── dim_vendors.sql          ← vendor dimension table
│       ├── fct_trips.sql            ← core fact table (incremental)
│       └── reporting/
│           ├── schema.yml
│           └── fct_monthly_zone_revenue.sql  ← homework Q3/Q4/Q5
│
└── tmp/                         ← DuckDB temp directory (Windows fix)
```

### Materialization Strategy

```yaml
staging:      +materialized: view    # no data stored — just saved SQL
intermediate: +materialized: table   # persisted — expensive ops done once
marts:        +materialized: table   # what analysts query
fct_trips:    materialized: incremental  # only new rows per run
```

---

## 4. Architecture and Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│                   RAW LAYER (prod schema)                     │
│  prod.green_tripdata    (7.78M rows, 2019-2020)              │
│  prod.yellow_tripdata   (raw yellow trips)                    │
│  prod.fhv_tripdata      (43M rows, 2019 FHV trips)           │
└────────────────────────┬─────────────────────────────────────┘
                         │  source() function
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                STAGING LAYER (VIEWs)                          │
│  stg_green_tripdata  → rename + cast + filter nulls          │
│  stg_yellow_tripdata → same + hardcode trip_type=1           │
│  stg_fhv_tripdata    → rename + filter dispatching_base_num  │
└────────────────────────┬─────────────────────────────────────┘
                         │  ref() function
                         ▼
┌──────────────────────────────────────────────────────────────┐
│              INTERMEDIATE LAYER (TABLEs)                      │
│  int_trips_unioned  → UNION ALL green + yellow               │
│  int_trips          → surrogate key + dedup + payment enrich │
└────────────────────────┬─────────────────────────────────────┘
                         │  ref() function
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                 MARTS LAYER (TABLEs)                          │
│  dim_zones          → 265 NYC taxi zones                     │
│  dim_vendors        → vendor names from fct_trips            │
│  fct_trips          → core fact (INCREMENTAL, joins zones)   │
│  fct_monthly_zone_revenue → aggregated for dashboards        │
└──────────────────────────────────────────────────────────────┘
```

### Why This Order?

Each layer has ONE job and depends on the previous:
- **Staging** — clean the raw data, nothing else
- **Intermediate** — combine and deduplicate, nothing else
- **Marts** — serve analytics-ready output, nothing else

The `ref()` function wires dependencies automatically. dbt builds in the correct order every time.

---

## 5. Setup and Workflow

### Prerequisites

1. Python 3.10+
2. dbt-duckdb installed: `pip install dbt-duckdb`
3. Raw data loaded into DuckDB prod schema (green, yellow, FHV parquet files)
4. `~/.dbt/profiles.yml` configured with dev + prod targets

### Build Commands

```bash
# Step 1: Verify connection
dbt debug --target prod

# Step 2: Install packages
dbt deps

# Step 3: Full production build (run ONCE for homework)
dbt build --target prod

# Step 4: Query answers in DuckDB
duckdb taxi_rides_ny.duckdb "SELECT COUNT(*) FROM prod.fct_monthly_zone_revenue"
```

### Daily Development Loop (Free)

```bash
dbt compile --select my_model       # validate SQL (zero cost)
dbt run --select my_model           # build on DuckDB
dbt test --select my_model          # test on DuckDB
dbt build --target dev              # full pipeline locally
```

### GCP Cost Rules

```
SAFE:    dbt compile, dbt run --target dev, dbt test --select X
CAREFUL: dbt build --target prod  (full pipeline on BigQuery)
DANGER:  dbt run --full-refresh --target prod  (full table rebuild)
```

---

## 6. Model Descriptions

### `stg_green_tripdata` / `stg_yellow_tripdata`
- **What:** Clean, typed staging models from raw taxi tables
- **Why VIEW:** No data stored — downstream TABLE models pull from them on demand
- **Key logic:**
  - Rename `vendorid` → `vendor_id`, `lpep_pickup_datetime` → `pickup_datetime`
  - Cast all columns to correct types
  - Filter `WHERE vendorid IS NOT NULL`
  - Yellow hardcodes `trip_type = 1` (street-hail only by law) and `ehail_fee = 0`

### `stg_fhv_tripdata`
- **What:** Staging model for For-Hire Vehicle trips (2019 only)
- **Key logic:**
  - Rename `PUlocationID` → `pickup_location_id`, `DOlocationID` → `dropoff_location_id`
  - Filter `WHERE dispatching_base_num IS NOT NULL`
  - No `vendor_id` — FHV data has different schema

### `int_trips_unioned`
- **What:** UNION ALL of green + yellow with `service_type` column added
- **Why separate model:** Single responsibility — this model only unions, nothing else
- **Result:** ~8.3M rows (Jan 2019 dev) / full 2019-2020 in prod

### `int_trips`
- **What:** Deduplicated, enriched trip data ready for marts
- **Three jobs:**
  1. Generate `trip_id` surrogate key: `MD5(vendor_id + pickup_datetime + pickup_location_id + service_type)`
  2. Enrich `payment_type` codes with human-readable descriptions via LEFT JOIN
  3. Deduplicate with `QUALIFY row_number() OVER (PARTITION BY ...) = 1`
- **Why LEFT JOIN:** Never drop trips with unknown payment types — keep all trips

### `dim_zones`
- **What:** Zone dimension table — abstraction over `taxi_zone_lookup` seed
- **Why not use seed directly:** Decouples `fct_trips` from the seed source. If data moves to a proper database table, only `dim_zones` changes — `fct_trips` stays the same.

### `fct_trips`
- **What:** Core fact table — one row per trip, enriched with zone names
- **Materialization:** INCREMENTAL (merge strategy)
  - First run: full rebuild
  - Subsequent runs: only `pickup_datetime > MAX(pickup_datetime)` → 160x cheaper on BigQuery
- **Two LEFT JOINs to `dim_zones`:** once for pickup zone, once for dropoff zone

### `fct_monthly_zone_revenue`
- **What:** Pre-aggregated monthly revenue by zone and service type
- **Why pre-aggregate:** Dashboard queries are fast — no need to scan 8M rows per load
- **Grain:** One row per (pickup_zone, month, service_type) combination

---

## 7. Tests

34 automated tests run on every `dbt build`:

| Test | Column | Model |
|---|---|---|
| `unique` | `trip_id`, `location_id` | `int_trips`, `dim_zones`, `fct_trips` |
| `not_null` | `trip_id`, `vendor_id`, `pickup_datetime`, `total_amount` | all layers |
| `accepted_values` | `service_type` in ['Green', 'Yellow'] | `int_trips`, `fct_trips`, `fct_monthly_zone_revenue` |
| `relationships` | `pickup_location_id` → `dim_zones.location_id` | `fct_trips` |
| `unique_combination_of_columns` | `pickup_zone + revenue_month + service_type` | `fct_monthly_zone_revenue` |

**Build result: PASS=46 WARN=0 ERROR=0**

---

## 8. Key Concepts Learned

### `source()` vs `ref()`

```sql
-- Raw warehouse tables:
from {{ source('raw', 'green_tripdata') }}

-- Other dbt models:
from {{ ref('stg_green_tripdata') }}
```

`ref()` builds the dependency graph automatically — dbt knows the build order.

### Surrogate Keys

```sql
{{ dbt_utils.generate_surrogate_key([
    'vendor_id', 'pickup_datetime', 'pickup_location_id', 'service_type'
]) }} as trip_id
```

No single column is unique enough. A hash of 4 columns creates a stable ID that survives rebuilds.

### Incremental Models

```sql
{% if is_incremental() %}
  where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
```

Process only new data. Essential for cost control on BigQuery.

### dbt Lineage

```bash
dbt run --select model_name          # only that model
dbt run --select +model_name         # model + all upstream
dbt run --select model_name+         # model + all downstream
dbt run --select +model_name+        # everything connected
```

---

## 9. Homework Answers

### Q1 — dbt Lineage and Execution

**Question:** If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer: `int_trips_unioned` only**

**Proof:** Running the command without `+` operator builds exactly one model. It does NOT include upstream dependencies (`stg_green_tripdata`, `stg_yellow_tripdata`). Without `+`, if staging views don't exist in the target schema, the run fails with "Table does not exist."

To include upstream: `dbt run --select +int_trips_unioned`

---

### Q2 — dbt Tests

**Question:** A new value `6` appears in `payment_type`. What happens when `dbt test --select fct_trips` runs?

**Answer: dbt will fail the test, returning a non-zero exit code**

**Why:** The `accepted_values` test generates SQL:
```sql
select count(*) from fct_trips
where payment_type not in (1, 2, 3, 4, 5)
-- value 6 is now present → returns rows → TEST FAILS
```

dbt never skips, auto-updates, or warns for `accepted_values`. It's binary — pass or fail.

---

### Q3 — Count of records in `fct_monthly_zone_revenue`

**Query:**
```sql
SELECT COUNT(*) FROM prod.fct_monthly_zone_revenue;
```

**Answer: 12,184**

---

### Q4 — Best Performing Zone for Green Taxis (2020)

**Query:**
```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 3;
```

**Result:**
| pickup_zone | total_revenue |
|---|---|
| East Harlem North | $1,817,494.55 |
| East Harlem South | $1,653,446.11 |
| Central Harlem | $1,097,245.22 |

**Answer: East Harlem North**

---

### Q5 — Green Taxi Trip Counts (October 2019)

**Query:**
```sql
SELECT SUM(total_monthly_trips) as total_trips
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10;
```

**Answer: 384,624**

---

### Q6 — Count of records in `stg_fhv_tripdata`

**Model:** `stg_fhv_tripdata.sql`
```sql
where dispatching_base_num is not null
```

**Query:**
```sql
SELECT COUNT(*) FROM prod.stg_fhv_tripdata;
```

**Answer: 43,244,693**

---

## 10. Bugs Fixed

### Bug 1: DuckDB QUALIFY + Surrogate Key Type Conflict

**Error:**
```
INTERNAL Error: Failed to bind column reference "service_type": 
inequal types (VARCHAR != TIMESTAMP)
```

**Root cause:** DuckDB 1.1.x has an internal binder bug — when `generate_surrogate_key` (which emits `cast(service_type as TEXT)` internally) is combined with `PARTITION BY service_type` in the same query, the optimizer mixes up column positions and throws an assertion failure.

**Fix:** Encode `service_type` as an integer in `PARTITION BY`:
```sql
-- ❌ Fails:
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1

-- ✅ Works:
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id,
                 case when service_type = 'Green' then 0 else 1 end
    order by dropoff_datetime
) = 1
```

### Bug 2: Windows Invalid Temp Directory Path

**Error:**
```
IO Error: Failed to create directory "\\.tmp": The specified path is invalid.
```

**Root cause:** DuckDB derives its spill-to-disk temp directory from the database file path. With an absolute path in `profiles.yml`, it resolves to an invalid Windows path `\\.tmp`.

**Fix:** Add `on-run-start` hook to `dbt_project.yml`:
```yaml
on-run-start:
  - "SET temp_directory = 'D:/your/project/path/tmp'"
```

This fires once per invocation before any model uses disk space — DuckDB never tries to re-set it on subsequent cursors.

### Bug 3: DuckDB File Locked by Python Process

**Error:**
```
IO Error: Cannot open file "taxi_rides_ny.duckdb": 
The process cannot access the file because it is being used by another process.
```

**Fix:** Kill the locking process in PowerShell:
```powershell
Stop-Process -Id <PID> -Force
# or
Get-Process python | Stop-Process -Force
```

### Bug 4: Wrong Filter in `stg_fhv_tripdata`

**Original:** `WHERE pickup_datetime IS NOT NULL`  
**Required by homework:** `WHERE dispatching_base_num IS NOT NULL`  
**Impact:** Different filter = different row count = wrong answer for Q6

---

## Build Summary

```
dbt build --target prod

Total time:    43 minutes 18 seconds
Models built:  9
Seeds loaded:  2
Tests run:     34
Result:        PASS=46  WARN=0  ERROR=0  SKIP=0
```

---


*DataTalks.Club Data Engineering Zoomcamp 2026 — Module 4: Analytics Engineering with dbt*  
*Stack: dbt Core 1.11.7 + DuckDB (dev) + BigQuery (prod)*
