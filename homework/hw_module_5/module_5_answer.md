# Module 5 Homework Answers — Data Platforms with Bruin

---

## Question 1. Bruin Pipeline Structure

**Answer: `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`**

A Bruin project requires:
- `.bruin.yml` — defines environments and connections (where data is stored)
- `pipeline/pipeline.yml` — defines pipeline name, schedule, variables, default connections
- `pipeline/assets/` — contains all asset files (SQL, Python, YAML)

```
my-pipeline/
├── .bruin.yml          ← required: connections config
└── pipeline/
    ├── pipeline.yml    ← required: pipeline config
    └── assets/         ← required: all assets live here
```

Reference: https://getbruin.com/docs/bruin/getting-started/pipeline.html

---

## Question 2. Materialization Strategies

**Answer: `time_interval` — incremental based on a time column**

`time_interval` is the correct strategy for NYC taxi data because:
- Data is organized by month based on `pickup_datetime`
- It **deletes rows in the time window** then **re-inserts** the query result
- Allows safe re-processing of specific date ranges without full refresh
- Used in both `staging.trips` and `reports.trips_report`

```yaml
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
```

| Strategy | Behavior |
|---|---|
| `append` | Add new rows only, never touch existing |
| `replace` | Truncate and rebuild entirely |
| `time_interval` ✅ | Delete rows in date range, then re-insert |
| `view` | Virtual table, no data stored |

---

## Question 3. Pipeline Variables

**Answer: `bruin run --var 'taxi_types=["yellow"]'`**

Pipeline variables defined as arrays must be passed as JSON arrays:

```bash
bruin run --var 'taxi_types=["yellow"]'
```

In your Python asset, the variable is read like this:
```python
import json, os
taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])
```

The other options are wrong because:
- `--taxi-types yellow` — not a valid Bruin flag
- `--var taxi_types=yellow` — missing JSON array format `["yellow"]`
- `--set` — not a valid Bruin flag

---

## Question 4. Running with Dependencies

**Answer: `bruin run ingestion/trips.py --downstream`**

To run a specific asset plus all assets that depend on it (downstream):

```bash
bruin run pipeline/assets/ingestion/trips.py --downstream
```

This will run in order:
1. `ingestion.trips` (the asset you specified)
2. `staging.trips` (depends on ingestion.trips)
3. `reports.trips_report` (depends on staging.trips)

The other options are wrong because:
- `--all` — not a valid Bruin flag
- `--recursive` — not a valid Bruin flag
- `--select ingestion.trips+` — that's dbt syntax, not Bruin

---

## Question 5. Quality Checks

**Answer: `name: not_null`**

To ensure `pickup_datetime` never has NULL values:

```yaml
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    checks:
      - name: not_null   ✅
```

Available built-in checks:
| Check | Purpose |
|---|---|
| `not_null` ✅ | Column must have no NULL values |
| `unique` | Column must have no duplicate values |
| `positive` | Values must be > 0 |
| `non_negative` | Values must be >= 0 |
| `accepted_values` | Values must be from a defined list |

The other options are wrong because:
- `unique` — prevents duplicates, not NULLs
- `positive` — checks numeric values > 0, not NULLs
- `accepted_values: [not_null]` — invalid syntax

---

## Question 6. Lineage and Dependencies

**Answer: `bruin lineage`**

To visualize the dependency graph between assets:

```bash
bruin lineage pipeline/assets/ingestion/trips.py
```

This shows:
- What the asset **depends on** (upstream)
- What **depends on** the asset (downstream)

Example output for `staging.trips`:
```
Upstream:   ingestion.trips
            ingestion.payment_lookup
Downstream: reports.trips_report
```

Reference: https://getbruin.com/docs/bruin/commands/lineage.html

---

## Question 7. First-Time Run

**Answer: `--full-refresh`**

When running a Bruin pipeline for the first time on a new database:

```bash
bruin run ./pipeline/pipeline.yml --full-refresh
```

`--full-refresh`:
- Creates tables from scratch if they don't exist
- Processes data from `start_date` in `pipeline.yml` to now
- Truncates and rebuilds existing tables

The other options are wrong because:
- `--create` — not a valid Bruin flag
- `--init` — not a valid Bruin flag
- `--truncate` — not a valid Bruin flag

---

## Summary

| Question | Answer |
|---|---|
| Q1. Pipeline structure | `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/` |
| Q2. Materialization strategy | `time_interval` |
| Q3. Override pipeline variable | `bruin run --var 'taxi_types=["yellow"]'` |
| Q4. Run with downstream | `bruin run ingestion/trips.py --downstream` |
| Q5. Quality check for NULLs | `name: not_null` |
| Q6. Visualize dependencies | `bruin lineage` |
| Q7. First-time run flag | `--full-refresh` |
