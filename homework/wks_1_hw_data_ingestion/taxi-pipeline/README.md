# 🚕 NYC Taxi Data Pipeline — dlt Workshop Homework

> Data Engineering Zoomcamp 2026 | Workshop: Data Ingestion with dlt

## What I Built

A full data ingestion pipeline that loads NYC Yellow Taxi trip data from a custom paginated REST API into DuckDB using **dlt (data load tool)**, with AI-assisted development via **dlt MCP Server** and **Cline**.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| `dlt` | Data ingestion pipeline |
| `DuckDB` | Local data warehouse |
| `uv` | Python package manager |
| `Marimo` | Notebook for queries & visualization |
| `Cline + Gemini 2.5 Flash` | AI Agent with dlt MCP Server |
| `VS Code` | IDE |

---

## Data Source

| Property | Value |
|----------|-------|
| Base URL | `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` |
| Format | Paginated JSON |
| Page Size | 1,000 records per page |
| Total Records | 10,000 rides |
| Pagination | Stops when empty page returned |

---

## Pipeline Code

```python
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "/",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                    "params": {
                        "page": 1,
                    },
                },
            }
        ],
    }
    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
    refresh="drop_sources",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_source())
    print(load_info)
```

---

## How to Run

```bash
# 1. Install dependencies
uv add "dlt[duckdb]" "dlt[workspace]" marimo

# 2. Run the pipeline
uv run python taxi_pipeline.py

# 3. View dashboard
uv run dlt pipeline taxi_pipeline show

# 4. Open Marimo notebook
uv run marimo edit notebook.py
```

---

## Homework Answers

### Question 1: Start and End Date of the Dataset

```sql
SELECT 
    MIN(trip_pickup_date_time) AS start_date,
    MAX(trip_pickup_date_time) AS end_date
FROM taxi_data.rides
```

✅ **Answer: 2009-06-01 to 2009-07-01**

---

### Question 2: Proportion of Trips Paid with Credit Card

```sql
SELECT 
    ROUND(COUNT(*) FILTER (WHERE payment_type = 'Credit') * 100.0 / COUNT(*), 2) AS credit_card_pct
FROM taxi_data.rides
```

✅ **Answer: 26.66%**

---

### Question 3: Total Amount of Money Generated in Tips

```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips 
FROM taxi_data.rides
```

✅ **Answer: $6,063.41**

---

## What I Learned

✅ Build REST API data pipelines with dlt  
✅ Use AI-assisted development with dlt MCP Server  
✅ Load paginated API data into DuckDB  
✅ Inspect pipeline data with dlt Dashboard and Marimo notebooks  
✅ Set up Cline + Gemini 2.5 Flash as AI Agent in VS Code  
✅ Use `uv` as a modern Python package manager  

Built a full NYC taxi data pipeline from a custom API — AI-assisted data engineering is the future! 🚀

---

## Project Structure

```
taxi-pipeline/
├── .dlt/                    # dlt config
├── .gemini/                 # Gemini MCP config
├── .venv/                   # Virtual environment (uv)
├── .vscode/                 # VS Code settings
├── notebook.py              # Marimo notebook
├── pyproject.toml           # Project dependencies
├── taxi_pipeline.duckdb     # Local DuckDB database
├── taxi_pipeline.py         # Main pipeline code
└── uv.lock                  # Locked dependencies
```
