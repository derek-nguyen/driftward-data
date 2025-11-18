# Getting Started

## Prerequisites

- Python 3.10-3.13
- [uv](https://docs.astral.sh/uv/) package manager
- Google Cloud credentials configured for BigQuery access

## Installation

1. Install `uv` following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/)

2. Clone the repository and navigate to the project directory

3. Create virtual environment and install dependencies:
```bash
uv sync
```

4. Activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS/Linux | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate` |

5. Verify setup:
```bash
dg check defs
```

## Running Dagster

Start the Dagster UI web server:
```bash
dg dev
```

Open http://localhost:3000 in your browser.

## Project Structure

```
src/driftward_data/
├── definitions.py          # Main entry point (auto-discovers defs/)
└── defs/
    ├── __init__.py
    ├── assets.py           # Asset definitions
    └── resources.py        # Resource definitions (BigQuery, etc.)
```

This project uses `load_from_defs_folder` for automatic discovery - no manual registration needed.

## Development Guide

### Asset Naming Conventions

Follow these naming conventions for data warehouse layers:

| Prefix | Layer | Purpose | Example |
| --- | --- | --- | --- |
| `raw_` | Raw/Bronze | Direct from source, minimal transformation | `raw_sba_foia_7a_loans` |
| `stg_` | Staging/Silver | Cleaned, typed, validated | `stg_sba_foia_7a_loans` |
| `fct_` | Fact | Transactional/event data with business logic | `fct_sba_loans` |
| `dim_` | Dimension | Entity/lookup tables | `dim_sba_lenders` |
| `agg_` | Aggregate | Pre-aggregated metrics | `agg_sba_loans_by_state` |
| `mart_` | Data Mart | Business-facing denormalized tables | `mart_sba_loan_analytics` |

**Naming Pattern:**
- Use `_` (underscore) to separate words within a name
- Asset names should be descriptive and indicate the data layer
- Example: `raw_sba_foia_7a_loans` → BigQuery table `sba_loans.raw_sba_foia_7a_loans`

**Example Pipeline:**
```
sba_foia_raw (GCS upload)
    ↓
raw_sba_foia_7a_loans (raw CSV → BigQuery)
    ↓
stg_sba_loans (cleaned & validated)
    ↓
fct_sba_loans (enriched fact table)
```

### Adding New Assets

Create assets in `src/driftward_data/defs/assets.py`:

```python
import dagster as dg
from dagster_gcp import BigQueryResource
import pandas as pd

@dg.asset
def raw_my_data_table(
    context: dg.AssetExecutionContext,
    bigquery: BigQueryResource
) -> None:
    """Load raw data from source to BigQuery.

    BigQuery table: my_dataset.raw_my_data_table
    """
    query = "SELECT * FROM source_table"
    with bigquery.get_client() as client:
        # ... load logic ...

    # Add profiling metadata (convert numpy/pandas types to Python types!)
    context.add_output_metadata({
        "total_rows": dg.MetadataValue.int(int(row_count)),
        "data_quality_null_percentage": dg.MetadataValue.float(float(null_pct)),
    })
```

The asset is automatically discovered - no changes to `definitions.py` required.

### Adding New Resources

Update `src/driftward_data/defs/resources.py`:

```python
import dagster as dg
from dagster_gcp import BigQueryResource

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "bigquery": BigQueryResource(project="your-project-id"),
            # Add new resources here:
            "new_resource": NewResourceClass(...),
        }
    )
```

Resources are injected into assets by matching parameter names to resource keys.

Reference: [Dagster Resources Documentation](https://docs.dagster.io/dagster-basics-tutorial/resources)

### Using AssetExecutionContext

The `AssetExecutionContext` provides runtime information and capabilities for your assets.

**When to use it:**
- You need logging (`context.log.info()`, `context.log.warning()`, etc.)
- You want to add metadata to asset runs (`context.add_output_metadata()`)
- You need runtime information (run ID, asset key, partition info)
- You're implementing complex orchestration logic

**When NOT to use it:**
- Simple data transformations that don't need logging
- Pure functions that only transform inputs to outputs
- Assets that only need resource dependencies

**Example with context:**
```python
import dagster as dg

@dg.asset
def sba_foia_raw(
    context: dg.AssetExecutionContext,
    gcs: GCSResource
) -> dict:
    """Downloads SBA FOIA data with logging and metadata."""
    context.log.info(f"Starting download from {URL}")

    # ... processing logic ...

    context.add_output_metadata({
        "total_files": len(files),
        "total_size_bytes": total_size,
    })

    return metadata
```

**Example without context:**
```python
@dg.asset
def simple_transform(bigquery: BigQueryResource) -> pd.DataFrame:
    """Simple query - no logging needed."""
    with bigquery.get_client() as client:
        return client.query("SELECT * FROM table").to_dataframe()
```

**Common context properties:**
- `context.run.run_id` - Unique identifier for this execution
- `context.asset_key` - Key of the asset being materialized
- `context.partition_key` - Partition being processed (if partitioned)
- `context.log` - Logger instance for this run

Reference: [AssetExecutionContext Documentation](https://docs.dagster.io/concepts/assets/software-defined-assets#assetexecutioncontext)

### Data Profiling Best Practices

Add comprehensive profiling metadata to your assets for better observability:

**Essential Metadata Types:**
```python
dg.MetadataValue.int()     # Row counts, column counts
dg.MetadataValue.float()   # Statistics, percentages
dg.MetadataValue.text()    # Simple strings (table names, dates)
dg.MetadataValue.md()      # Formatted markdown (distributions, lists)
```

**Important: Type Conversion**

Always convert numpy/pandas types to Python native types:

```python
# ❌ WRONG - Will cause SerializationError
total_rows = len(df)  # numpy.int64
null_pct = round(pct, 2)  # numpy.float64

# ✅ CORRECT - Convert to Python types
total_rows = int(len(df))
null_pct = float(round(pct, 2))
total_amount = float(df['amount'].sum())

# For distributions
state_dist = {str(k): int(v) for k, v in df['state'].value_counts().items()}
```

**Recommended Profiling Metrics:**

1. **Volume**: Total rows, columns, file counts
2. **Data Quality**: Null percentages, key column null counts
3. **Temporal**: Date ranges (min/max), as-of dates
4. **Statistics**: Min/max/mean/median for numeric columns
5. **Distributions**: Top N categories, status distributions

**Example:**
```python
context.add_output_metadata({
    # Volume
    "total_rows": dg.MetadataValue.int(int(len(df))),
    "total_columns": dg.MetadataValue.int(len(df.columns)),

    # Quality
    "data_quality_null_percentage": dg.MetadataValue.float(float(null_pct)),

    # Temporal
    "approval_date_range": dg.MetadataValue.text(f"{min_date} to {max_date}"),

    # Statistics
    "loan_amount_mean": dg.MetadataValue.float(float(df['amount'].mean())),

    # Distributions (as markdown)
    "top_10_states": dg.MetadataValue.md(
        "\n".join([f"{i+1}. **{state}**: {count:,}"
                   for i, (state, count) in enumerate(state_dist.items())])
    ),

    # Destination
    "bigquery_table": dg.MetadataValue.text(table_id),
})
```

Reference: [Dagster Metadata Documentation](https://docs.dagster.io/concepts/metadata-tags/asset-metadata)

## Common Commands

| Command | Description |
| --- | --- |
| `dg dev` | Start local Dagster UI server |
| `dg check defs` | Validate all definitions |
| `dg list defs` | List all assets and resources |
| `dg scaffold defs dagster.asset <path>` | Scaffold a new asset |
| `dagster run list` | List recent runs |
| `dagster run logs <RUN_ID>` | View logs for a specific run |

## Troubleshooting

### Resource not found error
Ensure the parameter name in your asset matches the key in `resources.py`:
```python
# In resources.py
resources={"bigquery": BigQueryResource(...)}

# In assets.py - parameter name must match
def my_asset(bigquery: BigQueryResource):  # ✅ Matches "bigquery" key
```

### Definition validation fails
Run with verbose output:
```bash
dg check defs --verbose
```

### BigQuery authentication issues
Ensure Google Cloud credentials are configured:
```bash
gcloud auth application-default login
```

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
- [dg CLI Documentation](https://docs.dagster.io/guides/dg)
