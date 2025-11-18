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

### Adding New Assets

Create assets in `src/driftward_data/defs/assets.py`:

```python
from dagster import asset
from dagster_gcp import BigQueryResource
import pandas as pd

@asset
def my_new_asset(bigquery: BigQueryResource) -> pd.DataFrame:
    """Description of what this asset does."""
    query = "SELECT * FROM my_table LIMIT 100"
    with bigquery.get_client() as client:
        return client.query(query).to_dataframe()
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
