from dagster import asset
from dagster_gcp import BigQueryResource
import pandas as pd


@asset
def sba_loans_sample(bigquery: BigQueryResource) -> pd.DataFrame:
    """Sample of SBA 7a loans from BigQuery."""
    query = """
        SELECT *
        FROM `learned-mind-476504-i3.sba_clean.loans_7a`
        LIMIT 100
    """

    with bigquery.get_client() as client:
        df = client.query(query).to_dataframe()

    return df
