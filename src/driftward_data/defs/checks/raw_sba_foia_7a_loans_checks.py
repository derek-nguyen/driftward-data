"""Asset checks for raw_sba_foia_7a_loans table."""

import dagster as dg
from dagster_gcp import BigQueryResource

from ..assets.raw_sba_foia_7a_loans import raw_sba_foia_7a_loans


@dg.asset_check(asset=raw_sba_foia_7a_loans)
def check_raw_as_of_date_not_null(
    context: dg.AssetCheckExecutionContext,
    bigquery: BigQueryResource,
) -> dg.AssetCheckResult:
    """Ensure AsOfDate column has no null values in raw table."""
    from google.cloud import bigquery as bq

    with bigquery.get_client() as bq_client:
        dataset_id = "sba_loans"
        table_id = f"{bq_client.project}.{dataset_id}.raw_sba_foia_7a_loans"

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(AsOfDate IS NULL) as null_count
        FROM `{table_id}`
        """

        result = bq_client.query(query).to_dataframe().iloc[0]
        total_rows = int(result['total_rows'])
        null_count = int(result['null_count'])

        passed = null_count == 0

        return dg.AssetCheckResult(
            passed=passed,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_count": dg.MetadataValue.int(null_count),
                "null_percentage": dg.MetadataValue.float(
                    float(null_count / total_rows * 100) if total_rows > 0 else 0.0
                ),
            },
            description=f"Found {null_count:,} rows ({null_count/total_rows*100:.2f}%) with null AsOfDate in raw table - these rows will be excluded from staging"
            if not passed
            else f"All {total_rows:,} rows in raw table have valid AsOfDate values",
        )
