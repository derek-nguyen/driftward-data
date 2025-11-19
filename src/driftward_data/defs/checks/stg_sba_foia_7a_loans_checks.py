"""Asset checks for stg_sba_foia_7a_loans table."""

import dagster as dg
from dagster_gcp import BigQueryResource
import pandas as pd

from ..assets.stg_sba_foia_7a_loans import stg_sba_foia_7a_loans


@dg.asset_check(asset=stg_sba_foia_7a_loans)
def check_as_of_date_not_null(
    context: dg.AssetCheckExecutionContext,
    bigquery: BigQueryResource,
) -> dg.AssetCheckResult:
    """Ensure as_of_date column has no null values in latest partition."""
    from google.cloud import bigquery as bq

    with bigquery.get_client() as bq_client:
        dataset_id = "sba_loans"
        table_id = f"{bq_client.project}.{dataset_id}.stg_sba_foia_7a_loans"

        # Get latest as_of_date
        latest_date_query = f"""
        SELECT MAX(as_of_date) as latest_as_of_date
        FROM `{table_id}`
        """
        latest_date_df = bq_client.query(latest_date_query).to_dataframe()
        latest_as_of_date = latest_date_df.iloc[0]['latest_as_of_date']

        # If no valid as_of_date exists, fail the check
        if pd.isna(latest_as_of_date):
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={},
                description="Staging table has no valid as_of_date values - cannot perform check",
            )

        # Check nulls only in latest partition
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(as_of_date IS NULL) as null_count
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date OR as_of_date IS NULL
        """

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter("latest_as_of_date", "DATE", latest_as_of_date)
            ]
        )

        result = bq_client.query(query, job_config=job_config).to_dataframe().iloc[0]
        total_rows = int(result['total_rows'])
        null_count = int(result['null_count'])

        passed = null_count == 0
        severity = dg.AssetCheckSeverity.ERROR if not passed else dg.AssetCheckSeverity.WARN

        return dg.AssetCheckResult(
            passed=passed,
            severity=severity,
            metadata={
                "latest_as_of_date": dg.MetadataValue.text(str(latest_as_of_date)),
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_count": dg.MetadataValue.int(null_count),
                "null_percentage": dg.MetadataValue.float(
                    float(null_count / total_rows * 100) if total_rows > 0 else 0.0
                ),
            },
            description=f"Found {null_count:,} null as_of_date values in latest partition ({latest_as_of_date})"
            if not passed
            else f"All {total_rows:,} rows in latest partition ({latest_as_of_date}) have valid as_of_date values",
        )


@dg.asset_check(asset=stg_sba_foia_7a_loans)
def check_approval_date_not_null(
    context: dg.AssetCheckExecutionContext,
    bigquery: BigQueryResource,
) -> dg.AssetCheckResult:
    """Ensure approval_date column has no null values in latest partition."""
    from google.cloud import bigquery as bq

    with bigquery.get_client() as bq_client:
        dataset_id = "sba_loans"
        table_id = f"{bq_client.project}.{dataset_id}.stg_sba_foia_7a_loans"

        # Get latest as_of_date
        latest_date_query = f"""
        SELECT MAX(as_of_date) as latest_as_of_date
        FROM `{table_id}`
        """
        latest_date_df = bq_client.query(latest_date_query).to_dataframe()
        latest_as_of_date = latest_date_df.iloc[0]['latest_as_of_date']

        # If no valid as_of_date exists, fail the check
        if pd.isna(latest_as_of_date):
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={},
                description="Staging table has no valid as_of_date values - cannot perform check",
            )

        # Check approval_date nulls only in latest partition
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(approval_date IS NULL) as null_count
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date
        """

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter("latest_as_of_date", "DATE", latest_as_of_date)
            ]
        )

        result = bq_client.query(query, job_config=job_config).to_dataframe().iloc[0]
        total_rows = int(result['total_rows'])
        null_count = int(result['null_count'])

        passed = null_count == 0

        return dg.AssetCheckResult(
            passed=passed,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "latest_as_of_date": dg.MetadataValue.text(str(latest_as_of_date)),
                "total_rows": dg.MetadataValue.int(total_rows),
                "null_count": dg.MetadataValue.int(null_count),
                "null_percentage": dg.MetadataValue.float(
                    float(null_count / total_rows * 100) if total_rows > 0 else 0.0
                ),
            },
            description=f"Found {null_count:,} null approval_date values in latest partition ({latest_as_of_date})"
            if not passed
            else f"All {total_rows:,} rows in latest partition ({latest_as_of_date}) have valid approval_date values",
        )


@dg.asset_check(asset=stg_sba_foia_7a_loans)
def check_gross_approval_positive(
    context: dg.AssetCheckExecutionContext,
    bigquery: BigQueryResource,
) -> dg.AssetCheckResult:
    """Ensure gross_approval amounts are positive in latest partition."""
    from google.cloud import bigquery as bq

    with bigquery.get_client() as bq_client:
        dataset_id = "sba_loans"
        table_id = f"{bq_client.project}.{dataset_id}.stg_sba_foia_7a_loans"

        # Get latest as_of_date
        latest_date_query = f"""
        SELECT MAX(as_of_date) as latest_as_of_date
        FROM `{table_id}`
        """
        latest_date_df = bq_client.query(latest_date_query).to_dataframe()
        latest_as_of_date = latest_date_df.iloc[0]['latest_as_of_date']

        # If no valid as_of_date exists, fail the check
        if pd.isna(latest_as_of_date):
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={},
                description="Staging table has no valid as_of_date values - cannot perform check",
            )

        # Check positive amounts only in latest partition
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(gross_approval IS NOT NULL AND gross_approval <= 0) as invalid_count
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date
        """

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter("latest_as_of_date", "DATE", latest_as_of_date)
            ]
        )

        result = bq_client.query(query, job_config=job_config).to_dataframe().iloc[0]
        total_rows = int(result['total_rows'])
        invalid_count = int(result['invalid_count'])

        passed = invalid_count == 0

        return dg.AssetCheckResult(
            passed=passed,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "latest_as_of_date": dg.MetadataValue.text(str(latest_as_of_date)),
                "total_rows": dg.MetadataValue.int(total_rows),
                "invalid_count": dg.MetadataValue.int(invalid_count),
                "invalid_percentage": dg.MetadataValue.float(
                    float(invalid_count / total_rows * 100) if total_rows > 0 else 0.0
                ),
            },
            description=f"Found {invalid_count:,} loans with non-positive gross_approval amounts in latest partition ({latest_as_of_date})"
            if not passed
            else f"All {total_rows:,} loan amounts in latest partition ({latest_as_of_date}) are positive",
        )
