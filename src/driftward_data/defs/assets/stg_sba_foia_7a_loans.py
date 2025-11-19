"""Staging table for SBA 7(a) FOIA loans with cleaned and standardized columns."""

import dagster as dg
from dagster_gcp import BigQueryResource
import pandas as pd

from .raw_sba_foia_7a_loans import raw_sba_foia_7a_loans


@dg.asset(deps=[raw_sba_foia_7a_loans])
def stg_sba_foia_7a_loans(
    context: dg.AssetExecutionContext,
    bigquery: BigQueryResource,
) -> None:
    """Staging table for SBA 7(a) FOIA loans with cleaned and standardized columns.

    Transforms raw SBA 7(a) data into a clean staging table with:
    - Standardized snake_case column names
    - Derived fields (is_default, is_funded, months_to_default, etc.)
    - Proper data types (NUMERIC for currency, STRING for ZIP codes)
    - Enhanced business logic for loan status analysis

    BigQuery table: sba_loans.stg_sba_foia_7a_loans
    Partitioning: MONTH on approval_date
    Clustering: borrower_state, loan_status, naics_code
    """
    from google.cloud import bigquery as bq

    with bigquery.get_client() as bq_client:
        dataset_id = "sba_loans"
        table_id = f"{bq_client.project}.{dataset_id}.stg_sba_foia_7a_loans"

        # SQL transformation query
        query = """
        CREATE OR REPLACE TABLE `{table_id}`
        PARTITION BY DATE_TRUNC(approval_date, MONTH)
        CLUSTER BY borrower_state, loan_status, naics_code
        AS
        SELECT
            -- Temporal & Program
            AsOfDate AS as_of_date,
            Program AS program,

            -- Borrower Information
            BorrName AS borrower_name,
            BorrStreet AS borrower_street,
            BorrCity AS borrower_city,
            BorrState AS borrower_state,
            CAST(BorrZip AS STRING) AS borrower_zip,
            CAST(LocationID AS STRING) AS location_id,

            -- Lender/Bank Information
            BankName AS bank_name,
            CAST(BankFDICNumber AS STRING) AS bank_fdic_number,
            CAST(BankNCUANumber AS STRING) AS bank_ncua_number,
            BankStreet AS bank_street,
            BankCity AS bank_city,
            BankState AS bank_state,
            CAST(BankZip AS STRING) AS bank_zip,

            -- Loan Amounts
            CAST(GrossApproval AS NUMERIC) AS gross_approval,
            CAST(SBAGuaranteedApproval AS NUMERIC) AS sba_guaranteed_approval,
            CAST(SAFE_DIVIDE(SBAGuaranteedApproval, NULLIF(GrossApproval, 0)) AS NUMERIC) AS sba_guaranteed_pct,

            -- Loan Dates
            ApprovalDate AS approval_date,
            ApprovalFY AS approval_fy,
            FirstDisbursementDate AS first_disbursement_date,

            -- Loan Terms
            ProcessingMethod AS processing_method,
            Subprogram AS subprogram,
            CAST(InitialInterestRate AS NUMERIC) AS initial_interest_rate,
            FixedorVariableInterestRate AS fixed_or_variable_interest_rate,
            TerminMonths AS term_in_months,
            CASE
                WHEN RevolverStatus = 1 THEN 'Y'
                WHEN RevolverStatus = 0 THEN 'N'
                ELSE NULL
            END AS revolver_status,

            -- Business Classification
            CAST(NAICSCode AS STRING) AS naics_code,
            NAICSDescription AS naics_description,
            FranchiseCode AS franchise_code,
            FranchiseName AS franchise_name,

            -- Project Location
            ProjectCounty AS project_county,
            ProjectState AS project_state,
            SBADistrictOffice AS sba_district_office,
            CAST(CongressionalDistrict AS STRING) AS congressional_district,

            -- Business Details
            BusinessType AS business_type,
            BusinessAge AS business_age,

            -- Loan Status
            LoanStatus AS loan_status,
            PaidinFullDate AS paid_in_full_date,
            ChargeoffDate AS charge_off_date,
            CAST(GrossChargeoffAmount AS NUMERIC) AS gross_charge_off_amount,

            -- Impact
            CAST(JobsSupported AS INT64) AS jobs_supported,
            CollateralInd AS collateral_ind,

            -- Derived Fields
            CASE WHEN LoanStatus = 'CHGOFF' THEN 1 ELSE 0 END AS is_default,
            CASE WHEN LoanStatus IN ('PIF', 'CHGOFF', 'EXEMPT') THEN 1 ELSE 0 END AS is_funded,
            CASE WHEN LoanStatus IN ('COMMIT') THEN 1 ELSE 0 END AS is_funding_committed,
            CASE WHEN LoanStatus IN ('CANCLD') THEN 1 ELSE 0 END AS is_cancelled,

            -- Time-based metrics
            CASE
                WHEN ChargeoffDate IS NOT NULL AND FirstDisbursementDate IS NOT NULL
                THEN DATE_DIFF(ChargeoffDate, FirstDisbursementDate, MONTH)
                ELSE NULL
            END AS months_to_default,

            CASE
                WHEN FirstDisbursementDate IS NOT NULL
                THEN DATE_DIFF(AsOfDate, FirstDisbursementDate, MONTH)
                ELSE NULL
            END AS months_since_disbursement

        FROM `{raw_table_id}`
        WHERE AsOfDate IS NOT NULL  -- Exclude any rows with null as_of_date from raw
        """.format(
            table_id=table_id,
            raw_table_id=f"{bq_client.project}.{dataset_id}.raw_sba_foia_7a_loans"
        )

        # Check for null as_of_date in raw table before transformation
        raw_table_id_full = f"{bq_client.project}.{dataset_id}.raw_sba_foia_7a_loans"
        null_check_query = f"""
        SELECT
            COUNT(*) as total_raw_rows,
            COUNTIF(AsOfDate IS NULL) as null_as_of_date_count
        FROM `{raw_table_id_full}`
        """
        null_check_result = bq_client.query(null_check_query).to_dataframe().iloc[0]
        total_raw = int(null_check_result['total_raw_rows'])
        null_count = int(null_check_result['null_as_of_date_count'])

        if null_count > 0:
            context.log.warning(
                f"Found {null_count:,} rows ({null_count/total_raw*100:.2f}%) "
                f"with null AsOfDate in raw table - these will be excluded from staging"
            )

        context.log.info(f"Creating staging table: {table_id}")
        query_job = bq_client.query(query)
        query_job.result()  # Wait for completion

        context.log.info(f"Successfully created staging table: {table_id}")

        # Define column descriptions
        column_descriptions = {
            "as_of_date": "Date of data snapshot",
            "program": "SBA loan program type",
            "borrower_name": "Borrower business name",
            "borrower_street": "Borrower street address",
            "borrower_city": "Borrower city",
            "borrower_state": "Borrower state",
            "borrower_zip": "Borrower ZIP code",
            "location_id": "Borrower location identifier",
            "bank_name": "Lender/bank name",
            "bank_fdic_number": "Bank FDIC/RDI number",
            "bank_ncua_number": "Bank NCUA number for credit unions",
            "bank_street": "Bank street address",
            "bank_city": "Bank city",
            "bank_state": "Bank state",
            "bank_zip": "Bank ZIP code",
            "gross_approval": "Total approved loan amount",
            "sba_guaranteed_approval": "SBA guaranteed portion",
            "sba_guaranteed_pct": "Percentage of loan guaranteed by SBA",
            "approval_date": "Date loan was approved",
            "approval_fy": "Fiscal year of approval",
            "first_disbursement_date": "Date of first disbursement",
            "processing_method": "Loan processing method",
            "subprogram": "SBA subprogram type",
            "initial_interest_rate": "Initial interest rate as decimal (e.g., 0.0575 for 5.75%)",
            "fixed_or_variable_interest_rate": "Fixed or Variable indicator",
            "term_in_months": "Loan term in months",
            "revolver_status": "Whether loan is a revolver",
            "naics_code": "NAICS industry code",
            "naics_description": "NAICS industry description",
            "franchise_code": "Franchise code if applicable",
            "franchise_name": "Franchise name if applicable",
            "project_county": "County where project/business is located",
            "project_state": "State where project/business is located",
            "sba_district_office": "SBA district office",
            "congressional_district": "Congressional district",
            "business_type": "Type of business entity",
            "business_age": "Age category of business at time of loan",
            "loan_status": "Current loan status: PIF, CHGOFF, ACTIVE, etc.",
            "paid_in_full_date": "Date loan was paid in full",
            "charge_off_date": "Date loan was charged off (defaulted)",
            "gross_charge_off_amount": "Amount charged off",
            "jobs_supported": "Number of jobs created/retained",
            "collateral_ind": "Whether loan is collateralized",
            "is_default": "1 if loan defaulted (CHGOFF), 0 otherwise",
            "is_funded": "1 if loan was funded (PIF, CHGOFF, EXEMPT), 0 otherwise",
            "is_funding_committed": "1 if funding committed but not disbursed, 0 otherwise",
            "is_cancelled": "1 if loan was cancelled, 0 otherwise",
            "months_to_default": "Months from first disbursement to charge-off",
            "months_since_disbursement": "Months from first disbursement to as_of_date",
        }

        # Update table schema with descriptions
        table = bq_client.get_table(table_id)
        new_schema = []
        for field in table.schema:
            new_schema.append(
                bq.SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode=field.mode,
                    description=column_descriptions.get(field.name, ""),
                )
            )
        table.schema = new_schema
        bq_client.update_table(table, ["schema"])
        context.log.info(f"Updated column descriptions for {table_id}")

        # Get latest as_of_date first
        latest_date_query = f"""
        SELECT MAX(as_of_date) as latest_as_of_date
        FROM `{table_id}`
        """
        latest_date_df = bq_client.query(latest_date_query).to_dataframe()
        latest_as_of_date = latest_date_df.iloc[0]['latest_as_of_date']

        # Safety check: if MAX returns NULL, table is empty or all dates are null
        if pd.isna(latest_as_of_date):
            context.log.error("No valid as_of_date found in staging table - cannot generate statistics")
            raise ValueError("Staging table has no valid as_of_date values")

        # Get table statistics for profiling (only latest partition)
        stats_query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT borrower_state) as distinct_states,
            COUNT(DISTINCT naics_code) as distinct_naics_codes,
            MIN(approval_date) as min_approval_date,
            MAX(approval_date) as max_approval_date,
            MIN(as_of_date) as as_of_date,
            SUM(gross_approval) as total_loan_amount,
            AVG(gross_approval) as avg_loan_amount,
            MIN(gross_approval) as min_loan_amount,
            MAX(gross_approval) as max_loan_amount,
            SUM(jobs_supported) as total_jobs,
            AVG(jobs_supported) as avg_jobs_per_loan,
            SUM(is_default) as total_defaults,
            SUM(is_funded) as total_funded,
            SUM(is_cancelled) as total_cancelled,
            AVG(CASE WHEN is_default = 1 THEN months_to_default END) as avg_months_to_default,
            AVG(sba_guaranteed_pct) as avg_guarantee_pct
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date
        """

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter("latest_as_of_date", "DATE", latest_as_of_date)
            ]
        )

        stats_df = bq_client.query(stats_query, job_config=job_config).to_dataframe()
        stats = stats_df.iloc[0].to_dict()

        # Get status distribution (only latest partition)
        status_query = f"""
        SELECT
            loan_status,
            COUNT(*) as count
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date
        GROUP BY loan_status
        ORDER BY count DESC
        """
        status_df = bq_client.query(status_query, job_config=job_config).to_dataframe()
        status_distribution = {str(row['loan_status']): int(row['count']) for _, row in status_df.iterrows()}

        # Get top states (only latest partition)
        state_query = f"""
        SELECT
            borrower_state,
            COUNT(*) as count,
            SUM(gross_approval) as total_amount
        FROM `{table_id}`
        WHERE as_of_date = @latest_as_of_date
          AND borrower_state IS NOT NULL
        GROUP BY borrower_state
        ORDER BY count DESC
        LIMIT 10
        """
        state_df = bq_client.query(state_query, job_config=job_config).to_dataframe()

        # Convert all stats to Python native types
        total_rows = int(stats['total_rows'])
        total_defaults = int(stats['total_defaults'])
        total_funded = int(stats['total_funded'])
        total_cancelled = int(stats['total_cancelled'])
        default_rate = float(total_defaults / total_rows * 100) if total_rows > 0 else 0.0

        context.log.info(f"Processed {total_rows:,} loans")
        context.log.info(f"Default rate: {default_rate:.2f}%")
        context.log.info(f"Total loan amount: ${float(stats['total_loan_amount']):,.2f}")

        # Add comprehensive metadata
        context.add_output_metadata(
            {
                # Volume
                "total_rows": dg.MetadataValue.int(total_rows),
                "distinct_states": dg.MetadataValue.int(int(stats['distinct_states'])),
                "distinct_naics_codes": dg.MetadataValue.int(int(stats['distinct_naics_codes'])),

                # Temporal
                "as_of_date": dg.MetadataValue.text(str(stats['as_of_date'])),
                "approval_date_range": dg.MetadataValue.text(
                    f"{stats['min_approval_date']} to {stats['max_approval_date']}"
                ),

                # Loan amounts
                "total_loan_amount": dg.MetadataValue.float(float(stats['total_loan_amount'])),
                "avg_loan_amount": dg.MetadataValue.float(float(stats['avg_loan_amount'])),
                "min_loan_amount": dg.MetadataValue.float(float(stats['min_loan_amount'])),
                "max_loan_amount": dg.MetadataValue.float(float(stats['max_loan_amount'])),

                # Loan status metrics
                "total_defaults": dg.MetadataValue.int(total_defaults),
                "total_funded": dg.MetadataValue.int(total_funded),
                "total_cancelled": dg.MetadataValue.int(total_cancelled),
                "default_rate_pct": dg.MetadataValue.float(float(default_rate)),

                # Average metrics
                "avg_guarantee_pct": dg.MetadataValue.float(
                    float(stats['avg_guarantee_pct']) if stats['avg_guarantee_pct'] is not None else 0.0
                ),
                "avg_months_to_default": dg.MetadataValue.float(
                    float(stats['avg_months_to_default']) if stats['avg_months_to_default'] is not None else 0.0
                ),

                # Jobs impact
                "total_jobs_supported": dg.MetadataValue.float(
                    float(stats['total_jobs']) if stats['total_jobs'] is not None else 0.0
                ),
                "avg_jobs_per_loan": dg.MetadataValue.float(
                    float(stats['avg_jobs_per_loan']) if stats['avg_jobs_per_loan'] is not None else 0.0
                ),

                # Distributions
                "loan_status_distribution": dg.MetadataValue.md(
                    "\n".join([f"- **{status}**: {count:,} loans ({count/total_rows*100:.1f}%)"
                              for status, count in status_distribution.items()])
                ),
                "top_10_states": dg.MetadataValue.md(
                    "\n".join([
                        f"{i+1}. **{row['borrower_state']}**: {int(row['count']):,} loans (${float(row['total_amount']):,.0f})"
                        for i, (_, row) in enumerate(state_df.iterrows())
                    ])
                ),

                # Destination
                "bigquery_table": dg.MetadataValue.text(table_id),
            }
        )
