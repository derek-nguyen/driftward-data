import dagster as dg
from dagster_gcp import BigQueryResource, GCSResource
import pandas as pd
import requests
import re
from datetime import datetime, timezone


SBA_FOIA_DATASET_URL = "https://data.sba.gov/dataset/7-a-504-foia"
GCS_BUCKET = "sba_loans"
GCS_PREFIX = "sba_foia_7a_504"

# BigQuery schema for SBA 7(a) loans
# Using NULLABLE for all fields to handle data quality issues
SBA_7A_SCHEMA = [
    # Temporal & Program
    {"name": "AsOfDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "Program", "type": "STRING", "mode": "NULLABLE"},
    # Borrower
    {"name": "BorrName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrStreet", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrCity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrZip", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "LocationID", "type": "INTEGER", "mode": "NULLABLE"},
    # Lender
    {"name": "BankName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankFDICNumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "BankNCUANumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "BankStreet", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankCity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankZip", "type": "INTEGER", "mode": "NULLABLE"},
    # Loan Terms
    {"name": "GrossApproval", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "SBAGuaranteedApproval", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "ApprovalDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ApprovalFY", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "FirstDisbursementDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ProcessingMethod", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Subprogram", "type": "STRING", "mode": "NULLABLE"},
    {"name": "InitialInterestRate", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "FixedorVariableInterestRate", "type": "STRING", "mode": "NULLABLE"},
    {"name": "TerminMonths", "type": "INTEGER", "mode": "NULLABLE"},
    # Business Classification
    {"name": "NAICSCode", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "NAICSDescription", "type": "STRING", "mode": "NULLABLE"},
    {"name": "FranchiseCode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "FranchiseName", "type": "STRING", "mode": "NULLABLE"},
    # Project Location
    {"name": "ProjectCounty", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ProjectState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "SBADistrictOffice", "type": "STRING", "mode": "NULLABLE"},
    {"name": "CongressionalDistrict", "type": "INTEGER", "mode": "NULLABLE"},
    # Business Details
    {"name": "BusinessType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BusinessAge", "type": "STRING", "mode": "NULLABLE"},
    # Loan Status
    {"name": "LoanStatus", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PaidinFullDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ChargeoffDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "GrossChargeoffAmount", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "RevolverStatus", "type": "INTEGER", "mode": "NULLABLE"},
    # Impact & Features
    {"name": "JobsSupported", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "CollateralInd", "type": "STRING", "mode": "NULLABLE"},
    {"name": "SoldSecondMarketInd", "type": "STRING", "mode": "NULLABLE"},
]


def fetch_sba_foia_urls() -> dict[str, str]:
    """Dynamically fetch current SBA FOIA file URLs from the dataset page."""
    response = requests.get(SBA_FOIA_DATASET_URL, timeout=60)
    response.raise_for_status()
    html_content = response.text

    # Reconstruct full URLs
    full_urls = re.findall(
        r'(https://data\.sba\.gov/dataset/[^"]+/download/[^"]+\.(?:csv|xlsx))',
        html_content,
    )

    # Extract as-of date from CSV filenames to apply to data dictionary
    as_of_date = None
    for url in full_urls:
        match = re.search(r"asof-(\d{6})", url)
        if match:
            as_of_date = match.group(1)
            break

    files = {}
    for url in full_urls:
        # Extract original filename from URL (preserves as-of date)
        original_filename = url.split("/")[-1]

        # Only include FOIA files and data dictionary
        if "foia-7a-" in original_filename or "foia-504-" in original_filename:
            # Use original filename without extension for storage
            # e.g., foia-504-fy1991-fy2009-asof-250930.csv -> foia-504-fy1991-fy2009-asof-250930
            filename_without_ext = original_filename.rsplit(".", 1)[0]
            files[filename_without_ext] = url
        elif "data_dictionary" in original_filename.lower() or (
            original_filename.endswith(".xlsx") and "foia" in original_filename.lower()
        ):
            # Append as-of date to data dictionary filename for consistency
            filename_without_ext = original_filename.rsplit(".", 1)[0]
            if as_of_date and "asof" not in filename_without_ext:
                filename_without_ext = f"{filename_without_ext}_asof_{as_of_date}"
            files[filename_without_ext] = url

    return files


@dg.asset
def sba_foia_raw(
    context: dg.AssetExecutionContext, gcs: GCSResource
) -> dict:
    """Downloads SBA 7(a) and 504 FOIA loan data and uploads to GCS.

    Source: https://data.sba.gov/dataset/7-a-504-foia
    Files include quarterly loan data and data dictionary.
    URLs are fetched dynamically to capture the latest quarterly release.
    """
    # Dynamically fetch current file URLs
    context.log.info(f"Fetching file URLs from {SBA_FOIA_DATASET_URL}")
    file_urls = fetch_sba_foia_urls()
    context.log.info(f"Found {len(file_urls)} files to download")

    uploaded_files = []

    client = gcs.get_client()
    bucket = client.bucket(GCS_BUCKET)

    for filename_base, url in file_urls.items():
        # Extract extension from URL
        original_filename = url.split("/")[-1]
        extension = original_filename.rsplit(".", 1)[-1]
        full_filename = f"{filename_base}.{extension}"

        context.log.info(f"Downloading {full_filename} from {url}")

        response = requests.get(url, timeout=300)
        response.raise_for_status()

        gcs_path = f"{GCS_PREFIX}/{full_filename}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(response.content)

        file_info = {
            "filename": full_filename,
            "gcs_path": f"gs://{GCS_BUCKET}/{gcs_path}",
            "size_bytes": len(response.content),
            "source_url": url,
        }
        uploaded_files.append(file_info)

        context.log.info(
            f"Uploaded {full_filename} to {file_info['gcs_path']} "
            f"({file_info['size_bytes']:,} bytes)"
        )

    metadata = {
        "files": uploaded_files,
        "total_files": len(uploaded_files),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "source_url": SBA_FOIA_DATASET_URL,
    }

    context.add_output_metadata(
        {
            "total_files": len(uploaded_files),
            "total_size_bytes": sum(f["size_bytes"] for f in uploaded_files),
            "gcs_bucket": GCS_BUCKET,
            "gcs_prefix": GCS_PREFIX,
            "source_url": SBA_FOIA_DATASET_URL,
        }
    )

    return metadata


@dg.asset(deps=[sba_foia_raw])
def raw_sba_foia_7a_loans(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
) -> None:
    """Loads SBA 7(a) FOIA loan data from GCS to BigQuery.

    Reads all 7(a) CSV files from GCS bucket, combines them into a single DataFrame,
    and loads into BigQuery table with partitioning and clustering for efficient queries.

    BigQuery table: sba_loans.raw_sba_foia_7a_loans
    Partitioning: MONTH on ApprovalDate
    Clustering: BorrState, LoanStatus, NAICSCode
    """
    from google.cloud import bigquery as bq

    # Initialize GCS client
    gcs_client = gcs.get_client()

    # List all 7(a) CSV files in GCS
    bucket = gcs_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=GCS_PREFIX)

    csv_files = [
        blob for blob in blobs
        if blob.name.endswith('.csv') and 'foia-7a-' in blob.name
    ]

    context.log.info(f"Found {len(csv_files)} 7(a) CSV files in gs://{GCS_BUCKET}/{GCS_PREFIX}")

    # Read and combine all CSV files
    dataframes = []
    source_files = []
    file_profiles = []

    for blob in csv_files:
        context.log.info(f"Reading {blob.name}")

        # Extract as-of date from filename (e.g., foia-7a-fy2010-fy2024-asof-250930.csv)
        filename = blob.name.split('/')[-1]  # Get just the filename
        as_of_from_filename = None
        match = re.search(r'asof[_-](\d{6})', filename)
        if match:
            # Convert YYMMDD to datetime (e.g., 250930 -> 2025-09-30)
            date_str = match.group(1)
            year = int('20' + date_str[:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            as_of_from_filename = pd.Timestamp(year=year, month=month, day=day)
            context.log.info(f"  Extracted as-of date from filename: {as_of_from_filename.date()}")

        # Download CSV content
        csv_content = blob.download_as_text()

        # Parse CSV with pandas
        df = pd.read_csv(
            pd.io.common.StringIO(csv_content),
            dtype={
                'BorrZip': str,  # Read as string first to handle leading zeros
                'BankZip': str,
                'LocationID': str,
                'BankFDICNumber': str,
                'BankNCUANumber': str,
                'NAICSCode': str,
                'CongressionalDistrict': str,
                'RevolverStatus': str,
            }
        )

        # Parse AsOfDate first if it exists (before filling)
        if 'AsOfDate' in df.columns:
            df['AsOfDate'] = pd.to_datetime(df['AsOfDate'], format='%m/%d/%Y', errors='coerce')

        # If AsOfDate column doesn't exist or has nulls, use filename date
        if as_of_from_filename is not None:
            if 'AsOfDate' not in df.columns:
                df['AsOfDate'] = as_of_from_filename
                context.log.info(f"  Added AsOfDate column from filename")
            else:
                # Fill nulls with filename date (as Timestamp, not string)
                null_count_before = df['AsOfDate'].isnull().sum()
                df['AsOfDate'] = df['AsOfDate'].fillna(as_of_from_filename)
                if null_count_before > 0:
                    context.log.info(f"  Filled {null_count_before:,} null AsOfDate values from filename")

        # Profile this file (convert numpy types to Python native types)
        file_profile = {
            "filename": blob.name,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "null_count": int(df.isnull().sum().sum()),
            "memory_mb": float(round(df.memory_usage(deep=True).sum() / 1024**2, 2)),
        }
        file_profiles.append(file_profile)

        dataframes.append(df)
        source_files.append(blob.name)
        context.log.info(f"  Loaded {len(df):,} rows, {file_profile['column_count']} columns, {file_profile['memory_mb']} MB from {blob.name}")

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    context.log.info(f"Combined total: {len(combined_df):,} rows")

    # Parse remaining date columns (MM/DD/YYYY format)
    # AsOfDate already parsed per-file before concatenation
    date_columns = ['ApprovalDate', 'FirstDisbursementDate', 'PaidinFullDate', 'ChargeoffDate']
    for col in date_columns:
        combined_df[col] = pd.to_datetime(combined_df[col], format='%m/%d/%Y', errors='coerce')

    # Convert numeric columns with proper type handling
    # ZIP codes - convert to integer, handling NaN
    combined_df['BorrZip'] = pd.to_numeric(combined_df['BorrZip'], errors='coerce').astype('Int64')
    combined_df['BankZip'] = pd.to_numeric(combined_df['BankZip'], errors='coerce').astype('Int64')

    # Other integer columns
    combined_df['LocationID'] = pd.to_numeric(combined_df['LocationID'], errors='coerce').astype('Int64')
    combined_df['BankFDICNumber'] = pd.to_numeric(combined_df['BankFDICNumber'], errors='coerce').astype('Int64')
    combined_df['BankNCUANumber'] = pd.to_numeric(combined_df['BankNCUANumber'], errors='coerce').astype('Int64')
    combined_df['NAICSCode'] = pd.to_numeric(combined_df['NAICSCode'], errors='coerce').astype('Int64')
    combined_df['CongressionalDistrict'] = pd.to_numeric(combined_df['CongressionalDistrict'], errors='coerce').astype('Int64')
    combined_df['ApprovalFY'] = pd.to_numeric(combined_df['ApprovalFY'], errors='coerce').astype('Int64')
    combined_df['TerminMonths'] = pd.to_numeric(combined_df['TerminMonths'], errors='coerce').astype('Int64')
    combined_df['RevolverStatus'] = pd.to_numeric(combined_df['RevolverStatus'], errors='coerce').astype('Int64')

    # Float columns
    combined_df['GrossApproval'] = pd.to_numeric(combined_df['GrossApproval'], errors='coerce')
    combined_df['SBAGuaranteedApproval'] = pd.to_numeric(combined_df['SBAGuaranteedApproval'], errors='coerce')
    combined_df['InitialInterestRate'] = pd.to_numeric(combined_df['InitialInterestRate'], errors='coerce')
    combined_df['GrossChargeoffAmount'] = pd.to_numeric(combined_df['GrossChargeoffAmount'], errors='coerce')
    combined_df['JobsSupported'] = pd.to_numeric(combined_df['JobsSupported'], errors='coerce')

    # Convert string columns to handle NaN properly (convert NaN to None for BigQuery)
    string_columns = [
        'Program', 'BorrName', 'BorrStreet', 'BorrCity', 'BorrState',
        'BankName', 'BankStreet', 'BankCity', 'BankState',
        'ProcessingMethod', 'Subprogram', 'FixedorVariableInterestRate',
        'NAICSDescription', 'FranchiseCode', 'FranchiseName',
        'ProjectCounty', 'ProjectState', 'SBADistrictOffice',
        'BusinessType', 'BusinessAge', 'LoanStatus',
        'CollateralInd', 'SoldSecondMarketInd'
    ]
    for col in string_columns:
        combined_df[col] = combined_df[col].astype(str).replace('nan', None)

    # Extract metadata and comprehensive data profiling (convert numpy/pandas types to Python types)
    as_of_date = combined_df['AsOfDate'].iloc[0] if len(combined_df) > 0 else None
    min_approval_date = combined_df['ApprovalDate'].min()
    max_approval_date = combined_df['ApprovalDate'].max()
    total_loan_amount = float(combined_df['GrossApproval'].sum())

    # Data quality metrics (convert numpy types to Python native types)
    total_rows = int(len(combined_df))
    total_nulls = int(combined_df.isnull().sum().sum())
    null_percentage = float(round((total_nulls / (total_rows * len(combined_df.columns))) * 100, 2))

    # Null counts for key columns
    key_null_counts = {
        'AsOfDate': int(combined_df['AsOfDate'].isnull().sum()),
        'ApprovalDate': int(combined_df['ApprovalDate'].isnull().sum()),
        'GrossApproval': int(combined_df['GrossApproval'].isnull().sum()),
        'BorrState': int(combined_df['BorrState'].isnull().sum()),
        'NAICSCode': int(combined_df['NAICSCode'].isnull().sum()),
    }

    # Loan amount statistics (convert numpy types to Python native types)
    loan_amount_stats = {
        'min': float(combined_df['GrossApproval'].min()),
        'max': float(combined_df['GrossApproval'].max()),
        'mean': float(combined_df['GrossApproval'].mean()),
        'median': float(combined_df['GrossApproval'].median()),
        'total': float(total_loan_amount),
    }

    # State distribution (top 10) - convert to Python int
    state_distribution = {str(k): int(v) for k, v in combined_df['BorrState'].value_counts().head(10).items()}

    # Loan status distribution - convert to Python int
    status_distribution = {str(k): int(v) for k, v in combined_df['LoanStatus'].value_counts().items()}

    # Jobs statistics - convert to Python float
    total_jobs_supported = float(combined_df['JobsSupported'].sum())
    avg_jobs_per_loan = float(combined_df['JobsSupported'].mean())

    # Fiscal year distribution - convert to Python int
    fy_distribution = {int(k): int(v) for k, v in combined_df['ApprovalFY'].value_counts().sort_index().tail(10).items()}

    context.log.info(f"Data as-of date: {as_of_date}")
    context.log.info(f"Total rows: {total_rows:,}")
    context.log.info(f"Data quality: {null_percentage}% null values")
    context.log.info(f"Approval date range: {min_approval_date} to {max_approval_date}")
    context.log.info(f"Total loan amount: ${total_loan_amount:,.2f}")
    context.log.info(f"Loan amount range: ${loan_amount_stats['min']:,.2f} - ${loan_amount_stats['max']:,.2f}")
    context.log.info(f"Total jobs supported: {total_jobs_supported:,.0f}")
    context.log.info(f"Top 5 states: {dict(list(state_distribution.items())[:5])}")

    # Use BigQuery client within context manager
    with bigquery.get_client() as bq_client:
        # Create BigQuery dataset if it doesn't exist
        dataset_id = "sba_loans"
        dataset_ref = bq.DatasetReference(bq_client.project, dataset_id)

        try:
            bq_client.get_dataset(dataset_ref)
            context.log.info(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bq.Dataset(dataset_ref)
            dataset.location = "US"
            bq_client.create_dataset(dataset)
            context.log.info(f"Created dataset {dataset_id}")

        # Configure BigQuery table with partitioning and clustering
        table_id = f"{bq_client.project}.{dataset_id}.raw_sba_foia_7a_loans"

        job_config = bq.LoadJobConfig(
            schema=[
                bq.SchemaField(field["name"], field["type"], mode=field["mode"])
                for field in SBA_7A_SCHEMA
            ],
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bq.TimePartitioning(
                type_=bq.TimePartitioningType.MONTH,
                field="ApprovalDate",
            ),
            clustering_fields=["BorrState", "LoanStatus", "NAICSCode"],
        )

        # Load data to BigQuery
        context.log.info(f"Loading {len(combined_df):,} rows to {table_id}")
        load_job = bq_client.load_table_from_dataframe(
            combined_df,
            table_id,
            job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()
        context.log.info(f"Successfully loaded data to {table_id}")

    # Add comprehensive output metadata using Dagster metadata types
    context.add_output_metadata(
        {
            # Source info
            "source_files_count": dg.MetadataValue.int(len(source_files)),
            "file_profiles": dg.MetadataValue.md(
                "\n".join([
                    f"- **{fp['filename']}**: {fp['row_count']:,} rows, {fp['column_count']} cols, {fp['memory_mb']} MB, {fp['null_count']:,} nulls"
                    for fp in file_profiles
                ])
            ),
            # Data volume
            "total_rows": dg.MetadataValue.int(total_rows),
            "total_columns": dg.MetadataValue.int(len(combined_df.columns)),
            # Data quality
            "data_quality_null_percentage": dg.MetadataValue.float(null_percentage),
            "key_column_null_counts": dg.MetadataValue.md(
                "\n".join([f"- **{col}**: {count:,} nulls" for col, count in key_null_counts.items()])
            ),
            # Temporal
            "as_of_date": dg.MetadataValue.text(str(as_of_date)),
            "approval_date_range": dg.MetadataValue.text(f"{min_approval_date} to {max_approval_date}"),
            "fiscal_year_distribution": dg.MetadataValue.md(
                "\n".join([f"- **FY {fy}**: {count:,} loans" for fy, count in sorted(fy_distribution.items())])
            ),
            # Loan statistics
            "loan_amount_min": dg.MetadataValue.float(loan_amount_stats['min']),
            "loan_amount_max": dg.MetadataValue.float(loan_amount_stats['max']),
            "loan_amount_mean": dg.MetadataValue.float(loan_amount_stats['mean']),
            "loan_amount_median": dg.MetadataValue.float(loan_amount_stats['median']),
            "loan_amount_total": dg.MetadataValue.float(loan_amount_stats['total']),
            # Geographic
            "top_10_states_by_count": dg.MetadataValue.md(
                "\n".join([f"{i+1}. **{state}**: {count:,} loans" for i, (state, count) in enumerate(state_distribution.items())])
            ),
            # Status
            "loan_status_distribution": dg.MetadataValue.md(
                "\n".join([f"- **{status}**: {count:,} loans" for status, count in status_distribution.items()])
            ),
            # Impact
            "total_jobs_supported": dg.MetadataValue.float(total_jobs_supported),
            "avg_jobs_per_loan": dg.MetadataValue.float(avg_jobs_per_loan),
            # Destination
            "bigquery_table": dg.MetadataValue.text(table_id),
        }
    )


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


@dg.asset_check(asset=raw_sba_foia_7a_loans)
def check_raw_as_of_date_not_null(
    context: dg.AssetExecutionContext,
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


@dg.asset_check(asset=stg_sba_foia_7a_loans)
def check_as_of_date_not_null(
    context: dg.AssetExecutionContext,
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
    context: dg.AssetExecutionContext,
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
    context: dg.AssetExecutionContext,
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