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

    # Parse date columns (MM/DD/YYYY format)
    date_columns = ['AsOfDate', 'ApprovalDate', 'FirstDisbursementDate', 'PaidinFullDate', 'ChargeoffDate']
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