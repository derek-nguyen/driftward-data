import dagster as dg
from dagster_gcp import BigQueryResource, GCSResource
import pandas as pd
import requests
import re
from datetime import datetime, timezone


SBA_FOIA_DATASET_URL = "https://data.sba.gov/dataset/7-a-504-foia"
GCS_BUCKET = "sba_loans"
GCS_PREFIX = "sba_foia_7a_504"


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