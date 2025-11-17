import dagster as dg
from dagster_gcp import BigQueryResource, GCSResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "bigquery": BigQueryResource(
                project="learned-mind-476504-i3",
            ),
            "gcs": GCSResource(
                project="learned-mind-476504-i3",
            ),
        }
    )
