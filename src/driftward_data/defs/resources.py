import dagster as dg
from dagster_gcp import BigQueryResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "bigquery": BigQueryResource(
                project="learned-mind-476504-i3",
            )
        }
    )
