from dagster import Definitions

from driftward_data.defs.assets import sba_loans_sample
from driftward_data.defs.resources import bigquery


defs = Definitions(
    assets=[sba_loans_sample],
    resources={"bigquery": bigquery},
)
