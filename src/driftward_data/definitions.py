# from dagster import Definitions, load_assets_from_modules

# from driftward_data.defs import assets
# from driftward_data.defs.resources import bigquery


# defs = Definitions(
#     assets=load_assets_from_modules([assets]),
#     resources={"bigquery": bigquery},
# )

from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)