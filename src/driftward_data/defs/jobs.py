import dagster as dg
from .assets.sba_foia_raw import sba_foia_raw


# Quarterly schedule: 1st of Feb, May, Aug, Nov (1 month after quarter end)
# SBA updates data approximately 1 month after each quarter ends
sba_foia_schedule = dg.ScheduleDefinition(
    name="sba_foia_quarterly",
    cron_schedule="0 0 1 2,5,8,11 *",  # Midnight on 1st of Feb, May, Aug, Nov
    target=dg.AssetSelection.assets(sba_foia_raw),
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(
        schedules=[sba_foia_schedule],
    )
