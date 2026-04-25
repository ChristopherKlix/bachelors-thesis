import pandas as pd
import datetime as dt
from omr_gridvis import GridVisRESTAPI, PROJECTS, VALUES, TYPES, TIMEBASES


def main():
    gridvis = GridVisRESTAPI()
    gridvis.set_project(PROJECTS.EP_OM_GridVis)
    gridvis.allow_self_signed_certificate(True)

    start_ts: dt.datetime = pd.Timestamp('2025-01-01 00:00')
    start_ts = start_ts.tz_localize('Europe/Berlin')
    end_ts: dt.datetime = pd.Timestamp('2026-01-01 00:00')
    end_ts = end_ts.tz_localize('Europe/Berlin')

    df: pd.DataFrame = gridvis.hist_values(
        p_id=85,
        p_value=VALUES.PowerActive,
        p_type=TYPES.SUM13,
        p_timebase=TIMEBASES.QUARTER_HOUR,
        start=start_ts,
        end=end_ts
    )

    print(df)

    df = gridvis.filter_full_intervals(df, dt.timedelta(minutes=60))

    print(df)


if __name__ == '__main__':
    main()
