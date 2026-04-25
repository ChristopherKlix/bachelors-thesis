"""
janitza_energy_aggregations.py
============================

"""

# Standard library
import asyncio
import datetime as dt
from typing import Any

# Third-party
import pandas as pd
import psycopg2
from psycopg2 import sql

from settings import settings


class JanitzaEnergyAggregations:
    """
    Provides functionality to manage and maintain aggregation cache tables
    for the database. Handles creation, deletion, and incremental updates of
    materialized views (MVs) and views related to Janitza energy data.

    This class is designed to ensure the cache remains consistent and up-to-date
    with the latest raw data.

    **Public Methods:**
        - :meth:`rebuild`: Rebuilds all cache MVs and VIEWs from scratch.
        - :meth:`clear`: Removes all cache MVs and VIEWs.
        - :meth:`start_auto_update`: Executes a refresh strategy to update
          aggregations to the latest available data.

    :raises RuntimeError: If an operation fails due to database connectivity or
                          invalid cache state.

    **Example:**::
        >>> agg = JanitzaEnergyAggregations()
        >>> agg.start_auto_update()
    """

    def __init__(self):
        self._conn = self._get_db_connection()

    ################################################################
    # MATERIALIZED VIEWs
    ################################################################

    async def rebuild(self, confirm: bool=False) -> dict[str, Any]:
        """
        Completely rebuilds all aggregation cache tables and materialized views.

        This will drop and recreate all views and MVs to ensure full consistency.
        Use this when the underlying schema or raw data structure changes.

        :param confirm: Needs to be set to ``True`` for the method to run, defaults to False
        :type confirm: bool, optional
        :raises ValueError: If confirm was not set to ``True``
        """

        t: pd.Timestamp = pd.Timestamp.now()

        if not confirm:
            raise ValueError(
                f'Make sure to set confirm to True to run this method. '
                f'This will rebuild all of the aggregation cache table '
                f'and might take some time. '
                f'No data will be deleted.'
            )
        
        await self.clear(confirm=confirm)

        # Get the earliest record in ``janitza_energy``
        # to include all data in the aggregations

        sql_query = sql.SQL('''
            SELECT
                MIN(start_ts) AS min_start_ts
            FROM janitza_energy
        ''')

        with self._conn.cursor() as cur:
            cur.execute(sql_query)
            min_ts: dt.datetime = cur.fetchone()[0]

        min_ts = pd.Timestamp(min_ts).tz_convert('Europe/Berlin')
        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

        start_year: int = min_ts.year

        # Create the ``janitza_energy_day_%`` and ``janitza_energy_month_%``
        # for each year (and month)

        for year in range(start_year, now.year+1):
            for month in range(1, 12+1):
                await self.update_janitza_energy_hour(year, month)
                await self.update_janitza_energy_day(year, month)

        await self._create_or_replace_janitza_energy_hour_view()
        await self._create_or_replace_janitza_energy_day_view()

        for year in range(start_year, now.year+1):
            await self.update_janitza_energy_month(year)

        # Finally, create the VIEWs that combine all MVs
        # ``janitza_energy_year``: just in time aggregation of VIEW ``janitza_energy_month``
        # ``janitza_energy_month``: union of all MVs ``janitza_energy_month_%``
        # ``janitza_energy_day``: union of all MVs ``janitza_energy_day_%``

        await self._create_or_replace_janitza_energy_month_view()

        await self._create_or_replace_janitza_energy_year_view()

        stats: dict[str, dt.timedelta] = {
            'time': pd.Timestamp.now() - t
        }

        return stats

    async def clear(self, confirm: bool=False) -> None:
        """
        Deletes all materialized views and views related to the aggregation cache.

        This effectively resets the entire cache layer and should be followed by
        a call to :meth:`rebuild` to restore consistency.

        :param confirm: Needs to be set to True for the method to run, defaults to False
        :type confirm: bool, optional
        :raises ValueError: If confirm was not set to True
        """
        if not confirm:
            raise ValueError(
                f'Make sure to set confirm to True to run this method. '
                f'This will delete all of the aggregation cache tables. '
                f'No raw data will be deleted.'
            )
        
        ################################
        # Drop all VIEWs
        # 
        # janitza_energy_year
        # janitza_energy_month
        # janitza_energy_day
        # janitza_energy_hour
        ################################

        await self._drop_janitza_energy_year_view()

        await self._drop_janitza_energy_month_view()

        await self._drop_janitza_energy_day_view()

        await self._drop_janitza_energy_hour_view()

        ################################
        # Drop all MATERIALIZED VIEWs
        ################################

        sql_query: sql.Composable = sql.SQL('''
            SELECT matviewname
            FROM pg_matviews
            WHERE matviewname LIKE {mv_name}
        ''').format(
            mv_name=sql.Placeholder('mv_name')
        )

        def run_query(aggregation: str) -> list[str]:
            with self._conn.cursor() as cur:
                cur.execute(sql_query, {'mv_name': f'janitza_energy_{aggregation}_%'})
                mv_list: list[str] = [row[0] for row in cur.fetchall()]

                return mv_list
        
        ################################
        # Get a list of all MATERIALIZED VIEWs: janitza_energy_month_XXXX
        ################################
            
        mv_list: list[str] = await asyncio.to_thread(run_query, aggregation='month')

        for mv_name in mv_list:
            await self._drop_janitza_energy_mv(mv_name)
        
        ################################
        # Get a list of all MATERIALIZED VIEWs: janitza_energy_day_XXXX_XX
        ################################

        mv_list = await asyncio.to_thread(run_query, aggregation='day')

        for mv_name in mv_list:
            await self._drop_janitza_energy_mv(mv_name)
        
        ################################
        # Get a list of all MATERIALIZED VIEWs: janitza_energy_hour_XXXX_XX
        ################################

        mv_list = await asyncio.to_thread(run_query, aggregation='hour')

        for mv_name in mv_list:
            await self._drop_janitza_energy_mv(mv_name)

    async def start_auto_update(
            self,
            rule_hour_include_last_month: bool=False,
            rule_day_include_last_month: bool=False,
            rule_month_include_last_year: bool=False
        ) -> None:
        """
        Executes an incremental refresh strategy for all aggregations.

        Can be triggered at any time to update cache tables with the latest
        available data without performing a full rebuild.

        :param rule_hour_include_last_month: Rule to include the previous month
        in the refresh for the hour aggregation, defaults to False
        :type rule_hour_include_last_month: bool, optional
        :param rule_day_include_last_month: Rule to include the previous month
        in the refresh for the day aggregation, defaults to False
        :type rule_day_include_last_month: bool, optional
        :param rule_month_include_last_year: Rule to include the previous year
        in the refresh for the month aggregation, defaults to False
        :type rule_month_include_last_year: bool, optional
        """

        # Hourly Energy
        _t: pd.Timestamp = pd.Timestamp.now()
        await self._start_auto_update_janitza_energy_hour(
            include_last_month=rule_hour_include_last_month
        )
        print(f'Janitza Energy Aggregation Hour: {pd.Timestamp.now() - _t}')

        # Daily Energy
        _t = pd.Timestamp.now()
        await self._start_auto_update_janitza_energy_day(
            include_last_month=rule_day_include_last_month
        )
        print(f'Janitza Energy Aggregation Day: {pd.Timestamp.now() - _t}')

        # Monthly Energy
        _t = pd.Timestamp.now()
        await self._start_auto_update_janitza_energy_month(
            include_last_year=rule_month_include_last_year
        )
        print(f'Janitza Energy Aggregation Month: {pd.Timestamp.now() - _t}')

        # Yearly Energy
        _t = pd.Timestamp.now()
        await self._start_auto_update_janitza_energy_year()
        print(f'Janitza Energy Aggregation Year: {pd.Timestamp.now() - _t}')

    # AUTO UPDATE

    async def _start_auto_update_janitza_energy_hour(
            self,
            include_last_month: bool=False
        ) -> None:
        print(f'#' * 64)
        print(f'Auto Update - Hour')
        print(f'#' * 64)
        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

        matview_pattern: str = f'janitza_energy_hour_{now.year}_%'

        print(f'Do the MATERIALIZED VIEWs for the current year already exist?')
        print(f'#' * 64)

        print(f'Getting matviews...')
        matviews: list[str] = await self._get_matviews(matview_pattern)

        # Do the MATERIALIZED VIEWs for the current year already exist?

        if len(matviews) == 0:
            print(f'No matviews exist.')
            # CREATE MATERIALIZED VIEWs for the current year
            for i in range(1, 13):
                print(f'Creating matview for {now.year}-{i:02}.')
                await self.update_janitza_energy_hour(year=now.year, month=i)

            print(f'Creating view janitza_energy_hour...')
            # CREATE OR REPLACE VIEW ``janitza_energy_hour``
            await self._create_or_replace_janitza_energy_hour_view()
        elif len(matviews) != 12:
            raise RuntimeError(
                f'An error occured while checking database integrity. '
                f'12 MATERIALIZED VIEWs for janitza_energy_hour_{now.year} expected.'
                f'{len(matviews)} MATERIALIZED VIEWs for janitza_energy_hour_{now.year} exist.'
                f'Check the MATERIALIZED VIEWs for janitza_energy_hour_{now.year}.'
            )
        else:
            print(f'Matviews already exist.')
            print(f'Refreshing matview for {now.year}-{now.month:02}.')
            # REFRESH MATERIALIZED VIEW for the current month
            await self.update_janitza_energy_hour(year=now.year, month=now.month)

        if include_last_month:
            start_ts, _ = self._get_range_from_interval('last_month')
            last_month: int = pd.Timestamp(start_ts).tz_convert('Europe/Berlin')

            await self.update_janitza_energy_hour(
                year=last_month.year,
                month=last_month.month
            )

    async def _start_auto_update_janitza_energy_day(
            self,
            include_last_month: bool=False
        ) -> None:
        print(f'#' * 64)
        print(f'Auto Update - Day')
        print(f'#' * 64)
        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

        matview_pattern: str = f'janitza_energy_day_{now.year}_%'

        print(f'Do the MATERIALIZED VIEWs for the current year already exist?')
        print(f'#' * 64)

        print(f'Getting matviews...')
        matviews: list[str] = await self._get_matviews(matview_pattern)

        # Do the MATERIALIZED VIEWs for the current year already exist?

        if len(matviews) == 0:
            print(f'No matviews exist.')
            # CREATE MATERIALIZED VIEWs for the current year
            for i in range(1, 13):
                print(f'Creating matview for {now.year}-{i:02}.')
                await self.update_janitza_energy_day(year=now.year, month=i)

            print(f'Creating view janitza_energy_day...')
            # CREATE OR REPLACE VIEW ``janitza_energy_day``
            await self._create_or_replace_janitza_energy_day_view()
        elif len(matviews) != 12:
            raise RuntimeError(
                f'An error occured while checking database integrity. '
                f'12 MATERIALIZED VIEWs for janitza_energy_day_{now.year} expected.'
                f'{len(matviews)} MATERIALIZED VIEWs for janitza_energy_day_{now.year} exist.'
                f'Check the MATERIALIZED VIEWs for janitza_energy_day_{now.year}.'
            )
        else:
            print(f'Matviews already exist.')
            print(f'Refreshing matview for {now.year}-{now.month:02}.')
            # REFRESH MATERIALIZED VIEW for the current month
            await self.update_janitza_energy_day(year=now.year, month=now.month)

        if include_last_month:
            start_ts, _ = self._get_range_from_interval('last_month')
            last_month: int = pd.Timestamp(start_ts).tz_convert('Europe/Berlin')

            await self.update_janitza_energy_day(
                year=last_month.year,
                month=last_month.month
            )

    async def _start_auto_update_janitza_energy_month(
            self,
            include_last_year: bool=False
        ) -> None:
        print(f'#' * 64)
        print(f'Auto Update - Month')
        print(f'#' * 64)
        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

        matview_pattern: str = f'janitza_energy_month_{now.year}'

        print(f'Does the MATERIALIZED VIEW for the current year already exist?')
        print(f'#' * 64)

        print(f'Getting matviews...')
        matviews: list[str] = await self._get_matviews(matview_pattern)

        if len(matviews) == 0:
            print(f'No matviews exist.')

        # Does the MATERIALIZED VIEW for the current year already exist?


        # CREATE OR REFRESH MATERIALIZED VIEW for the current year
        print(f'Anyway...')
        print(f'Creating or refreshing matview for {now.year}.')
        await self.update_janitza_energy_month(year=now.year)

        if len(matviews) == 0:
            print(f'Since the matview didn\'t exist we need to create or replace the view janitza_energy_month')
            # CREATE OR REPLACE VIEW ``janitza_energy_month``
            await self._create_or_replace_janitza_energy_month_view()
        
        if include_last_year:
            start_ts, _ = self._get_range_from_interval('last_year')
            last_year: int = pd.Timestamp(start_ts).tz_convert('Europe/Berlin')

            await self.update_janitza_energy_month(year=last_year.year)

    async def _start_auto_update_janitza_energy_year(self):
        print(f'#' * 64)
        print(f'Auto Update - Year')
        print(f'#' * 64)
        print(f'Create or replace view for janitza_energy_year.')
        await self._create_or_replace_janitza_energy_year_view()

    # CREATE or REFRESH

    async def update_janitza_energy_hour(self, year: int, month: int) -> None:
        AGGREGATION: str = 'hour'

        if not (1 <= month <= 12):
            raise ValueError(f'month must be between 1 and 12, got {month}')
        
        sql_query: sql.Composable = self._get_janitza_energy_hour_sql(year, month)

        partition: str = f'{year}_{month:02}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._update_janitza_energy_mv(
            mv_name=mv_name,
            sql_select=sql_query
        )

    async def update_janitza_energy_day(self, year: int, month: int) -> None:
        AGGREGATION: str = 'day'

        if not (1 <= month <= 12):
            raise ValueError(f'month must be between 1 and 12, got {month}')
        
        sql_query: sql.Composable = self._get_janitza_energy_day_sql(year, month)

        partition: str = f'{year}_{month:02}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._update_janitza_energy_mv(
            mv_name=mv_name,
            sql_select=sql_query
        )

    async def update_janitza_energy_month(self, year: int) -> None:
        AGGREGATION: str = 'month'

        sql_query: sql.Composable = self._get_janitza_energy_month_sql(year)

        partition: str = f'{year}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._update_janitza_energy_mv(
            mv_name=mv_name,
            sql_select=sql_query
        )

    # DROP

    async def _drop_janitza_energy_hour(self, year: int, month: int) -> None:
        AGGREGATION: str = 'hour'

        if not (1 <= month <= 12):
            raise ValueError(f'month must be between 1 and 12, got {month}')

        partition: str = f'{year}_{month:02}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._drop_janitza_energy_mv(mv_name)

    async def _drop_janitza_energy_day(self, year: int, month: int) -> None:
        AGGREGATION: str = 'day'

        if not (1 <= month <= 12):
            raise ValueError(f'month must be between 1 and 12, got {month}')

        partition: str = f'{year}_{month:02}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._drop_janitza_energy_mv(mv_name)

    async def _drop_janitza_energy_month(self, year: int) -> None:
        AGGREGATION: str = 'month'

        partition: str = f'{year}'

        mv_name: str = f'janitza_energy_{AGGREGATION}_{partition}'

        await self._drop_janitza_energy_mv(mv_name)

    # SQL

    def _get_janitza_energy_hour_sql(self, year: int, month: int) -> sql.Composable:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-01T00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(months=1)

        sql_query: sql.Composable = sql.SQL ('''
            WITH period AS (
                SELECT
                    {start_ts} AS start_ts,
                    {end_ts} AS end_ts
            ),
            data AS (
                SELECT
                    janitza_id,
                    date_trunc('hour', pe.start_ts AT TIME ZONE 'Europe/Berlin')::date AS hour,
                    date_trunc('hour', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts_hour,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_consumed) AS energy_active_consumed,
                    SUM(energy_reactive_fundamental_supplied) AS energy_reactive_fundamental_supplied,
                    SUM(energy_reactive_fundamental_consumed) AS energy_reactive_fundamental_consumed
                FROM janitza_energy pe, period p
                WHERE pe.start_ts >= p.start_ts
                AND pe.start_ts <  p.end_ts
                GROUP BY janitza_id, hour, start_ts_hour
                ORDER BY janitza_id, hour, start_ts_hour
            )
            SELECT
                janitza_id,
                start_ts_hour AS start_ts,
                start_ts_hour + INTERVAL '1 hour' AS end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            FROM data
        ''').format(
            start_ts=sql.SQL('{}').format(sql.Literal(start_ts)),
            end_ts=sql.SQL('{}').format(sql.Literal(end_ts))
        )

        return sql_query
    
    def _get_janitza_energy_day_sql(self, year: int, month: int) -> sql.Composable:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-01T00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(months=1)

        sql_query: sql.Composable = sql.SQL ('''
            WITH period AS (
                SELECT
                    {start_ts} AS start_ts,
                    {end_ts} AS end_ts
            ),
            data AS (
                SELECT
                    janitza_id,
                    date_trunc('day', pe.start_ts AT TIME ZONE 'Europe/Berlin')::date AS day,
                    date_trunc('day', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts_day,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_consumed) AS energy_active_consumed,
                    SUM(energy_reactive_fundamental_supplied) AS energy_reactive_fundamental_supplied,
                    SUM(energy_reactive_fundamental_consumed) AS energy_reactive_fundamental_consumed
                FROM janitza_energy pe, period p
                WHERE pe.start_ts >= p.start_ts
                AND pe.start_ts <  p.end_ts
                GROUP BY janitza_id, day, start_ts_day
                ORDER BY janitza_id, day, start_ts_day
            )
            SELECT
                janitza_id,
                start_ts_day AS start_ts,
                (start_ts_day AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 day') AT TIME ZONE 'Europe/Berlin' AS end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            FROM data
        ''').format(
            start_ts=sql.SQL('{}').format(sql.Literal(start_ts)),
            end_ts=sql.SQL('{}').format(sql.Literal(end_ts))
        )

        return sql_query

    def _get_janitza_energy_month_sql(self, year: int) -> sql.Composable:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-01-01T00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(years=1)

        # Generate a list of MV names for the current year
        # ex.: janitza_energy_day_2025_10
        month_mvs: list[str] = [
            f'janitza_energy_day_{year}_{i:02}' for i in range(1, 13)
        ]

        # Generate a SELECT query for each MV
        # ex.: SELECT * FROM janitza_energy_day_2025_10
        select_sql_mvs: list[sql.Composable] = [
            sql.SQL('SELECT * FROM {mv}').format(
                mv=sql.Identifier(mv)
            )
            for mv in month_mvs
        ]

        # Generate a UNION ALL query for all of the MVs of the current year
        # ex.:
        # SELECT * FROM janitza_energy_day_2025_01 UNION ALL
        # SELECT * FROM janitza_energy_day_2025_02 UNION ALL
        # ...
        # SELECT * FROM janitza_energy_day_2025_12 ALL
        union_sql: sql.Composable = sql.SQL(' UNION ALL ').join(select_sql_mvs)

        sql_query: sql.Composable = sql.SQL('''
            WITH period AS (
                SELECT
                    {start_ts} AS start_ts,
                    {end_ts} AS end_ts
            ),
            {janitza_energy_day_year} AS (
                {union_sql}
            ),
            data AS (
                SELECT
                    janitza_id,
                    date_trunc('month', pe.start_ts AT TIME ZONE 'Europe/Berlin')::date AS month,
                    date_trunc('month', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts_month,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_consumed) AS energy_active_consumed,
                    SUM(energy_reactive_fundamental_supplied) AS energy_reactive_fundamental_supplied,
                    SUM(energy_reactive_fundamental_consumed) AS energy_reactive_fundamental_consumed
                FROM {janitza_energy_day_year} pe, period p
                WHERE pe.start_ts >= p.start_ts
                AND pe.start_ts <  p.end_ts
                GROUP BY janitza_id, month, start_ts_month
                ORDER BY janitza_id, month, start_ts_month
            )
            SELECT
                janitza_id,
                start_ts_month AS start_ts,
                (start_ts_month AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 month') AT TIME ZONE 'Europe/Berlin' AS end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            FROM data
        ''').format(
            start_ts=sql.SQL('{}').format(sql.Literal(start_ts)),
            end_ts=sql.SQL('{}').format(sql.Literal(end_ts)),
            janitza_energy_day_year=sql.Identifier(f'janitza_energy_day_{year}'),
            union_sql=union_sql
        )
        
        return sql_query

    def _get_janitza_energy_year_sql(self) -> sql.Composable:
        sql_select: sql.Composable = sql.SQL('''
            WITH data AS (
                SELECT
                    janitza_id,
                    date_trunc('year', pe.start_ts AT TIME ZONE 'Europe/Berlin')::date AS year,
                    date_trunc('year', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts_year,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_consumed) AS energy_active_consumed,
                    SUM(energy_reactive_fundamental_supplied) AS energy_reactive_fundamental_supplied,
                    SUM(energy_reactive_fundamental_consumed) AS energy_reactive_fundamental_consumed
                FROM {janitza_energy_month} pe
                GROUP BY janitza_id, year, start_ts_year
                ORDER BY janitza_id, year, start_ts_year
            )
            SELECT
                janitza_id,
                start_ts_year AS start_ts,
                (start_ts_year AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 year') AT TIME ZONE 'Europe/Berlin' AS end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            FROM data
        ''').format(
            janitza_energy_month=sql.Identifier('janitza_energy_month')
        )

        return sql_select

    # Util

    async def _update_janitza_energy_mv( 
            self, mv_name: str,
            sql_select: sql.Composable
        ) -> None:
        sql_mv_exists: sql.Composable = sql.SQL('''
            SELECT EXISTS (
                SELECT 1
                FROM pg_matviews
                WHERE matviewname = {mv_name}
            )
        ''').format(
            mv_name=sql.Placeholder('mv_name')
        )

        sql_refresh: sql.Composable = sql.SQL(
            'REFRESH MATERIALIZED VIEW {mv_name}'
        ).format(
            mv_name=sql.Identifier(mv_name)
        )

        sql_drop: sql.Composable = sql.SQL(
            'DROP MATERIALIZED VIEW IF EXISTS {mv_name}'
        ).format(
            mv_name=sql.Identifier(mv_name)
        )

        sql_create: sql.Composable = sql.Composed([
            sql.SQL(
                'CREATE MATERIALIZED VIEW {mv_name} AS'
            ).format(
                mv_name=sql.Identifier(mv_name)
            ),
            sql_select
        ])

        sql_idx: sql.Composable = sql.SQL(
            'CREATE INDEX {idx_name} ON {mv_name} ({cols})'
        ).format(
            idx_name=sql.Identifier(f'idx_{mv_name}_janitza_id_start_ts'),
            mv_name=sql.Identifier(mv_name),
            cols=sql.SQL(', ').join(map(sql.Identifier, ['janitza_id', 'start_ts']))
        )

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_mv_exists, {'mv_name': mv_name})
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')

                mv_exists: bool = cur.fetchone()[0]

                try:
                    if mv_exists:
                        cur.execute(sql_refresh)
                    else:
                        cur.execute(sql_drop)
                        cur.execute(sql_create)
                        cur.execute(sql_idx)

                    self._conn.commit()
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')

        await asyncio.to_thread(run_query)

    async def _drop_janitza_energy_mv(self, mv_name: str) -> None:
        sql_query: sql.Composable = sql.SQL(
            'DROP MATERIALIZED VIEW IF EXISTS {mv_name}'
        ).format(
            mv_name=sql.Identifier(mv_name)
        )

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_query)
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')
                self._conn.commit()

        await asyncio.to_thread(run_query)

    ################################################################
    # VIEWS
    ################################################################

    # CREATE or REPLACE

    async def _create_or_replace_janitza_energy_hour_view(self) -> None:
        AGGREGATION: str = 'hour'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._create_or_replace_janitza_energy_view(view_name)
    
    async def _create_or_replace_janitza_energy_day_view(self) -> None:
        AGGREGATION: str = 'day'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._create_or_replace_janitza_energy_view(view_name)

    async def _create_or_replace_janitza_energy_month_view(self) -> None:
        AGGREGATION: str = 'month'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._create_or_replace_janitza_energy_view(view_name)

    async def _create_or_replace_janitza_energy_year_view(self) -> None:
        AGGREGATION: str = 'year'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        sql_select: sql.Composable = self._get_janitza_energy_year_sql()

        sql_create: sql.Composable = sql.Composed([
            sql.SQL(
            'CREATE OR REPLACE VIEW {view_name} AS'
            ).format(
                view_name=sql.Identifier(view_name)
            ),
            sql_select
        ])

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_create)
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')
                self._conn.commit()

        await asyncio.to_thread(run_query)

    # DROP

    async def _drop_janitza_energy_hour_view(self) -> None:
        AGGREGATION: str = 'hour'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._drop_janitza_energy_view(view_name)

    async def _drop_janitza_energy_day_view(self) -> None:
        AGGREGATION: str = 'day'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._drop_janitza_energy_view(view_name)

    async def _drop_janitza_energy_month_view(self) -> None:
        AGGREGATION: str = 'month'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._drop_janitza_energy_view(view_name)

    async def _drop_janitza_energy_year_view(self) -> None:
        AGGREGATION: str = 'year'
        view_name: str = f'janitza_energy_{AGGREGATION}'

        await self._drop_janitza_energy_view(view_name)

    # Util

    async def _create_or_replace_janitza_energy_view(self, view_name: str) -> None:
        mv_name: str = f'{view_name}_%'
        
        sql_query: sql.Composable = sql.SQL('''
            DO $$
            DECLARE
                mv_name text;
                sql_text text := 'CREATE OR REPLACE VIEW {view_name} AS ';
                first boolean := true;
            BEGIN
                FOR mv_name IN
                    SELECT matviewname
                    FROM pg_matviews
                    WHERE matviewname LIKE {mv_name}
                    ORDER BY matviewname
                LOOP
                    IF first THEN
                        sql_text := sql_text || 'SELECT * FROM ' || mv_name;
                        first := false;
                    ELSE
                        sql_text := sql_text || ' UNION ALL SELECT * FROM ' || mv_name;
                    END IF;
                END LOOP;
                                            
                IF first THEN
                    RAISE NOTICE 'No materialized views match pattern %', {mv_name};
                    RETURN;
                END IF;

                EXECUTE sql_text;
            END $$
        ''').format(
            view_name=sql.Identifier(view_name),
            mv_name=sql.Literal(mv_name)
        )

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_query)
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')
                self._conn.commit()

        await asyncio.to_thread(run_query)

    async def _drop_janitza_energy_view(self, view_name: str) -> None:
        sql_query: sql.Composable = sql.SQL(
            'DROP VIEW IF EXISTS {view_name}'
        ).format(
            view_name=sql.Identifier(view_name)
        )

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_query)
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')
                self._conn.commit()

        await asyncio.to_thread(run_query)

    ################################################################
    # Util
    ################################################################

    async def _get_matviews(self, mv_name_pattern: str) -> list[str]:
        sql_query: sql.Composable = sql.SQL('''
            SELECT matviewname
            FROM pg_matviews
            WHERE matviewname LIKE {mv_name_pattern}
            ORDER BY matviewname;
        ''').format(
            mv_name_pattern=sql.Literal(mv_name_pattern)
        )

        def run_query() -> list[str]:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                mv_names: list[str] = [r[0] for r in cur.fetchall()]
            
            return mv_names
        
        mv_names: list[str] = await asyncio.to_thread(run_query)

        return mv_names
    
    async def _get_views(self, view_name_pattern: str) -> list[str]:
        sql_query: sql.Composable = sql.SQL('''
            SELECT viewname
            FROM pg_views
            WHERE schemaname = 'public' AND viewname LIKE {view_name_pattern}
            ORDER BY viewname;
        ''').format(
            view_name_pattern=sql.Literal(view_name_pattern)
        )

        def run_query() -> list[str]:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                view_names: list[str] = [r[0] for r in cur.fetchall()]
            
            return view_names
        
        view_names: list[str] = await asyncio.to_thread(run_query)

        return view_names

    def _get_range_from_interval(
            self,
            interval: str,
            tz: str='Europe/Berlin'
        ) -> tuple[pd.Timestamp, pd.Timestamp]:
        INTERVALS: list[str] = [
            'this_hour', 'last_hour', 'last_three_hours',
            'today', 'yesterday', 'last_three_days',
            'this_week', 'last_week', 'last_three_weeks',
            'this_month', 'last_month', 'last_three_months',
            'this_year', 'last_year', 'last_three_years'
        ]

        if interval not in INTERVALS:
            raise ValueError(
                f"Interval must be one of {', '.join(INTERVALS)}, got {interval!r}"
            )
        
        now: dt.datetime = pd.Timestamp.now(tz)
        last_midnight: dt.datetime = now.normalize()
        next_midnight: dt.datetime = last_midnight + pd.Timedelta(days=1)
        previous_midnight: dt.datetime = last_midnight - pd.Timedelta(days=1)
        start_of_week: dt.datetime = last_midnight - dt.timedelta(days=now.weekday())
        start_of_month: dt.datetime = last_midnight.replace(day=1)
        start_of_year: dt.datetime = last_midnight.replace(month=1, day=1)
        
        if interval == 'this_hour':
            start_ts: dt.datetime = now.normalize().replace(hour=now.hour)
            end_ts: dt.datetime = start_ts + pd.Timedelta(hours=1)

            return start_ts, end_ts
        
        if interval == 'last_hour':
            end_ts: dt.datetime = now.normalize().replace(hour=now.hour)
            start_ts: dt.datetime = end_ts - pd.Timedelta(hours=1)

            return start_ts, end_ts
        
        if interval == 'last_three_hours':
            end_ts: dt.datetime = now.normalize().replace(hour=now.hour)
            start_ts: dt.datetime = end_ts - pd.Timedelta(hours=3)

            return start_ts, end_ts
        
        if interval == 'today':
            start_ts: dt.datetime = last_midnight
            end_ts: dt.datetime = next_midnight

            return start_ts, end_ts
        
        if interval == 'yesterday':
            start_ts: dt.datetime = previous_midnight
            end_ts: dt.datetime = last_midnight

            return start_ts, end_ts
        
        if interval == 'last_three_days':
            start_ts: dt.datetime = last_midnight - pd.Timedelta(days=3)
            end_ts: dt.datetime = last_midnight

            return start_ts, end_ts
        
        if interval == 'this_week':
            start_ts: dt.datetime = start_of_week
            end_ts: dt.datetime = start_of_week + pd.Timedelta(weeks=1)

            return start_ts, end_ts
        
        if interval == 'last_week':
            end_ts: dt.datetime = start_of_week
            start_ts: dt.datetime = start_of_week - pd.Timedelta(weeks=1)

            return start_ts, end_ts
        
        if interval == 'last_three_weeks':
            end_ts: dt.datetime = start_of_week
            start_ts: dt.datetime = start_of_week - pd.Timedelta(weeks=3)

            return start_ts, end_ts
        
        if interval == 'this_month':
            start_ts: dt.datetime = start_of_month
            end_ts: dt.datetime = start_ts + pd.offsets.MonthBegin(1)

            return start_ts, end_ts
        
        if interval == 'last_month':
            end_ts: dt.datetime = start_of_month
            start_ts: dt.datetime = end_ts - pd.offsets.MonthBegin(1)

            return start_ts, end_ts
        
        if interval == 'last_three_months':
            end_ts: dt.datetime = start_of_month
            start_ts: dt.datetime = end_ts - pd.offsets.MonthBegin(3)

            return start_ts, end_ts
        
        if interval == 'this_year':
            start_ts: dt.datetime = start_of_year
            end_ts: dt.datetime = start_ts + pd.offsets.YearBegin(1)

            return start_ts, end_ts
        
        if interval == 'last_year':
            end_ts: dt.datetime = start_of_year
            start_ts: dt.datetime = end_ts - pd.offsets.YearBegin(1)

            return start_ts, end_ts
        
        if interval == 'last_three_years':
            end_ts: dt.datetime = start_of_year
            start_ts: dt.datetime = end_ts - pd.offsets.YearBegin(3)

            return start_ts, end_ts

    ################################################################
    # DB util
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
