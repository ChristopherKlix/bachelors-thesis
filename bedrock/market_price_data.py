"""
market_price_data.py
====================

This module provides functionality to ingest, store, and query European electricity market prices
from external sources such as energy-charts.info and EPEX SPOT.

It includes asynchronous methods for ETL operations and PostgreSQL database interactions.
"""

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests

from settings import settings


class MarketPriceData:
    """
    Handles the ingestion, querying, and transformation of European electricity market prices.

    This class interacts with external APIs (e.g., energy-charts.info), performs ETL operations,
    and manages a PostgreSQL database table `market_prices` with views for quarter-hourly and hourly data.

    Attributes:
        _ENERGY_CHARTS_URL (str): Base URL for the energy-charts API.
        _BIDDING_ZONES (list[str]): List of supported European bidding zones.
    """

    _ENERGY_CHARTS_URL: str = 'https://api.energy-charts.info'
    _BIDDING_ZONES: list[str] = [
        'AT', 'BE', 'CH', 'CZ',
        'DE-LU', 'DE-AT-LU', 'DK1', 'DK2',
        'FR', 'HU', 'IT-North', 'NL',
        'NO2', 'PL', 'SE4', 'SI'
    ]

    def __init__(self):
        self._conn = self._get_db_connection()

    ################################################################
    # Public methods
    ################################################################

    async def start_ingress(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None,
            bzn: str='DE-LU'
        ) -> dict[str, Any]:
        """
        Start the market price ingestion process.

        This method requests Day-Ahead and Intraday Continuous Average market
        prices from EPEX SPOT and writes them into the `market_prices` table.

        :param interval: Optional. Predefined interval string (e.g., 'today', 'this_month'). 
                        Either this or start_ts/end_ts must be provided, not both.
        :param start_ts: Optional. Start timestamp for the request. Must be tz-aware.
        :param end_ts: Optional. End timestamp for the request. Must be tz-aware.
        :param bzn: Bidding zone code. Must be one of the recognized European bidding zones.
        :raises ValueError: If the DataFrame validation fails during write to the database (`_write_market_prices_to_db`).
        :return: Dictionary with statistics about inserts, updates, request time, write time, and total time.
        """
        t: pd.Timestamp = pd.Timestamp.now()

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError(
                'Provide either interval or start_ts/end_ts range, not both'
            )

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError(
                'If interval is not provided, both start_ts and end_ts must be given'
            )

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')
        
        if bzn not in self._BIDDING_ZONES:
            raise ValueError(f'bzn must be one of {self._BIDDING_ZONES}, got {bzn}')
        
        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)
        
        t_request_start: pd.Timestamp = pd.Timestamp.now()
        df: pd.DataFrame = await self._request_market_prices_from_epex_spot(
            start_ts=start_ts,
            end_ts=end_ts,
            bzn=bzn
        )
        t_request: pd.Timedelta = pd.Timestamp.now() - t_request_start

        if df.empty:
            stats: dict[str, Any] = {
                'detail': f'no data available for the requested '
                          f'interval {start_ts} - {end_ts}',
                'inserts': 0,
                'updates': 0,
                'time_request': t_request,
                'time_write': None,
                'time_total': pd.Timestamp.now() - t
            }

            return stats

        try:
            stats_write: dict[str, Any] = await self._write_market_prices_to_db(df)
        except ValueError as e:
            stats: dict[str, Any] = {
                'detail': f'Validating DataFrame failed: {e}',
                'inserts': 0,
                'updates': 0,
                'time_request': t_request,
                'time_write': None,
                'time_total': pd.Timestamp.now() - t
            }
            print(df)
            return stats

        
        stats: dict[str, Any] = {
            'inserts': stats_write['inserts'],
            'updates': stats_write['updates'],
            'time_request': t_request,
            'time_write': stats_write['time'],
            'time_total': pd.Timestamp.now() - t
        }

        return stats
    
    async def get(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None,
            markets: list[str]=['day_ahead'],
            resolution: int=900,
            bzn: str='DE-LU'
        ) -> pd.DataFrame:
        ALLOWED_RESOLUTIONS: list[int] = [900, 3600]

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError(
                'Provide either interval or start_ts/end_ts range, not both'
            )

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError(
                'If interval is not provided, both start_ts and end_ts must be given'
            )

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')
        
        if bzn not in self._BIDDING_ZONES:
            raise ValueError(f'bzn must be one of {self._BIDDING_ZONES}, got {bzn}')
        
        if resolution not in ALLOWED_RESOLUTIONS:
            raise ValueError(f'resolution must be one of {ALLOWED_RESOLUTIONS}, got {resolution}')
        
        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        tables: dict[int, str] = {
            900: 'market_prices_qh',
            3600: 'market_prices_h'
        }
        
        columns: list[sql.SQL] = [sql.Identifier(market) for market in markets]

        sql_query: sql.SQL = sql.SQL('''
            SELECT
                start_ts,
                end_ts,
                {columns}
            FROM {table}
            WHERE bidding_zone = {bzn}
            AND start_ts >= {start_ts}
            AND start_ts < {end_ts}
            ORDER BY start_ts ASC
        ''').format(
            table=sql.Identifier(tables[resolution]),
            columns=sql.SQL(', ').join(columns),
            bzn=sql.Placeholder('bzn'),
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts')
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query, {'bzn': bzn, 'start_ts': start_ts, 'end_ts': end_ts})
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df = await asyncio.to_thread(run_query)

        df = df.astype({market: 'float64' for market in markets})

        df = df.set_index('start_ts')

        return df

    async def get_day_ahead_with_negative_hours(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> pd.DataFrame:
        """
        Retrieve hours with negative day-ahead market prices and classify them by EEG categories.

        This method queries the `market_prices_h` view and identifies consecutive negative price intervals.
        It also computes EEG classification based on the number of consecutive negative hours.

        :param interval: Optional. Predefined interval string (e.g., 'today', 'last_week').
                            Either this or start_ts/end_ts must be provided, not both.
        :type interval: str, optional
        :param start_ts: Optional. Start timestamp for the query. Must be timezone-aware.
        :type start_ts: datetime.datetime, optional
        :param end_ts: Optional. End timestamp for the query. Must be timezone-aware.
        :type end_ts: datetime.datetime, optional
        :param bzn: Bidding zone code. Must be one of the supported European bidding zones.
        :type bzn: str
        :raises ValueError: If input parameters are invalid or unsupported.
        :return: DataFrame containing start/end timestamps, day-ahead prices, consecutive negative hours,
                    and EEG classification.
        :rtype: pandas.DataFrame
        """

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError(
                'Provide either interval or start_ts/end_ts range, not both'
            )

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError(
                'If interval is not provided, both start_ts and end_ts must be given'
            )

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')
        
        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        sql_query: sql.Composable = sql.SQL('''
            WITH hourly AS (
                WITH base AS (
                    SELECT
                        start_ts,
                        end_ts,
                        day_ahead,
                        LAG(day_ahead) OVER (ORDER BY start_ts) AS prev_day_ahead
                    FROM market_prices_h
                    WHERE bidding_zone = 'DE-LU'
                    AND start_ts >= {start_ts}
                    AND start_ts < {end_ts}
                ),
                groups AS (
                    SELECT
                        *,
                        SUM(
                            CASE WHEN (prev_day_ahead >= 0 OR day_ahead >= 0) THEN 1 ELSE 0 END
                        )
                        OVER (ORDER BY start_ts) AS grp
                    FROM base
                ),
                runs AS (
                    SELECT *,
                        CASE WHEN day_ahead < 0
                                THEN ROW_NUMBER() OVER (PARTITION BY grp ORDER BY start_ts)
                        END AS consecutive_negatives
                    FROM groups
                )
                SELECT start_ts, end_ts, day_ahead, consecutive_negatives,
                    CASE
                        WHEN consecutive_negatives IS NULL THEN NULL
                        WHEN consecutive_negatives BETWEEN 1 AND 1 THEN 1
                        WHEN consecutive_negatives BETWEEN 2 AND 2 THEN 2
                        WHEN consecutive_negatives BETWEEN 3 AND 3 THEN 3
                        WHEN consecutive_negatives BETWEEN 4 AND 5 THEN 4
                        ELSE 6
                    END AS eeg_negative_hours,
                    SUM(CASE WHEN day_ahead < 0 THEN 1 ELSE 0 END)
                        OVER (PARTITION BY grp) AS streak_length
                FROM runs
            ),
            quarter AS (
                SELECT start_ts, end_ts, day_ahead
                FROM market_prices_qh
                WHERE start_ts >= {start_ts}
                AND start_ts < {end_ts}
            )
            SELECT q.start_ts,
                q.end_ts,
                q.day_ahead,
                h.consecutive_negatives,
                h.eeg_negative_hours,
                h.streak_length
            FROM quarter q
            JOIN hourly h
            ON q.start_ts >= h.start_ts
            AND q.start_ts < h.end_ts
        ''').format(
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts')
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query, {'start_ts': start_ts, 'end_ts': end_ts})
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        # df = df.astype({
        #     'day_ahead': 'float64',
        #     'consecutive_negatives': 'int64',
        #     'eeg_negative_hours': 'int64',
        # })

        df = df.set_index('start_ts')

        return df

    async def rebuild_market_prices_table(self) -> None:
        """
        Rebuild the ``market_prices`` table from scratch.

        Drops the existing table (if any), creates a new table with the
        appropriate columns and constraints, adds an index, and creates
        two views:

        - ``market_prices_qh``: quarter-hourly view with pivoted prices
        for 'day_ahead' and 'intraday_avg' markets.
        - ``market_prices_h``: hourly aggregated view with rounded average
        prices for the same markets.

        The table schema:

        - id: INT, primary key, identity
        - ts: TIMESTAMPTZ, end of interval
        - resolution: INT, interval in seconds (e.g., 900, 3600)
        - market: TEXT, market type ('day_ahead', 'intraday_avg')
        - price: NUMERIC(10,3), market price
        - bidding_zone: TEXT, e.g., 'DE-LU'
        - source: TEXT, e.g., 'EEX', 'EPEX'
        - UNIQUE constraint on (market, bidding_zone, ts, resolution)

        The method runs synchronously inside ``asyncio.to_thread`` to avoid
        blocking the event loop.

        Raises
        ------
        psycopg2.Error
            If any SQL execution fails.
        """
        TABLE_NAME: str = 'market_prices'

        sql_drop_table: sql.Composable = sql.SQL('''
            DROP TABLE IF EXISTS {table}
        ''').format(
            table=sql.Identifier(TABLE_NAME)
        )

        sql_create_table: sql.Composable = sql.SQL('''
            CREATE TABLE {table} (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL,      -- end of interval
                resolution INT NOT NULL,      -- in seconds, e.g. 3600 or 900
                market TEXT NOT NULL,         -- 'day_ahead', 'intraday_avg'
                price NUMERIC(10,3) NOT NULL,
                bidding_zone TEXT NOT NULL,   -- 'DE-LU'
                source TEXT NOT NULL,         -- 'EEX', 'EPEX', etc.
                UNIQUE (market, bidding_zone, ts, resolution)
            )
        ''').format(
            table=sql.Identifier(TABLE_NAME)
        )

        sql_index: sql.Composable = sql.SQL('''
            CREATE INDEX {idx_name}
            ON {table} (bidding_zone, market, ts, resolution);
        ''').format(
            idx_name=sql.Identifier(f'idx_{TABLE_NAME}_zone_market_ts_res'),
            table=sql.Identifier(TABLE_NAME)
        )

        sql_view_qh: sql.Composable = sql.SQL('''
            CREATE OR REPLACE VIEW {view_name} AS
            WITH expanded AS (
                -- already quarter-hour prices, use as-is
                SELECT market, ts, resolution, price, source, bidding_zone
                FROM {table}
                WHERE resolution = 900

                UNION ALL

                -- only expand old hourly data if it exists
                SELECT
                    mp.market,
                    gs.ts,
                    900,
                    mp.price,
                    mp.source,
                    mp.bidding_zone
                FROM {table} mp
                CROSS JOIN LATERAL (
                    SELECT generate_series(
                        mp.ts - interval '45 min',
                        mp.ts,
                        interval '15 min'
                    ) AS ts
                ) gs
                WHERE mp.resolution = 3600
            ),
            pivoted AS (
                SELECT
                    ts - make_interval(secs => resolution) AS start_ts,
                    ts AS end_ts,
                    bidding_zone,
                    MAX(price) FILTER (WHERE market = 'day_ahead') AS day_ahead,
                    MAX(price) FILTER (WHERE market = 'intraday_avg') AS intraday_avg
                FROM expanded
                GROUP BY ts, resolution, bidding_zone
            )
            SELECT * FROM pivoted
            ORDER BY bidding_zone, start_ts;
        ''').format(
            view_name=sql.Identifier(f'{TABLE_NAME}_qh'),
            table=sql.Identifier(TABLE_NAME)
        )

        sql_view_h: sql.Composable = sql.SQL('''
            CREATE OR REPLACE VIEW {table} AS (
                SELECT
                    date_trunc('hour', start_ts) AS start_ts,
                    date_trunc('hour', start_ts) + INTERVAL '1 hour' AS end_ts,
                    bidding_zone,
                    ROUND(AVG(day_ahead), 3) AS day_ahead,
                    ROUND(AVG(intraday_avg), 3) AS intraday_avg
                FROM {market_prices_qh}
                GROUP BY bidding_zone, date_trunc('hour', start_ts)
                ORDER BY bidding_zone, start_ts
            )
        ''').format(
            table=sql.Identifier('market_prices_h'),
            market_prices_qh=sql.Identifier('market_prices_qh')
        )

        def run_query():
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_drop_table)
                    cur.execute(sql_create_table)
                    cur.execute(sql_index)
                    cur.execute(sql_view_qh)
                    cur.execute(sql_view_h)
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise e

            self._conn.commit()

        try:
            await asyncio.to_thread(run_query)
        except psycopg2.Error as e:
            raise e

    ################################################################
    # Ingress methods
    # 
    # These methods perform the ETL pipeline for the market prices
    ################################################################

    async def _request_market_prices_from_epex_spot(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime,
            bzn: str
        ) -> pd.DataFrame:
        """
        Request data Day Ahead and Intraday Continuous Average
        from the ENTSO-E Transparency Platform

        Legacy Note: Currently runs through the energy-charts aggregator

        1. Run request
        2. Parse data
        """

        # Legacy reuqest via energy-charts aggregator
        # DataFrame schema:
        # index: start_ts: datetime64[ns, UTC]
        # end_ts: datetime64[ns, UTC]
        # dayahead: float64
        df: pd.DataFrame | None = await asyncio.to_thread(
            self._get_day_ahead_from_energy_charts,
            start_ts=start_ts,
            end_ts=end_ts,
            bzn=bzn
        )

        if df is None:
            df = pd.DataFrame({
                'ts': pd.Series(dtype='datetime64[ns, UTC]'),
                'price': pd.Series(dtype='float64'),
                'market': pd.Series(dtype=str),
                'bidding_zone': pd.Series(dtype=str),
                'source': pd.Series(dtype=str)
            })
            return df

        df = df.reset_index()
        df = df.rename(columns={
            'end_ts': 'ts',
            'day_ahead': 'price'
        })

        df = df[['ts', 'price']]
        df['resolution'] = 900
        df['market'] = 'day_ahead'
        df['bidding_zone'] = bzn
        df['source'] = 'energy-charts'

        return df

    async def _write_market_prices_to_db(
            self,
            df: pd.DataFrame
        ) -> dict[str, int]:
        """
        Insert or update market prices into the `market_prices` table.

        Workflow:

        1. Validate the DataFrame via ``_validate_df``
        2. Transform the DataFrame into a list of tuples for bulk insertion
        3. Execute an UPSERT query using ``execute_values``:
        - Inserts new rows
        - Updates ``price`` and ``source`` if a conflict occurs
        4. Returns statistics on inserted and updated rows, and elapsed time

        DataFrame schema:

        - ``ts``: pd.Timestamp tz-aware, end of interval
        - ``resolution``: int, interval in seconds (e.g., 3600 or 900)
        - ``market``: str, ``'day_ahead'`` or ``'intraday'``
        - ``price``: float, market price
        - ``bidding_zone``: str, e.g., ``'DE-LU'``
        - ``source``: str, data source, e.g., ``'EPEX SPOT'``

        :param df: DataFrame with market prices to insert/update
        :type df: pd.DataFrame
        :return: Dictionary with number of inserts, updates, and elapsed time
        :rtype: dict[str, int | pd.Timedelta]
        """
        t: pd.Timestamp = pd.Timestamp.now()

        try:
            self._validate_market_prices_df(df)
        except ValueError as e:
            raise e

        sql_query: sql.Composable = sql.SQL('''
            INSERT INTO {table}
                (ts, resolution, market, price, bidding_zone, source)
            VALUES %s
            ON CONFLICT (ts, resolution, market, bidding_zone) DO UPDATE
            SET
                price = EXCLUDED.price,
                source = EXCLUDED.source
            RETURNING CASE WHEN xmax = 0 THEN 'insert' ELSE 'update' END AS action
        ''').format(
            table=sql.Identifier('market_prices')
        )

        # transform df to list of tuples for execute_values
        records: list[tuple] = [
            (
                row.ts.to_pydatetime(), row.resolution, row.market,
                row.price, row.bidding_zone, row.source
            )
            for row in df.itertuples(index=False)
        ]

        def run_insert():
            with self._conn.cursor() as cur:
                try:
                    rows = execute_values(cur, sql_query, records, fetch=True)
                except Exception as e:
                    self._conn.rollback()
                    raise e
                    return
            
            self._conn.commit()
            return rows
        
        rows: list[tuple] = await asyncio.to_thread(run_insert)

        stats: dict[str, int] = {
            'inserts': sum(1 for r in rows if r[0] == 'insert'),
            'updates': sum(1 for r in rows if r[0] == 'update'),
            'time': pd.Timestamp.now() - t
        }

        return stats

    def _validate_market_prices_df(self, df: pd.DataFrame):
        """
        Validate a DataFrame before inserting into the ``market_prices`` table.

        Checks performed:

        - Required columns exist: ``ts``, ``resolution``, ``market``, ``price``, ``bidding_zone``, ``source``
        - No nulls in required columns
        - Correct dtypes:
            - ``ts``: tz-aware datetime
            - ``resolution``: int
            - ``price``: numeric
        - ``market`` values are allowed (``'day_ahead'``, ``'intraday'``)
        - ``resolution`` > 0
        - (Optional) Additional checks like duplicates or realistic price ranges

        :param df: DataFrame to validate
        :type df: pd.DataFrame
        :raises ValueError: If any validation fails
        """
        required_cols: list[str] = [
            'ts', 'resolution', 'market', 'price', 'bidding_zone', 'source'
        ]

        # 1. All required columns present
        missing: list[str] = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        # 2. Non-nullable columns
        if df[required_cols].isnull().any().any():
            raise ValueError('Null values found in required columns')

        # 3. Correct types
        # if not pd.api.types.is_datetime64tz_dtype(df['ts']):
        if not isinstance(df['ts'].dtype, pd.DatetimeTZDtype):
            raise ValueError('ts must be tz-aware datetime')
        if not pd.api.types.is_integer_dtype(df['resolution']):
            raise ValueError('resolution must be int')
        if not pd.api.types.is_numeric_dtype(df['price']):
            raise ValueError('price must be numeric')

        # # 4. Allowed markets
        # allowed_markets = {'day_ahead', 'intraday'}
        # if not df['market'].isin(allowed_markets).all():
        #     raise ValueError('market column contains invalid values')

        # 5. Positive resolution
        if (df['resolution'] <= 0).any():
            raise ValueError('resolution must be > 0')

    async def _ingest_historical_data_from_csv(self, file_path: Path):
        """
        Ingest historical data from a CSV file

        1. Load CSV file
        2. Write the data into the ``market_prices`` table
        """
        df: pd.DataFrame = pd.read_csv(file_path)

        df['start_ts'] = pd.to_datetime(df['start_ts'])
        df['end_ts'] = pd.to_datetime(df['end_ts'])
        df['dayahead'] = df['dayahead'].astype('float64')

        df = df.rename(columns={
            'end_ts': 'ts',
            'dayahead': 'price'
        })

        df = df[['ts', 'price']]
        df['resolution'] = 3600
        df['market'] = 'day_ahead'
        df['bidding_zone'] = 'DE-LU'
        df['source'] = 'energy-charts'

        try:
            self._validate_market_prices_df(df)
        except Exception as e:
            raise e

        try:
            stats: dict[str, Any] = await self._write_market_prices_to_db(df)
        except Exception as e:
            raise e
        
        return stats

    ################################################################
    # Utility methods
    ################################################################

    def _get_range_from_interval(
            self,
            interval: str,
            tz: str='Europe/Berlin'
        ) -> tuple[pd.Timestamp, pd.Timestamp]:
        INTERVALS: list[str] = [
            'this_hour', 'last_hour', 'last_three_hours',
            'tomorrow', 'today', 'yesterday', 'last_three_days',
            'this_week', 'last_week', 'last_three_weeks',
            'this_month', 'last_month', 'last_three_months',
            'this_year', 'last_year', 'last_three_years'
        ]

        if interval not in INTERVALS:
            raise ValueError(f"Interval must be one of 'yesterday' or 'today', got {interval!r}")
        
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
        
        if interval == 'tomorrow':
            start_ts: dt.datetime = next_midnight
            end_ts: dt.datetime = start_ts + pd.Timedelta(days=1)

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

    def _split_into_days(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime,
            tz: str='Europe/Berlin'
        ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Split a time range into individual day intervals.

        This utility method divides a given timezone-aware datetime range into a list of
        (start, end) tuples representing each day within the range. The timestamps are
        converted to the specified timezone.

        :param start_ts: Start of the time range. Must be timezone-aware.
        :type start_ts: datetime.datetime
        :param end_ts: End of the time range. Must be timezone-aware.
        :type end_ts: datetime.datetime
        :param tz: Timezone to convert timestamps to. Defaults to 'Europe/Berlin'.
        :type tz: str
        :raises ValueError: If either start_ts or end_ts is not timezone-aware.
        :return: List of tuples, each representing a single day's start and end timestamps.
        :rtype: list[tuple[pandas.Timestamp, pandas.Timestamp]]
        """

        if start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')

        start_ts = pd.Timestamp(start_ts).tz_convert(tz)
        end_ts = pd.Timestamp(end_ts).tz_convert(tz)

        days: list[tuple[dt.datetime, dt.datetime]] = []

        cur: pd.Timestamp = start_ts

        while cur < end_ts:
            nxt: pd.Timestamp = min(
                (cur + pd.offsets.DateOffset(days=1)).normalize(),
                end_ts
            )
            
            if nxt <= cur:
                nxt = end_ts
            days.append((cur, nxt))
            cur = nxt

        return days

    ################################################################
    # Energy-charts data
    ################################################################

    def _get_day_ahead_from_energy_charts(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime,
            bzn: str
        ) -> pd.DataFrame | None:
        if start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')

        dfs: list[pd.DataFrame] = []

        intervals: list[tuple[dt.datetime, dt.datetime]] = self._split_into_days(
            start_ts=start_ts,
            end_ts=end_ts
        )

        for interval in intervals:
            try:
                interval_df: pd.DataFrame | None = self._get_day_ahead_from_energy_charts_batch(
                    start_ts=interval[0],
                    end_ts=interval[1],
                    bzn=bzn
                )

                if interval_df is not None:
                    dfs.append(interval_df)
            except RuntimeError as e:
                if len(dfs) == 0:
                    raise e
                
        if len(dfs) == 0:
            return None

        df: pd.DataFrame = pd.concat(dfs)

        return df
    
    def _get_day_ahead_from_energy_charts_batch(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime,
            bzn: str
        ) -> pd.DataFrame | None:
        url: str = f'{self._ENERGY_CHARTS_URL}/price'

        headers: dict[str, str] = {
            'Accept': 'application/json'
        }

        params: dict[str, str] = {
            'bzn': bzn,
            'start': f'{start_ts.isoformat(timespec='minutes')}',
            'end': f'{end_ts.isoformat(timespec='minutes')}'
        }

        response: requests.Response | None = None

        try:
            response = requests.get(
                url=url,
                headers=headers,
                params=params
            )
        except Exception as _:
            raise RuntimeError(f'Could not retrieve data.')
        
        if response.headers['content-type'] != 'application/json':
            if response.status_code == 404:
                return None
        
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            raise RuntimeError(f'{response.text}')

        ts_list: list[int] = data.get('unix_seconds')
        prices_list: list[float] = data.get('price')

        ts_list_dt: list[dt.datetime] = [
            pd.Timestamp.fromtimestamp(ts) for ts in ts_list
        ]

        # energy-charts only returns a timestamp for the start of the interval.
        # Since the API provides historical data as well as current data
        # the interval can either be 1 hour or 15 min depending on the timestamp.
        # We could infer the interval resolution if we take the Timedelta between
        # two records.
        # But the issues occur should a query only return one record
        # and there is no way to infer the resolution for the last timestamp.
        # We then need to fallback to the official
        # regulation for the EEX Day Ahead.
        # before 2025-10-01T00:00+02:00 -> hourly prices
        # after  2025-10-01T00:00+02:00 -> quarter-hourly prices
        # 
        # Since we need to include a fallback anyway and the regulations
        # define the European electricity market and not any data that might
        # as well not conform to official regulations
        # it is safe to assume all the prices follow these regulations.
        # This is not ideal but hey...

        start_ts_series: pd.Series = pd.Series(ts_list_dt)
        start_ts_series = start_ts_series.dt.tz_localize('UTC')

        sdac_ts: pd.Timestamp = pd.Timestamp.fromisoformat('2025-10-01T00:00')
        sdac_ts = sdac_ts.tz_localize('Europe/Berlin')
        hourly: pd.Timedelta = pd.Timedelta(hours=1)
        quarter_hourly: pd.Timedelta = pd.Timedelta(minutes=15)

        resolution_series: pd.Series = start_ts_series.apply(
            lambda ts: hourly if ts < sdac_ts else quarter_hourly
        )

        prices_series: pd.Series = pd.Series(prices_list)
        prices_series = prices_series.astype('float64')
        prices_series = prices_series / 1_000 * 100
        prices_series = prices_series.round(3)
        
        end_ts_series: pd.Series = start_ts_series + resolution_series

        data: dict[str, pd.Series] = {
            'start_ts': start_ts_series,
            'end_ts': end_ts_series,
            'day_ahead': prices_series
        }

        df: pd.DataFrame = pd.DataFrame(data)

        df = df.set_index('start_ts')

        mask: pd.Series = (df.index >= start_ts) & (df.index < end_ts)
        df = df.loc[mask]

        return df

    ################################################################
    # DB utility methods
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
