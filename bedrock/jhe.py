"""
jhe.py
======

"""

# Standard library
import asyncio
from pathlib import Path

# Third-party
import pandas as pd
import psycopg2
from psycopg2 import sql

from settings import settings


class JHE:
    def __init__(self):
        self._conn = self._get_db_connection()

    ################################################################
    # Public
    ################################################################

    def get_main_data(self) -> str:
        try:
            df: pd.DataFrame | None = self._get_main_data()
        except Exception as e:
            return f'error: {e}'
        
        df['poi'] = df['poi'].str.replace('UW Baumgart', 'UWS Baumgart')
        df['poi'] = df['poi'].str.replace('UW Wittstock', 'UWS Wittstock')
        df['poi'] = df['poi'].str.replace('UW Quering', 'UWS Quering')
        df['poi'] = df['poi'].str.replace('UW Grimme', 'UWS Grimme')
        
        return df.to_csv(sep=';', decimal=',', index=False)

    def get_mini_bess(self) -> pd.DataFrame:
        temp_file_path: Path = Path('./res/mini_bess.csv')

        df: pd.DataFrame = pd.read_csv(
            temp_file_path,
            engine='pyarrow'
        )

        return df

    async def update_max_power_active_consumed(self) -> None:
        sql_query: sql.Composable = sql.SQL('''
            REFRESH MATERIALIZED VIEW {table}
        ''').format(
            table=sql.Identifier('plant_max_power_active_consumed')
        )

        def run_query():
            with self._conn.cursor() as cur:
                cur.execute(sql_query)

        await asyncio.to_thread(run_query)

    def get_max_power_active_consumed(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            WITH params AS (
                SELECT
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin')::date AS start_ts,
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin')::date + INTERVAL '1 year' AS end_ts
            ),
            data AS (
                SELECT
                    main_data_2_id,
                    DATE_TRUNC('day', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts,
                    DATE_TRUNC('day', pe.start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 day' AS end_ts,
                    ROUND(MAX(energy_active_consumed) * 4, 3) AS max_power_active_consumed
                FROM {plant_energy} pe, params p
                WHERE pe.start_ts >= p.start_ts
                AND pe.start_ts < p.end_ts
                GROUP BY DATE_TRUNC('day', pe.start_ts AT TIME ZONE 'Europe/Berlin'), main_data_2_id
                ORDER BY main_data_2_id, DATE_TRUNC('day', pe.start_ts AT TIME ZONE 'Europe/Berlin')
            )
            SELECT
                md2.name AS plant_name,
                (pe.start_ts AT TIME ZONE 'Europe/Berlin')::date AS day,
                pe.start_ts,
                pe.end_ts,
                pe.max_power_active_consumed
            FROM data pe
            LEFT JOIN main_data_2 md2
            ON pe.main_data_2_id = md2.id
        ''').format(
            plant_energy=sql.Identifier('plant_energy')
        )

        sql_query: sql.Composable = sql.SQL('''
        SELECT
            *
        FROM {table}
        ''').format(
            table=sql.Identifier('plant_max_power_active_consumed')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)
            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df['max_power_active_consumed'] = df['max_power_active_consumed'].astype('float64')
        # df['day'] = pd.to_datetime(df['day']).dt.date

        return df

    ################################################################
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
    
    ################################################################
    # Internal methods
    ################################################################

    def _get_main_data(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                md2.see_number,
                md2.name,
                parks.name AS park,
                md2.nominal_power,
                md2.country,
                md2.commissioning_date,
                j.name AS poi_janitza,
                pois.name AS poi,
                spvs.name AS spv
            FROM {md2} md2
            LEFT JOIN {parks}
                ON md2.park_id = parks.id
            LEFT JOIN {spvs} spvs
                ON md2.spv_id = spvs.id
            LEFT JOIN {pois}
                ON md2.poi_id = pois.id
            LEFT JOIN {j} j
                ON pois.janitza_id = j.id
        ''').format(
            md2=sql.Identifier('main_data_2'),
            j=sql.Identifier('janitzas'),
            parks=sql.Identifier('parks'),
            pois=sql.Identifier('pois'),
            spvs=sql.Identifier('spvs_current')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)
            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df['nominal_power'] = df['nominal_power'].astype('float64')
        df['commissioning_date'] = pd.to_datetime(df['commissioning_date']).dt.date

        return df
