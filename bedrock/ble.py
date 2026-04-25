"""
ble.py
======

"""

# Standard library
import asyncio
import datetime as dt
import locale

# Third-party
import pandas as pd
import psycopg2
from psycopg2 import sql

from settings import settings


class BLE:
    def __init__(self):
        self._conn = self._get_db_connection()

    ################################################################
    # Public
    ################################################################

    async def get_ble_main_data(self) -> pd.DataFrame:
        df: pd.DataFrame | None = await self._get_ble_main_data()

        return df

    async def get_ble_energy(self) -> str:
        df: pd.DataFrame = await self._get_ble_energy()

        return df

    ################################################################
    # Day ahead
    ################################################################

    async def get_ble_day_ahead(self, resolution: int) -> pd.DataFrame:
        TABLES: dict[int, str] = {
            900: 'ble_day_ahead_qh',
            3600: 'ble_day_ahead_h'
        }

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                start_ts,
                end_ts,
                day_ahead
            FROM {table}
        ''').format(
            table=sql.Identifier(TABLES[resolution])
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        df = df.astype({
            'day_ahead': 'float64'
        })

        df = df.set_index('start_ts')

        return df

    async def update_ble_day_ahead_cache(self) -> None:
        sql_drop_qh: sql.Composable = sql.SQL(
            'DROP TABLE IF EXISTS ble_day_ahead_qh'
        )

        sql_query_qh: sql.SQL = sql.SQL('''
            CREATE TABLE ble_day_ahead_qh AS
            WITH period AS (
                SELECT 
                    DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_year,
                    DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 year' AS end_year
            ),
            base AS (
                SELECT
                    start_ts,
                    end_ts,
                    day_ahead,
                    LAG(day_ahead) OVER (ORDER BY start_ts) AS prev_day_ahead
                FROM market_prices_h, period
                WHERE bidding_zone = 'DE-LU'
                AND start_ts >= period.start_year
                AND start_ts < period.end_year
            ),
            groups AS (
                SELECT *,
                    SUM(CASE WHEN prev_day_ahead >= 0 OR day_ahead >= 0 THEN 1 ELSE 0 END) 
                    OVER (ORDER BY start_ts) AS grp
                FROM base
            ),
            data AS (
                SELECT *,
                    CASE WHEN day_ahead < 0 THEN ROW_NUMBER() OVER (PARTITION BY grp ORDER BY start_ts) END AS consecutive_negatives,
                    SUM(CASE WHEN day_ahead < 0 THEN 1 ELSE 0 END) OVER (PARTITION BY grp) AS streak_length
                FROM groups
            ),
            hourly AS (
                SELECT
                    start_ts,
                    end_ts,
                    day_ahead,
                    consecutive_negatives,
                    streak_length,
                    CASE
                        WHEN streak_length = 0 THEN NULL
                        WHEN streak_length <= 3 THEN streak_length
                        WHEN streak_length BETWEEN 4 AND 5 THEN 4
                        ELSE 6
                    END AS eeg_negative_hours_rule
                FROM data
            ),
            quarter_hourly AS (
                SELECT
                    start_ts,
                    end_ts,
                    day_ahead
                FROM market_prices_qh, period
                WHERE bidding_zone = 'DE-LU'
                AND start_ts >= period.start_year
                AND start_ts < period.end_year
            )
            SELECT
                qh.start_ts,
                qh.end_ts,
                qh.day_ahead,
                h.consecutive_negatives,
                h.streak_length,
                h.eeg_negative_hours_rule
            FROM quarter_hourly qh
            JOIN hourly h
            ON qh.start_ts >= h.start_ts
            AND qh.start_ts < h.end_ts
        ''')

        sql_drop_h: sql.Composable = sql.SQL(
            'DROP TABLE IF EXISTS ble_day_ahead_h'
        )

        sql_query_h: sql.SQL = sql.SQL('''
            CREATE TABLE ble_day_ahead_h AS
            WITH period AS (
                SELECT 
                    DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_year,
                    DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 year' AS end_year
            ),
            base AS (
                SELECT
                    start_ts,
                    end_ts,
                    day_ahead,
                    LAG(day_ahead) OVER (ORDER BY start_ts) AS prev_day_ahead
                FROM market_prices_h, period
                WHERE bidding_zone = 'DE-LU'
                AND start_ts >= period.start_year
                AND start_ts < period.end_year
            ),
            groups AS (
                SELECT *,
                    SUM(CASE WHEN prev_day_ahead >= 0 OR day_ahead >= 0 THEN 1 ELSE 0 END) 
                    OVER (ORDER BY start_ts) AS grp
                FROM base
            ),
            data AS (
                SELECT *,
                    CASE WHEN day_ahead < 0 THEN ROW_NUMBER() OVER (PARTITION BY grp ORDER BY start_ts) END AS consecutive_negatives,
                    SUM(CASE WHEN day_ahead < 0 THEN 1 ELSE 0 END) OVER (PARTITION BY grp) AS streak_length
                FROM groups
            )
            SELECT
                start_ts,
                end_ts,
                day_ahead,
                consecutive_negatives,
                streak_length,
                CASE
                    WHEN streak_length = 0 THEN NULL
                    WHEN streak_length <= 3 THEN streak_length
                    WHEN streak_length BETWEEN 4 AND 5 THEN 4
                    ELSE 6
                END AS eeg_negative_hours_rule
            FROM data
        ''')

        def run_query() -> None:
            cur = self._conn.cursor()

            try:
                cur.execute(sql_drop_qh)
                cur.execute(sql_drop_h)
                cur.execute(sql_query_qh)
                cur.execute(sql_query_h)
            except psycopg2.Error as e:
                self._conn.rollback()
                raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')
            
            self._conn.commit()
            
            cur.close()
        
        await asyncio.to_thread(run_query)

    ################################################################
    # Negative hours
    ################################################################

    async def get_ble_negative_hours_energy(self) -> pd.DataFrame:
        """
        Retrieve monthly negative hours energy data for BLE plants.

        This query aggregates plant energy during negative price hours for the
        current calendar year. Values are aggregated per month and per plant.

        :return: DataFrame indexed by ``see_number`` and ``month`` with monthly
                negative hours energy values.
        :rtype: pandas.DataFrame

        **Returned columns:**
            - ``see_number`` — Plant identifier
            - ``month`` — Year-month period
            - ``start_ts`` — Start of the monthly interval
            - ``end_ts`` — End of the monthly interval
            - ``energy_active_supplied`` — Supplied active energy (MWh, negative sign)
            - ``energy_active_supplied_neg_null`` — Supplied active energy with NULL
            values replaced by zero (MWh, negative sign)

        **Notes:**
            - Only data from the current year (Europe/Berlin timezone) is returned.
            - Specific ``see_number`` values are blacklisted and excluded.
            - Energy values are inverted (multiplied by -1) to reflect convention.
            - The ``month`` column is converted to a :class:`pandas.Period` with
            monthly frequency.
        """
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                *
            FROM {ble_negative_hours_energy_day}
            ORDER BY see_number, start_ts
        ''').format(
            ble_negative_hours_energy_day=sql.Identifier('ble_negative_hours_energy_day')
        )

        sql_query: sql.Composable = sql.SQL('''
            WITH params AS (
                SELECT
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin')::date AS start_ts,
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin')::date + INTERVAL '1 year' AS end_ts,
                    INTERVAL '1 day' AS step_interval
            ),
            intervals AS (
                SELECT
                    generate_series(
                        start_ts,
                        end_ts - step_interval,
                        step_interval
                    ) AT TIME ZONE 'Europe/Berlin' AS start_ts,
                    step_interval
                FROM params
            ),
            plants AS (
                SELECT
                    id AS main_data_2_id,
                    see_number,
                    name,
                    park_id
                FROM {main_data_2}
                WHERE see_number LIKE 'SEE%%'
            )
            SELECT
                p.see_number,
                date_trunc('day', i.start_ts AT TIME ZONE 'Europe/Berlin')::date AS day,
                p.name AS plant_name,
                i.start_ts AS start_ts,
                (i.start_ts AT TIME ZONE 'Europe/Berlin' + i.step_interval) AT TIME ZONE 'Europe/Berlin' AS end_ts,
                nhe.energy_active_supplied,
                nhe.energy_active_supplied_neg_null
            FROM plants p
            CROSS JOIN intervals i
            LEFT JOIN {nhe} nhe
                ON nhe.see_number = p.see_number
                AND nhe.start_ts = i.start_ts
            LEFT JOIN {pe} pe
                ON p.main_data_2_id = pe.main_data_2_id
                AND nhe.start_ts = pe.start_ts
            ORDER BY p.name, i.start_ts     
        ''').format(
            main_data_2=sql.Identifier('main_data_2'),
            nhe=sql.Identifier('ble_negative_hours_energy_day'),
            pe=sql.Identifier('plant_energy_day')
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        df = df.astype({
            'energy_active_supplied': 'float64',
            'energy_active_supplied_neg_null': 'float64'
        })

        df['energy_active_supplied'] *= (-1)
        df['energy_active_supplied_neg_null'] *= (-1)

        df['day'] = (
            pd.to_datetime(df['day'])
            .dt.to_period('D')
        )

        df = df.set_index(['see_number', 'day'])

        return df

    async def update_ble_negative_hours_energy_cache(self) -> None:
        sql_drop: sql.Composable = sql.SQL(
            'DROP TABLE IF EXISTS ble_negative_hours_energy_day'
        )

        # Only current year
        sql_query: sql.SQL = sql.SQL('''
            CREATE TABLE ble_negative_hours_energy_day AS
            WITH data_0 AS (
                SELECT
                    md2.see_number,
                    nhe.start_ts,
                    nhe.end_ts,
                    pe.energy_active_supplied,
                    COALESCE(nhe.energy_active_supplied, 0.000) AS energy_active_supplied_neg_null
                FROM negative_hours_energy nhe
                LEFT JOIN main_data_2 md2
                    ON nhe.main_data_2_id = md2.id
                LEFT JOIN plant_energy pe
                    ON nhe.start_ts = pe.start_ts AND nhe.main_data_2_id = pe.main_data_2_id
                WHERE nhe.start_ts >= DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin'
                AND nhe.start_ts < DATE_TRUNC('year', NOW() AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 year'
            ),
            data_1 AS (
                SELECT
                    see_number,
                    DATE_TRUNC('day', start_ts AT TIME ZONE 'Europe/Berlin')::date AS day,
                    DATE_TRUNC('day', start_ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'Europe/Berlin' AS start_ts_day,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_supplied_neg_null) AS energy_active_supplied_neg_null
                FROM data_0
                GROUP BY see_number, day, start_ts_day
            )
            SELECT
                see_number,
                day,
                start_ts_day AS start_ts,
                (start_ts_day AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 day') AT TIME ZONE 'Europe/Berlin' AS end_ts,
                energy_active_supplied,
                energy_active_supplied_neg_null
            FROM data_1
        ''')

        # Entire data
        sql_query: sql.Composable = sql.SQL('''
            CREATE TABLE ble_negative_hours_energy_day AS
            WITH data_0 AS (
                SELECT
                    md2.see_number,
                    nhe.start_ts,
                    nhe.end_ts,
                    pe.energy_active_supplied,
                    COALESCE(nhe.energy_active_supplied, 0.000) AS energy_active_supplied_neg_null
                FROM negative_hours_energy nhe
                LEFT JOIN main_data_2 md2
                    ON nhe.main_data_2_id = md2.id
                LEFT JOIN plant_energy pe
                    ON nhe.start_ts = pe.start_ts
                AND nhe.main_data_2_id = pe.main_data_2_id
            ),
            data_1 AS (
                SELECT
                    see_number,
                    DATE_TRUNC('day', start_ts AT TIME ZONE 'Europe/Berlin')::date AS day,
                    DATE_TRUNC('day', start_ts AT TIME ZONE 'Europe/Berlin')
                        AT TIME ZONE 'Europe/Berlin' AS start_ts_day,
                    SUM(energy_active_supplied) AS energy_active_supplied,
                    SUM(energy_active_supplied_neg_null) AS energy_active_supplied_neg_null
                FROM data_0
                GROUP BY see_number, day, start_ts_day
            )
            SELECT
                see_number,
                day,
                start_ts_day AS start_ts,
                (start_ts_day AT TIME ZONE 'Europe/Berlin' + INTERVAL '1 day')
                    AT TIME ZONE 'Europe/Berlin' AS end_ts,
                energy_active_supplied,
                energy_active_supplied_neg_null
            FROM data_1
        ''')

        def run_query() -> pd.DataFrame:
            cur = self._conn.cursor()
            try:
                cur.execute(sql_drop)
                cur.execute(sql_query)
            except psycopg2.Error as e:
                self._conn.rollback()
            
            self._conn.commit()
            cur.close()
        
        await asyncio.to_thread(run_query)

    async def get_ble_negative_hours_energy_month(self) -> pd.DataFrame:
        df: pd.DataFrame = await BLE().get_ble_negative_hours_energy()

        df = df.reset_index()
        df = df.set_index('see_number')

        plant_name_mapping = df['plant_name'].copy().drop_duplicates()

        df = df.drop(columns=['plant_name', 'day', 'end_ts'])
        df = df.reset_index()
        df['start_ts'] = df['start_ts'].dt.tz_convert('Europe/Berlin')
        df = df.set_index(['see_number', 'start_ts'])

        df = (
            df
            .groupby(level='see_number')
            .resample('MS', level='start_ts')
            .sum()
        )
        df = df.reset_index()
        df['end_ts'] = df['start_ts'] + pd.DateOffset(months=1)
        df['month'] = df['start_ts'].dt.strftime('%Y-%m')

        df['start_ts'] = df['start_ts'].dt.tz_convert('UTC')
        df['end_ts'] = df['end_ts'].dt.tz_convert('UTC')

        df['start_ts_europe_berlin'] = df['start_ts'].dt.tz_convert('Europe/Berlin')
        df['end_ts_europe_berlin'] = df['end_ts'].dt.tz_convert('Europe/Berlin')

        df = df.set_index(['see_number'])
        df['plant_name'] = plant_name_mapping
        df = df.reset_index()

        df = df[[
            'see_number', 'month',
            'start_ts', 'end_ts',
            'energy_active_supplied', 'energy_active_supplied_neg_null'
        ]]

        df = df.set_index(['see_number', 'month'])

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

    async def _get_ble_main_data(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                plants.id,
                plants.name,
                parks.id as park_id,
                parks.name as park_name,
                plants.see_number,
                plants.eeg_key,
                plants.eeg_number,
                plants.report_id,
                plants.rated_power,
                plants.commissioning_date,
                plants.country,
                plants.federal_state,
                plants.zip_code,
                plants.nominal_power,
                plants.feed_in_date,
                plants.longitude,
                plants.latitude,
                plants.elevation,
                plants.azimuth,
                plants.slope,
                plants.controller_name,
                plants.bank,
                plants.om,
                plants.reporting,
                plants.plant_janitza_id,
                plant_j.name AS plant_janitza_name,
                plants.poi_id,
                plants.spv_id,
                plants.portfolio_id,
                portfolios.name AS portfolio_name,
                pois.name AS poi_name,
                pois.sel_number AS poi_sel_number,
                pois.san_number AS poi_san_number,
                pois.janitza_id AS poi_janitza_id,
                poi_j.name AS poi_janitza_name,
                st.short_name AS station_type_short_name,
                go.id AS grid_operator_id,
                go.name AS grid_operator_name,
                go.snb_number AS grid_operator_snb_number,
                got.short_name AS grid_operator_type_name,
                spvs.short_name AS spv_short_name,
                spvs.name AS spv_name
            FROM {plants} plants
            LEFT JOIN {parks}
                ON plants.park_id = parks.id
            LEFT JOIN {plant_j} plant_j
                ON plants.plant_janitza_id = plant_j.id
            LEFT JOIN {portfolios}
                ON plants.portfolio_id = portfolios.id
            LEFT JOIN {pois} pois
                ON plants.poi_id = pois.id
            LEFT JOIN {poi_j} poi_j
                ON pois.janitza_id = poi_j.id
            LEFT JOIN {st} st
                ON pois.station_type_id = st.id
            LEFT JOIN {go} go
                ON pois.grid_operator_id = go.id
            LEFT JOIN {got} got
                ON go.grid_operator_type_id = got.id
            LEFT JOIN {spvs} spvs
                ON plants.spv_id = spvs.id
            ORDER BY name ASC
        ''').format(
            plants=sql.Identifier('main_data_2'),
            parks=sql.Identifier('parks'),
            plant_j=sql.Identifier('janitzas'),
            poi_j=sql.Identifier('janitzas'),
            portfolios=sql.Identifier('portfolios'),
            pois=sql.Identifier('pois'),
            st=sql.Identifier('station_types'),
            go=sql.Identifier('grid_operators'),
            got=sql.Identifier('grid_operator_types'),
            spvs=sql.Identifier('spvs_current')
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)

                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        dt_cols: list[str] = ['commissioning_date', 'feed_in_date']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        df = df.set_index('id')

        float_cols = [
            'nominal_power', 'rated_power',
            'latitude', 'longitude',
            'elevation', 'slope', 'azimuth'
        ]

        df[float_cols] = df[float_cols].astype('float64')

        return df

    async def _get_ble_energy(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            WITH params AS (
                SELECT
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin') AS start_ts,
                    date_trunc('year', now() AT TIME ZONE 'Europe/Berlin') + INTERVAL '1 year' AS end_ts,
                    INTERVAL '1 day' AS step_interval
            ),
            intervals AS (
                SELECT
                    generate_series(
                        start_ts,
                        end_ts - step_interval,
                        step_interval
                    ) AT TIME ZONE 'Europe/Berlin' AS start_ts,
                    step_interval
                FROM params
            ),
            plants AS (
                SELECT
                    md2.id AS main_data_2_id,
                    md2.report_id,
                    md2.name,
                    md2.park_id,
                    md2.poi_id,
                    md2.see_number
                FROM {md2} md2
            )
            SELECT
                p.report_id,
                p.name,
                parks.name AS park,
                p.see_number,
                j.name AS janitza_name,
                i.start_ts AS start_ts,
                i.start_ts + i.step_interval AS end_ts,
                pe.energy_active_supplied AS energy
            FROM plants p
            CROSS JOIN intervals i
            LEFT JOIN {pe} pe
                ON pe.main_data_2_id = p.main_data_2_id
                AND pe.start_ts = i.start_ts
            LEFT JOIN {parks}
                ON p.park_id = parks.id
            LEFT JOIN {pois}
                ON p.poi_id = pois.id
            LEFT JOIN {j} j
                ON pois.janitza_id = j.id
            ORDER BY p.main_data_2_id, i.start_ts
        ''').format(
            md2=sql.Identifier('main_data_2'),
            parks=sql.Identifier('parks'),
            pois=sql.Identifier('pois'),
            j=sql.Identifier('janitzas'),
            pe=sql.Identifier('plant_energy_day')
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)

                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)
            
            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        df['start_ts'] = pd.to_datetime(df['start_ts'])
        df['end_ts'] = pd.to_datetime(df['end_ts'])
        df['energy'] = df['energy'].astype('float64')

        df['energy'] = df['energy'] * (-1)

        return df
