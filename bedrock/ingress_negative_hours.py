"""
ingress_negative_hours.py
=========================

v2

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


class NegativeHours:
    TABLE_NAME: str = 'negative_hours_energy'

    def __init__(self):
        self._conn = self._get_db_connection()

    ################################################################
    # Public
    ################################################################

    async def update(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> dict[str, Any]:
        """
        Update the ``negative_hours_energy`` table with data from ``plant_energy``.

        Either ``interval`` or ``start_ts``/``end_ts`` range must be provided, not both.
        Optionally rebuilds the table before inserting new data.

        Idempotent. Safely inserts or updates source data into the destination.
        Can be called repeatedly without side effects.

        Runtime tests:

        - Full year around 12 minutes

        :param interval: Predefined time interval (e.g., 'last_hour', 'today').
        :type interval: str or None
        :param start_ts: Start of the timestamp range (timezone-aware).
        :type start_ts: dt.datetime or None
        :param end_ts: End of the timestamp range (timezone-aware).
        :type end_ts: dt.datetime or None

        :returns: Summary of operation
        :rtype: dict[str, Any]
        :keys:
            - inserts: Number of inserted rows.
            - updates: Number of updated rows.
            - time: Total execution time as pandas.Timestamp delta.
        """
        t = pd.Timestamp.now()

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError('Provide either interval or start_ts/end_ts range, not both')

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError('If interval is not provided, both start_ts and end_ts must be given')

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')

        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        insert_sql: sql.Composable = sql.SQL('''
            WITH data AS (
                WITH main_data AS (
                    SELECT
                        md2.id,
                        md2.see_number,
                        see.commissioning_date,
                        see.eeg_number,
                        split_part(eeg.auction_award_number, '/', 1) AS bidding_round,
                        a.bid_submission_date
                    FROM main_data_2 md2
                    LEFT JOIN mastr_einheitensolar see
                        ON md2.see_number = see.see_number
                    LEFT JOIN mastr_anlageneegsolar eeg
                        ON see.eeg_number = eeg.eeg_number
                    LEFT JOIN auctions a
                        ON split_part(eeg.auction_award_number, '/', 1) = a.bidding_round
                    WHERE see.see_number IS NOT NULL
                ),
                quarter_hourly_prices AS (
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
                )
                SELECT
                    main_data_2_id,
                    start_ts,
                    end_ts,
                    CASE
                        WHEN mapped_eeg_rule = 0.25 AND day_ahead < 0 THEN energy_active_supplied
                        WHEN streak_length >= mapped_eeg_rule THEN energy_active_supplied
                        ELSE NULL
                    END AS neg_energy,
                    eeg_negative_hours,
                    mapped_eeg_rule
                FROM (
                    SELECT
                        pe.main_data_2_id,
                        pe.start_ts,
                        pe.end_ts,
                        pe.energy_active_supplied,
                        eeg.day_ahead,
                        eeg.eeg_negative_hours,
                        eeg.streak_length,
                        CASE
                            -- Skip if timestamp year < bidding round year
                            WHEN EXTRACT(YEAR FROM pe.start_ts) < EXTRACT(YEAR FROM COALESCE(md.bid_submission_date, md.commissioning_date)) THEN NULL
                                      
                            -- InnAusV
			                WHEN md.bidding_round LIKE 'Inn%%' THEN 1

                            -- EEG 2023 13. Änderung (Solarspitzengesetz)
                            WHEN COALESCE(md.bid_submission_date, md.commissioning_date) >= date '2025-02-25' THEN 0.25

                            -- EEG 2023
                            WHEN COALESCE(md.bid_submission_date, md.commissioning_date) >= date '2023-01-01' THEN
                                CASE EXTRACT(YEAR FROM pe.start_ts)
                                    WHEN 2023 THEN 4
                                    WHEN 2024 THEN 3
                                    WHEN 2025 THEN 3
                                    WHEN 2026 THEN 2
                                    ELSE 1
                                END

                            -- EEG 2021
                            WHEN COALESCE(md.bid_submission_date, md.commissioning_date) >= date '2021-01-01' THEN 4

                            -- EEG 2017 (from 2016-01-01)
                            WHEN COALESCE(md.bid_submission_date, md.commissioning_date) >= date '2017-01-01' and md.commissioning_date >= date '2016-01-01' THEN 6

                            -- EEG 2014 (from 2016-01-01)
                            WHEN COALESCE(md.bid_submission_date, md.commissioning_date) < date '2017-01-01' and md.commissioning_date >= date '2016-01-01' THEN 6

                            -- EEG 2017 & 2014 (before 2016-01-01)
                            ELSE NULL
                        END AS mapped_eeg_rule
                    FROM {pe} pe
                    JOIN main_data md
                        ON pe.main_data_2_id = md.id
                    LEFT JOIN quarter_hourly_prices eeg
                        ON pe.start_ts = eeg.start_ts
                    WHERE pe.start_ts >= {start_ts}
                    AND pe.start_ts < {end_ts}
                ) t
            )
            INSERT INTO {table} (
                main_data_2_id,
                start_ts,
                end_ts,
                energy_active_supplied
            )
            SELECT
                main_data_2_id,
                start_ts,
                end_ts,
                neg_energy AS energy_active_supplied
            FROM data
            WHERE start_ts >= {start_ts}
            AND start_ts < {end_ts}
            ON CONFLICT (main_data_2_id, start_ts) DO UPDATE
            SET
                end_ts = EXCLUDED.end_ts,
                energy_active_supplied = EXCLUDED.energy_active_supplied
            RETURNING CASE WHEN xmax = 0 THEN 'insert' ELSE 'update' END AS action;
        ''').format(
            table=sql.Identifier(self.TABLE_NAME),
            pe=sql.Identifier('plant_energy'),
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts')
        )

        def run_insert() -> list[tuple]:
            cur = self._conn.cursor()

            try:
                cur.execute(insert_sql, {'start_ts': start_ts, 'end_ts': end_ts})
            except psycopg2.Error as e:
                raise RuntimeError(f'Insert failed: {e.pgcode} {e.pgerror}')
            
            rows: list[tuple] = cur.fetchall()
            cur.close()
            self._conn.commit()

            return rows
        
        try:
            rows: list[tuple] = await asyncio.to_thread(run_insert)
        except Exception as e:
            raise e
        
        stats: dict[str, int] = {
            'inserts': sum(1 for r in rows if r[0] == 'insert'),
            'updates': sum(1 for r in rows if r[0] == 'update'),
            'time': pd.Timestamp.now() - t
        }

        return stats

    async def get(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> pd.DataFrame:
        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError('Provide either interval or start_ts/end_ts range, not both')

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError('If interval is not provided, both start_ts and end_ts must be given')

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')

        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        td: pd.Timedelta = end_ts - start_ts

        if (end_ts - start_ts) > pd.Timedelta(days=31):
            raise ValueError(
                f'Interval must not be greater than 7 days, but is {(end_ts - start_ts)}'
            )

        sql_query_plant_count: sql.Composable = sql.SQL('''
            SELECT
                COUNT(id)
            FROM {plants} plants
        ''').format(
            plants=sql.Identifier('main_data_2')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query_plant_count)
            plant_count: int = cur.fetchone()[0]

        estimated_records: int = (td.total_seconds() // 900) * plant_count
        
        if estimated_records > 2_000_000:
            raise ValueError(
                f'Record Count must not be greater than 1M, but is {estimated_records}'
            )

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                pe.main_data_2_id AS plant_id,
                pe.start_ts,
                pe.end_ts,
                pe.energy_active_supplied,
                nhe.energy_active_supplied AS energy_active_supplied_neg_null
            FROM {pe} pe
            LEFT JOIN {nhe} nhe
                ON pe.main_data_2_id = nhe.main_data_2_id
                AND pe.start_ts = nhe.start_ts
            WHERE pe.start_ts >= {start_ts}
            AND pe.start_ts < {end_ts}
            ORDER BY pe.main_data_2_id, pe.start_ts
        ''').format(
            nhe=sql.Identifier(self.TABLE_NAME),
            pe=sql.Identifier('plant_energy'),
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

        float_cols: list[str] = [
            'energy_active_supplied',
            'energy_active_supplied_neg_null'
        ]

        df[float_cols] = df[float_cols].astype('float64')

        df['plant_id'] = df['plant_id'].astype(int)

        start_idx: pd.DatetimeIndex = pd.date_range(
            start=start_ts,
            end=end_ts,
            freq='15min',
            inclusive='left',
            name='start_ts'
        )

        idx: pd.MultiIndex = pd.MultiIndex.from_product([
            pd.Index(df['plant_id'].unique(), name='plant_id'),
            start_idx
        ])

        df = df.set_index(['plant_id', 'start_ts'])
        df = df.reindex(idx)

        df = df.reset_index()
        df['end_ts'] = df['start_ts'] + pd.DateOffset(minutes=15)
        df = df.set_index(['plant_id', 'start_ts', 'end_ts'])

        return df

    async def clear(self) -> None:
        """
        Drop and recreate the ``negative_hours_energy`` table with its indexes.

        Steps performed:
        - Drop the table if it exists
        - Create the table with the schema:
            - id: primary key, auto-increment
            - main_data_2_id: int, foreign key
            - start_ts: non-null tz-aware datetime
            - end_ts: non-null tz-aware datetime
            - energy_active_supplied: numeric
        - Create an index on bidding_round
        - Commit changes

        :raises Exception: If any SQL execution fails
        """
        sql_drop: sql.Composable = sql.SQL('''
            DROP TABLE IF EXISTS {table};
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        sql_create: sql.Composable = sql.SQL('''
            CREATE TABLE {table} (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                main_data_2_id INT NOT NULL REFERENCES main_data_2(id),
                start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL,
                energy_active_supplied NUMERIC(18,3),
                UNIQUE (main_data_2_id, start_ts)
            );
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        sql_index_start_ts: sql.SQL = sql.SQL('''
            CREATE INDEX IF NOT EXISTS {idx} ON {tbl} ({col});
        ''').format(
            idx=sql.Identifier(f'{self.TABLE_NAME}_start_ts_idx'),
            tbl=sql.Identifier(self.TABLE_NAME),
            col=sql.Identifier('start_ts')
        )

        sql_index_main_data_2_id: sql.SQL = sql.SQL('''
            CREATE INDEX IF NOT EXISTS {idx} ON {tbl} ({col});
        ''').format(
            idx=sql.Identifier(f'{self.TABLE_NAME}_main_data_2_id_idx'),
            tbl=sql.Identifier(self.TABLE_NAME),
            col=sql.Identifier('main_data_2_id')
        )

        def run_query():
            cur = self._conn.cursor()

            try:
                cur.execute(sql_drop)
                cur.execute(sql_create)
                cur.execute(sql_index_start_ts)
                cur.execute(sql_index_main_data_2_id)
            except Exception as e:
                cur.close()
                self._conn.rollback()
                raise e
            
            cur.close()
            self._conn.commit()

        try:
            await asyncio.to_thread(run_query)
        except Exception as e:
            raise e

    async def get_negative_hours_rules(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> pd.DataFrame:
        """
        Retrieve the mapped negative hours rules per plant and year.

        You can provide either a predefined interval string or a start/end timestamp range.

        :param interval: Optional interval identifier. If provided, `start_ts` and `end_ts` must not be set.
        :type interval: str, optional
        :param start_ts: Start timestamp of the range (timezone-aware). Required if `interval` is None.
        :type start_ts: datetime.datetime, optional
        :param end_ts: End timestamp of the range (timezone-aware). Required if `interval` is None.
        :type end_ts: datetime.datetime, optional

        :raises ValueError: If both interval and start/end timestamps are provided,
                            or if one of start_ts/end_ts is missing,
                            or if start_ts/end_ts are not timezone-aware.

        :return: DataFrame indexed by `see_number` and `year` containing the relevant dates and mapped EEG/InnAusV rule.
        :rtype: pandas.DataFrame

        **Notes:**
        - The returned DataFrame includes the following columns:
        - `see_number`
        - `year`
        - `start_ts`, `end_ts`
        - `commissioning_date`
        - `bid_submission_date`
        - `relevant_date`
        - `mapped_eeg_rule`
        - Rules are applied according to EEG and InnAusV regulations, using the plant's relevant date.
        - Years before the relevant date are skipped (mapped as NULL).
        """
        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError('Provide either interval or start_ts/end_ts range, not both')

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError('If interval is not provided, both start_ts and end_ts must be given')

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')

        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        sql_query: sql.Composable = sql.SQL('''
            WITH data AS (
                SELECT
                    md2.see_number,
                    a.bidding_round,
                    see.commissioning_date,
                    a.bid_submission_date,
                    COALESCE(a.bid_submission_date, see.commissioning_date) AS relevant_date
                FROM main_data_2 md2
                LEFT JOIN mastr_einheitensolar see
                    ON md2.see_number = see.see_number
                LEFT JOIN mastr_anlageneegsolar eeg
                    ON see.eeg_number = eeg.eeg_number
                LEFT JOIN auctions a
                    ON split_part(eeg.auction_award_number, '/', 1) = a.bidding_round
                WHERE md2.see_number LIKE 'SEE%%'
            )
            SELECT
                d.see_number,
                EXTRACT(YEAR FROM start_ts AT TIME ZONE 'Europe/Berlin')::int AS year,
                start_ts,
                start_ts + '1 year' AS end_ts,
                d.commissioning_date,
                d.bid_submission_date,
                d.relevant_date,
                CASE
                    -- Skip if timestamp year < relevant date
                    WHEN EXTRACT(YEAR FROM start_ts AT TIME ZONE 'Europe/Berlin') < EXTRACT(YEAR FROM d.relevant_date) THEN NULL
                            
                    -- InnAusV
                    WHEN bidding_round LIKE 'Inn%%' THEN 1
                
                    -- EEG 2023 13. Änderung (Solarspitzengesetz)
                    WHEN d.relevant_date >= date '2025-02-25' THEN 0.25
                
                    -- EEG 2023
                    WHEN d.relevant_date >= date '2023-01-01' THEN
                        CASE EXTRACT(YEAR FROM start_ts AT TIME ZONE 'Europe/Berlin')
                            WHEN 2023 THEN 4
                            WHEN 2024 THEN 3
                            WHEN 2025 THEN 3
                            WHEN 2026 THEN 2
                            ELSE 1
                        END
                
                    -- EEG 2021
                    WHEN d.relevant_date >= date '2021-01-01' THEN 4
                
                    -- EEG 2017 (from 2016-01-01)
                    WHEN d.relevant_date >= date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN 6
                
                    -- EEG 2014 (from 2016-01-01)
                    WHEN d.relevant_date < date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN 6
                
                    -- EEG 2017 & 2014 (before 2016-01-01)
                    ELSE NULL
                END AS mapped_eeg_rule,
                CASE
                    -- Skip if timestamp year < relevant date
                    WHEN EXTRACT(YEAR FROM start_ts AT TIME ZONE 'Europe/Berlin') < EXTRACT(YEAR FROM d.relevant_date) THEN NULL
                            
                    -- InnAusV
                    WHEN bidding_round LIKE 'Inn%%' THEN 'InnAusV'
                
                    -- EEG 2023 13. Änderung (Solarspitzengesetz)
                    WHEN d.relevant_date >= date '2025-02-25' THEN 'EEG 2023'
                
                    -- EEG 2023
                    WHEN d.relevant_date >= date '2023-01-01' THEN 'EEG 2023'
                
                    -- EEG 2021
                    WHEN d.relevant_date >= date '2021-01-01' THEN 'EEG 2021'
                
                    -- EEG 2017 (from 2016-01-01)
                    WHEN d.relevant_date >= date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN 'EEG 2017'
                
                    -- EEG 2014 (from 2016-01-01)
                    WHEN d.relevant_date < date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN 'EEG 2014'
                
                    -- EEG 2017 & 2014 (before 2016-01-01)
                    ELSE NULL
                END AS eeg_version,
                CASE
                    -- Skip if timestamp year < relevant date
                    WHEN EXTRACT(YEAR FROM start_ts AT TIME ZONE 'Europe/Berlin') < EXTRACT(YEAR FROM d.relevant_date) THEN NULL
                            
                    -- InnAusV
                    WHEN bidding_round LIKE 'Inn%%' THEN 'InnAusV'
                
                    -- EEG 2023 13. Änderung (Solarspitzengesetz)
                    WHEN d.relevant_date >= date '2025-02-25' THEN '13th Amendment (Solarspitzengesetz)'
                
                    -- EEG 2023
                    WHEN d.relevant_date >= date '2023-01-01' THEN '12th Amendment'
                
                    -- EEG 2021
                    WHEN d.relevant_date >= date '2021-01-01' THEN NULL
                
                    -- EEG 2017 (from 2016-01-01)
                    WHEN d.relevant_date >= date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN NULL
                
                    -- EEG 2014 (from 2016-01-01)
                    WHEN d.relevant_date < date '2017-01-01' and d.commissioning_date >= date '2016-01-01' THEN NULL
                
                    -- EEG 2017 & 2014 (before 2016-01-01)
                    ELSE NULL
                END AS eeg_amendment
            FROM data d
            CROSS JOIN generate_series({start_ts}, {end_ts} - INTERVAL '1 year', '1 year') start_ts
            ORDER BY d.see_number, start_ts;
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
        
        df = await asyncio.to_thread(run_query)

        df = df.set_index(['see_number', 'year'])

        return df

    ################################################################
    # Internal methods - update
    ################################################################

    def _validate_df(self, df: pd.DataFrame) -> None:
        """
        Validate a DataFrame before inserting into the ``negative_hours`` table.

        Checks performed:

        - Required columns exist: ``see_number``, ``commissioning_date``,
          ``bidding_round``, ``bid_submission_date``
        - No nulls in non-nullable columns
        - Correct dtypes:
            - ``see_number``: str
            - ``commissioning_date``: datetime
            - ``bidding_round``: str
            - ``bid_submission_date``: datetime
        - ``see_number`` is unique

        :param df: DataFrame to validate
        :type df: pd.DataFrame
        :raises ValueError: If any validation fails
        """
        required_cols: list[str] = [
            'see_number', 'commissioning_date',
            'bidding_round', 'bid_submission_date'
        ]

        non_nullable_columns: list[str] = [
            'see_number', 'commissioning_date'
        ]

        # 1. All required columns present
        missing: list[str] = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f'Missing required columns: {missing}')

        # 2. Non-nullable columns
        if df[non_nullable_columns].isnull().any().any():
            raise ValueError('Null values found in required columns')

        # 3. Correct types
        if not pd.api.types.is_datetime64_dtype(df['commissioning_date']):
            raise ValueError(f'commissioning_date must be a datetime column')
        
        if not pd.api.types.is_datetime64_dtype(df['bid_submission_date']):
            raise ValueError(f'bid_submission_date must be a datetime column')
        
        # 4. Unique columns
        if not df['see_number'].is_unique:
            raise ValueError('Duplicate values found in see_number')

    ################################################################
    # Internal methods - utility
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

    ################################################################
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
