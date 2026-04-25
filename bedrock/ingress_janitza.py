"""
ingress_janitza.py
==================

v3

"""

# Standard library
import asyncio
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import json
from typing import Any
import re

# Third-party
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

# Local packages
from omr_gridvis import GridVisRESTAPI, PROJECTS, VALUES, TYPES, TIMEBASES
from settings import settings


class Endpoint:
    def __init__(self, p_value: VALUES, p_type: TYPES, p_timebase: TIMEBASES):
        self._p_value: VALUES = p_value
        self._p_type: TYPES = p_type
        self._p_timebase: TIMEBASES = p_timebase

    @property
    def p_value(self) -> VALUES:
        return self._p_value
    
    @property
    def p_type(self) -> TYPES:
        return self._p_type
    
    @property
    def p_timebase(self) -> TIMEBASES:
        return self._p_timebase
    
    @property
    def name(self) -> str:
        return f'{self._p_value.name}.{self._p_type.name}.{self._p_timebase.name}'
    
    def __str__(self) -> str:
        return f'Endpoint<{self._p_value.name}.{self._p_type.name}.{self._p_timebase.name}>'
    
    def to_dict(self) -> dict[str, Any]:
        return {
            'p_value': self._p_value.name,
            'p_type': self._p_type.name,
            'p_timebase': self._p_timebase.name
        }
    
    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> 'Endpoint':
        return cls(
            p_value=VALUES[d['p_value']],
            p_type=TYPES[d['p_type']],
            p_timebase=TIMEBASES[d['p_timebase']]
        )


class GridVisRequest:
    def __init__(
            self,
            janitza_id: int,
            gridvis_id: int,
            endpoint: Endpoint,
            start_ts: dt.datetime,
            end_ts: dt.datetime
        ):
        self._janitza_id = janitza_id
        self._gridvis_id = gridvis_id
        self._endpoint = endpoint
        self._start_ts = start_ts
        self._end_ts = end_ts

    @property
    def janitza_id(self) -> int:
        return self._janitza_id

    @property
    def gridvis_id(self) -> int:
        return self._gridvis_id
    
    @property
    def endpoint(self) -> Endpoint:
        return self._endpoint
    
    @property
    def start_ts(self) -> dt.datetime:
        return self._start_ts
    
    @property
    def end_ts(self) -> dt.datetime:
        return self._end_ts
    
    def __str__(self) -> str:
        return f'GridVisRequest<{self._gridvis_id}, {self._endpoint}>'
    
    def __repr__(self) -> str:
        return f'GridVisRequest<{self._gridvis_id}, {self._endpoint}>'


class Janitza:
    """
    The ``Janitza`` class handles all processes related to the ingress pipeline for
    retrieving Janitza data from the GridVis REST API and loading the data
    into the database.
    """

    def __init__(self):
        self._conn = self._get_db_connection()
        self.gridvis = GridVisRESTAPI()
        self.gridvis.set_project(PROJECTS.EP_OM_GridVis)
        self.gridvis.allow_self_signed_certificate(True)

    ################################################################
    # Public - Janitza measurement data
    ################################################################

    async def start_ingress(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> dict[str, Any]:
        """
        Start the Janitza ingress job by generating and processing requests.

        Either an interval or a datetime range must be provided. Allowed
        intervals are 'yesterday' and 'today'. If using a datetime range,
        both start_ts and end_ts must be provided and must be timezone-aware.

        Idempotent. Safely inserts or updates source data into the destination.
        Can be called repeatedly without side effects.

        Runtime tests:

        - Full year around 1.5 hrs

        :param interval: Predefined time window ('yesterday' or 'today').
        :type interval: str or None
        :param start_ts: Start of custom datetime range (timezone-aware).
        :type start_ts: dt.datetime or None
        :param end_ts: End of custom datetime range (timezone-aware).
        :type end_ts: dt.datetime or None

        :returns: Summary of operation
        :rtype: dict[str, Any]
        :keys:
            - time: Total execution time as pandas.Timestamp delta.
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

        intervals: list[tuple[pd.Timestamp, pd.Timestamp]] = self._split_into_months(
            start_ts=start_ts,
            end_ts=end_ts
        )

        intervals = [(start_ts, end_ts)]

        all_stats: list[dict] = []

        t = pd.Timestamp.now()

        for i in intervals:
            _stats: dict[str, Any] = await self._ingress(
                start_ts=i[0],
                end_ts=i[1]
            )

            all_stats.append(_stats)

        stats_df: pd.DataFrame = pd.DataFrame(all_stats)
        stats_df = stats_df.sum(numeric_only=False)

        stats: dict[str, Any] = stats_df.to_dict()

        stats['ingress_time'] = pd.Timestamp.now() - t

        return stats

    ################################################################
    # Public - Janitza devices
    ################################################################

    async def start_ingress_devices(self) -> dict[str, int]:
        """
        Update the devices by retrieving them from the GridVis REST API
        and writing them to the database.

        Idempotent. Safely inserts or updates source data into the destination.
        Can be called repeatedly without side effects.
        """
        gridvis_devices: pd.DataFrame = self._get_devices_from_gridvis()

        gridvis_devices = gridvis_devices[
            [
                'janitza_name', 'description', 'type_display_name',
                'serial_number', 'connection_string'
            ]
        ]

        gridvis_devices = gridvis_devices.rename(columns={
            'janitza_id': 'gridvis_id',
            'janitza_name': 'name',
            'type_display_name': 'type_name'
        })

        # def extract_ip(s: str) -> str | None:
        #     match = re.search(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', s)
        #     return match.group(0) if match else None

        def extract_ip_port(s: str) -> str | None:
            match = re.search(r'\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b', s)
            return match.group(0) if match else None
        
        gridvis_devices['ip_address'] = gridvis_devices['connection_string'].apply(
            extract_ip_port
        )

        gridvis_devices['type_name'] = gridvis_devices['type_name'].apply(
            lambda x: x.strip()
        )

        db_devices_df: pd.DataFrame = self._get_devices_from_db()
        db_devices_df = db_devices_df.set_index('gridvis_id')

        gridvis_devices_df: pd.DataFrame = gridvis_devices.reset_index().copy()
        gridvis_devices_df = gridvis_devices_df.rename(columns={
            'janitza_id': 'gridvis_id'
        })
        gridvis_devices_df = gridvis_devices_df.set_index('gridvis_id')

        response: dict[str, int] = await asyncio.to_thread(self._write_devices_to_db, gridvis_devices)

        return response

    def get_devices_df(self) -> pd.DataFrame:
        return self._get_devices_from_db()

    ################################################################
    # Public - db accessors
    ################################################################

    def get_janitza_measurement_id(
            self,
            p_value: VALUES,
            p_type: TYPES,
            p_timebase: TIMEBASES
        ) -> int:
        """
        Retrieve the associated ID for a given Janitza measurement.

        :param p_value: Measurement value identifier.
        :type p_value: VALUES
        :param p_type: Measurement type identifier.
        :type p_type: TYPES
        :param p_timebase: Measurement timebase identifier.
        :type p_timebase: TIMEBASES
        :raises ValueError: If no matching measurement is found.
        :return: The ID of the matching Janitza measurement.
        :rtype: int
        """

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id
            FROM {table}
            WHERE  value = {value}
            AND     type = {type}
            AND timebase = {timebase}
        ''').format(
            table=sql.Identifier('janitza_measurements'),
            value=sql.Placeholder('value'),
            type=sql.Placeholder('type'),
            timebase=sql.Placeholder('timebase')
        )

        values: dict[str, str] = {
            'value': p_value.value,
            'type': p_type.value,
            'timebase': p_timebase.value
        }

        with self._conn.cursor() as cur:
            cur.execute(sql_query, values)
            row = cur.fetchone()

        if row:
            return row[0]
        
        raise ValueError(f'Measurement not found: {p_value}, {p_type}, {p_timebase}')

    def get_aggregation_id(self, aggregation: str) -> int:
        """
        Retrieve the ID of a given aggregation.

        :param aggregation: Name of the aggregation.
        :type aggregation: str
        :raises ValueError: If no matching aggregation is found.
        :return: The ID of the aggregation.
        :rtype: int
        """

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id
            FROM {table}
            WHERE name = {aggregation}
        ''').format(
            table=sql.Identifier('aggregations'),
            aggregation=sql.Placeholder('aggregation')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query, {'aggregation': aggregation})

            row = cur.fetchone()

        if row:
            return row[0]
        
        raise ValueError(f'Aggregation not found: {aggregation}')

    def get_measurement_from_id(self, measurement_id: int) -> pd.Series:
        """
        Retrieve a Janitza measurement by its ID.

        :param measurement_id: The ID of the measurement to fetch.
        :type measurement_id: int
        :return: A pandas Series containing the measurement data, indexed by column names.
        :rtype: pd.Series
        """

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                *
            FROM janitza_measurements
            WHERE id = {measurement_id};
        ''').format(
            table=sql.Identifier('janitza_measurements'),
            measurement_id=sql.Placeholder('measurement_id')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query, {'measurement_id': measurement_id})

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        df = df.sort_index()

        return df.iloc[0]

    def get_measurement_id(
            self,
            measurement_value: VALUES,
            measurement_type: TYPES,
            measurement_timebase: TIMEBASES
        ) -> int:
        """
        Retrieve the ID of a Janitza measurement based on value, type, and timebase.

        :param measurement_value: Measurement value identifier.
        :type measurement_value: VALUES
        :param measurement_type: Measurement type identifier.
        :type measurement_type: TYPES
        :param measurement_timebase: Measurement timebase identifier.
        :type measurement_timebase: TIMEBASES
        :return: The ID of the matching Janitza measurement.
        :rtype: int
        """

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id
            FROM {table}
            WHERE  value = {value}
            AND     type = {type}
            AND timebase = {timebase}
        ''').format(
            table=sql.Identifier('janitza_measurements'),
            value=sql.Placeholder('value'),
            type=sql.Placeholder('type'),
            timebase=sql.Placeholder('timebase')
        )

        print(f'Getting janitza_measurement_id for {measurement_value.value} {measurement_type.value} {measurement_timebase.value}')

        values: dict[str, str] = {
            'value': measurement_value.value,
            'type': measurement_type.value,
            'timebase': measurement_timebase.value
        }

        with self._conn.cursor() as cur:
            cur.execute(sql_query, values)
            measurement_id: int = cur.fetchone()[0]

        return measurement_id

    async def get_janitza_id(self, janitza_name: str | list[str]) -> int | list[int]:
        """
        Retrieve the ID(s) of Janitza entries by name.

        :param janitza_name: A single Janitza name as a string or a list of names.
        :type janitza_name: str or list of str
        :return: The ID corresponding to the name if a single name is provided
                or a list of IDs if multiple names are provided.
        :rtype: int or list of int
        :raises ValueError: If an empty list is provided.
        """
        if isinstance(janitza_name, list) and len(janitza_name) == 0:
            raise ValueError(f'janitza_name must contain at least one item.')
        
        janitza_names: list[str] = []

        if isinstance(janitza_name, str):
            janitza_names = [janitza_name]
        else:
            janitza_names = janitza_name

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id
            FROM {table}
            WHERE name IN {names}
        ''').format(
            table=sql.Identifier('janitzas'),
            names=sql.Identifier(f'({(', '.join(['%s'] * len(janitza_names)))})')
        )

        def run_query() -> list[int]:
            with self._conn.cursor() as cur:
                cur = self._conn.cursor()
                cur.execute(sql_query, janitza_names)
                janitza_ids: list[int] = [row[0] for row in cur.fetchall()]
            
            return janitza_ids
        
        janitza_ids: list[int] = await asyncio.to_thread(run_query)

        if len(janitza_ids) == 1:
            return janitza_ids[0]
        
        return janitza_ids

    ################################################################
    # Interal methods - Janitza measurement data
    ################################################################

    async def _ingress(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime
        ) -> dict[str, Any]:
        """
        
        """

        print(f'Ingress data for {start_ts} - {end_ts}')
        
        t = pd.Timestamp.now()

        # Retrieve devices from DB to only request
        # data for already existing devices
        devices_df: pd.DataFrame = self.get_devices_df()

        requests_per_janitza: list[list[GridVisRequest]] = devices_df.reset_index().apply(
            self._generate_requests,
            axis=1,
            start_ts=start_ts,
            end_ts=end_ts
        ).tolist()

        all_requests: list[GridVisRequest] = [
            req for sublist in requests_per_janitza for req in sublist
        ]

        await self._process_worker(all_requests)

        stats: dict[str, Any] = {
            'total_time': pd.Timestamp.now() - t
        }

        return stats

    async def _process_worker(self, reqs: list[GridVisRequest]) -> None:
        """
        Process multiple GridVis requests concurrently and write results to the database.

        :param reqs: List of ``GridVisRequest`` objects to be executed.
        :type reqs: list[GridVisRequest]
        :returns: None
        :rtype: None
        """

        # tasks = [asyncio.to_thread(self._run_request, req) for req in reqs]

        # for fut in asyncio.as_completed(tasks):
        #     df, req = await fut
        #     if df is not None:
        #         self._process_data(df, req)
        
        executor = ThreadPoolExecutor(max_workers=20)  # tune this

        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(executor, self._run_request, req)
            for req in reqs
        ]

        for fut in asyncio.as_completed(tasks):
            df, req = await fut
            if df is not None:
                self._process_data(df, req)

    def _process_data(self, df: pd.DataFrame, request: GridVisRequest) -> None:
        """
        Prepare GridVis data and persist it to the database.

        This method slices the DataFrame to the requested time interval,
        filters it to keep only the ``'avg'`` aggregation, reshapes it to a single
        ``'value'`` column, resolves metadata IDs (aggregation, measurement, device),
        and delegates the insert to :meth:`_write_data_to_db`.

        :param df: DataFrame with raw GridVis data, indexed by timestamp.
        :type df: pandas.DataFrame
        :param request: GridVisRequest containing metadata and time range.
        :type request: GridVisRequest
        :returns: None
        :rtype: None
        """

        aggregation: str = 'avg'

        start_ts: dt.datetime = request.start_ts
        end_ts: dt.datetime = request.end_ts

        # print(f'{start_ts} - {end_ts}')
        # print(f'{df.index.min()} - {df.index.max()}')

        # Slice df to make sure that only the requested interval is included
        df = df[(df.index >= start_ts) & (df.index < end_ts)]

        df = df.reset_index()
        df = df.rename(columns={'end': 'ts'})
        df = df.set_index('ts')

        # Select only the 'avg' column and discard 'min' and 'max'
        df = df[[c for c in df.columns if aggregation in c]]

        resolution: int = int(request.endpoint.p_timebase.value)

        aggregation_id: int = self.get_aggregation_id('avg')

        janitza_measurement_id: int = self.get_janitza_measurement_id(
            p_value=request.endpoint.p_value,
            p_type=request.endpoint.p_type,
            p_timebase=request.endpoint.p_timebase
        )

        janitza_id: int = request.janitza_id

        first_col = df.columns[0]

        df = df.rename(columns={first_col: 'value'})

        self._write_data_to_db(
            df=df,
            resolution=resolution,
            aggregation_id=aggregation_id,
            janitza_measurement_id=janitza_measurement_id,
            janitza_id=janitza_id
        )

    def _run_request(
            self,
            request: GridVisRequest
        ) -> tuple[pd.DataFrame | None, GridVisRequest]:

        start_ts: dt.datetime = request.start_ts
        end_ts: dt.datetime = request.end_ts

        # Hotfix to fix gridvis shift in package
        fix_list = [
            VALUES.ActiveEnergy, VALUES.ActiveEnergySupplied, VALUES.ActiveEnergyConsumed
        ]
        
        if request.endpoint.p_value in fix_list:
            start_ts = request.start_ts + pd.offsets.DateOffset(seconds=int(request.endpoint.p_timebase.value))
            end_ts = request.end_ts + pd.offsets.DateOffset(seconds=int(request.endpoint.p_timebase.value))

        try:
            df: pd.DataFrame = self.gridvis.hist_values(
                p_id=request.gridvis_id,
                p_value=request.endpoint.p_value,
                p_type=request.endpoint.p_type,
                p_timebase=request.endpoint.p_timebase,
                start=start_ts,
                end=end_ts
            )
        except RuntimeError as _:
            return None, request
        
        endpoint: Endpoint = request.endpoint
        interval_in_seconds: int = int(endpoint.p_timebase.value)
        
        df = self.gridvis.filter_full_intervals(
            df=df,
            interval=pd.Timedelta(seconds=interval_in_seconds)
        )
        
        return df, request

    def _generate_requests(
            self,
            row: pd.Series,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> list[GridVisRequest]:
        janitza_id: int = row['id']
        gridvis_id: int = row['gridvis_id']

        endpoints: list[Endpoint] = self._load_available_endpoints()

        stats_df: pd.DataFrame = self._generate_stats_df(endpoints)

        stats_df['request'] = stats_df.apply(lambda row: GridVisRequest(
            janitza_id=janitza_id,
            gridvis_id=gridvis_id,
            endpoint=row['endpoint'],
            start_ts=start_ts,
            end_ts=end_ts
        ), axis=1)

        return stats_df['request'].tolist()

    def _load_available_endpoints(self) -> list[Endpoint]:
        """
        Load a predefined list of endpoints from the JSON file.

        :return: a list of endpoints
        :rtype: list[Endpoint]
        """
        with open('./res/endpoints.json') as f:
            endpoints = [Endpoint.from_dict(d) for d in json.load(f)]
        
        return endpoints

    def _generate_stats_df(self, endpoints: list[Endpoint]) -> pd.DataFrame:
        df: pd.DataFrame = pd.DataFrame()

        df['name'] = [e.name for e in endpoints]
        df['endpoint'] = [e for e in endpoints]

        df = df.set_index('name')

        df['available'] = pd.NA

        return df

    def _write_data_to_db(
            self,
            df: pd.DataFrame,
            resolution: int,
            aggregation_id: int,
            janitza_measurement_id: int,
            janitza_id: int
        ) -> None:
        # # Resolve duplicate records
        # # Ex.: GridVis ID 85 - RON12A 2025-01-13 - Active Energy - SUM13
        # # 2025-01-13 13:00:00+00:00 - 2025-01-13 14:00:00+00:00
        # # In this interval the Janitza has a record at 13:35:04+00:00
        # # with the timestamp being rounded to 14:00:00+00:00 in the omr_gridvis package
        # # The "last" record is being kept until the package has been updated.
        # df = df[~df.index.duplicated(keep='last')]

        # Resolves duplicate records
        # Ex.: GridVis ID 405 - SOE41A 2025-05-07 - Power Active - SUM13
        # 2025-05-07 20:15:00+00:00 - 2025-05-07 20:30:00+00:00
        # In this interval the Janitza has two records with millisecond offset only.
        # The first interval still has a value, the second (and following) is zero.
        # Note: GridVis does not show the interval with the 0 value,
        # but the RESTAPI does return both intervals.
        if not df.index.is_unique:
            # def pick_nonzero(g):
            #     nz = g[g['value'] != 0]
            #     return nz.iloc[0] if not nz.empty else g.iloc[0]

            # df = df.groupby(level=0, group_keys=False).apply(pick_nonzero)

            filename: str = f'df_{janitza_id}_{janitza_measurement_id}.csv'
            df.to_csv(filename)

            df = df.sort_values('value')                # puts 0 before non-zero
            df = df[~df.index.duplicated(keep='last')]  # keeps the non-zero

        batch_size: int = 5_000

        sql_query: sql.Composable = sql.SQL('''
            INSERT INTO {janitza_measurement_data}
                (ts, resolution, aggregation_id, janitza_measurement_id, janitza_id, value)
            VALUES %s
            ON CONFLICT (janitza_id, janitza_measurement_id, ts) DO UPDATE
            SET
                resolution = EXCLUDED.resolution,
                aggregation_id = EXCLUDED.aggregation_id,
                value = EXCLUDED.value
            WHERE janitza_measurement_data.resolution IS DISTINCT FROM EXCLUDED.resolution
            OR janitza_measurement_data.aggregation_id IS DISTINCT FROM EXCLUDED.aggregation_id
            OR janitza_measurement_data.value IS DISTINCT FROM EXCLUDED.value
        ''').format(
            janitza_measurement_data=sql.Identifier('janitza_measurement_data')
        )

        records: list[tuple[dt.datetime, int, int, int, int, float]] = [
            (ts, resolution, aggregation_id, janitza_measurement_id, janitza_id, val)
            for ts, val in zip(df.index, df['value'])
        ]

        with self._conn.cursor() as cur:
            try:
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    execute_values(cur, sql_query, batch)
            except Exception as e:
                filename: str = f'df_{janitza_id}_{janitza_measurement_id}.csv'
                df.to_csv(filename)
                self._conn.rollback()
                raise RuntimeError(f'Error for Janitza "{janitza_id}"\n{e}')
        
        self._conn.commit()

    def _get_pivot_sql(self, janitza_measurement_ids: list[int]) -> sql.Composable:
        """
        .. deprecated:: 3.0
            This function is deprecated and will be removed in a future release.
            It has been replaced by a new internal logic and has no direct equivalent.
        """
        col_names: list[tuple[int, str]] = []

        for measurement_id in janitza_measurement_ids:
            row: pd.Series = self.get_measurement_from_id(measurement_id)
            col_name: str = f'{row['value']}_{row['type']}'
            col_names.append((measurement_id, col_name))

        filters: list[str] = [
            f'MAX(value) FILTER (WHERE janitza_measurement_id = {_id})  AS {_name}'
            for _id, _name
            in col_names
        ]

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                ts - (resolution * interval '1 second') AS start_ts,
                ts AS end_ts,
                janitza_id,
                {filters}
            FROM {janitza_measurement_data}
            WHERE janitza_measurement_id IN ({janitza_measurement_ids})
            AND ts > {start_ts}
            AND ts <=  {end_ts}
            GROUP BY ts, janitza_id, resolution
        ''').format(
            filters=sql.SQL((', '.join(filters))),
            janitza_measurement_data=sql.Identifier('janitza_measurement_data'),
            janitza_measurement_ids=sql.SQL(', '.join(str(i) for i in janitza_measurement_ids)),
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts')
        )
        
        return sql_query

    def _get_janitza_energy_table_sql(self) -> sql.Composable:
        """
        .. deprecated:: 3.0
            This function is deprecated and will be removed in a future release.
            It has been replaced by a new internal logic and has no direct equivalent.
        """
        power_active_id: int = self.get_measurement_id(
            VALUES.PowerActive,
            TYPES.SUM13,
            TIMEBASES.QUARTER_HOUR
        )

        power_reactive_fundamental_id: int = self.get_measurement_id(
            VALUES.PowerReactivefund,
            TYPES.SUM13,
            TIMEBASES.QUARTER_HOUR
        )

        janitza_measurement_ids: list[int] = [
            power_active_id,
            power_reactive_fundamental_id
        ]

        col_map: dict[int, str] = {
            measurement_id: f"{row['value']}_{row['type']}"
            for measurement_id in janitza_measurement_ids
            if (row := self.get_measurement_from_id(measurement_id)) is not None
        }

        pivot_sql: sql.Composable = self._get_pivot_sql(
            janitza_measurement_ids=janitza_measurement_ids
        )

        # This query utilizes the pivot and selects the power active
        # and power active fundamental series from the Janitza raw data for SUM13.
        # It splits both series into supplied and consumed.
        sql_query: sql.Composable = sql.SQL('''
            WITH data AS (
                {pivot_sql}
            )
            SELECT
                janitza_id,
                start_ts,
                end_ts,
                ROUND(LEAST({energy_active_supplied}, 0) * 0.25 / 1_000.0, 3) AS energy_active_supplied,
                ROUND(GREATEST({energy_active_consumed}, 0) * 0.25 / 1_000.0, 3) AS energy_active_consumed,
                ROUND(LEAST({energy_reactive_fundamental_supplied}, 0) * 0.25 / 1_000.0, 3) AS energy_reactive_fundamental_supplied,
                ROUND(GREATEST({energy_reactive_fundamental_consumed}, 0) * 0.25 / 1_000.0, 3) AS energy_reactive_fundamental_consumed
            FROM data
            ORDER BY janitza_id, start_ts
        ''').format(
            pivot_sql=pivot_sql,
            energy_active_supplied=sql.SQL(col_map[power_active_id]),
            energy_active_consumed=sql.SQL(col_map[power_active_id]),
            energy_reactive_fundamental_supplied=sql.SQL(col_map[power_reactive_fundamental_id]),
            energy_reactive_fundamental_consumed=sql.SQL(col_map[power_reactive_fundamental_id])
        )

        return sql_query

    ################################################################
    # Interal methods - Janitza devices
    ################################################################

    def _get_devices_from_gridvis(self) -> pd.DataFrame:
        df: pd.DataFrame = self.gridvis.get_devices()

        return df

    def _get_devices_from_db(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                *
            FROM {table}
        ''').format(
            table=sql.Identifier('janitzas')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        cur.close()

        df = df.sort_index()

        return df

    def _get_gridvis_id_from_janitza_id(self, janitza_id: int) -> int | None:
        try:
            cur = self._conn.cursor()
        except Exception as e:
            return None
        
        cur.execute('SELECT gridvis_id FROM janitzas WHERE id = %s', (janitza_id,))

        row = cur.fetchone()

        if row is None:
            return None
        
        gridvis_id: int = row[0]

        return gridvis_id

    def _write_devices_to_db(self, gridvis_devices: pd.DataFrame) -> dict[str, int]:
        gridvis_devices = gridvis_devices.reset_index()

        gridvis_devices = gridvis_devices.rename(columns={
            'janitza_id': 'gridvis_id',
            'janitza_name': 'name'
        })

        created: int = 0
        updated: int = 0

        sql_query_get: sql.Composable = sql.SQL('''
            SELECT
                id
            FROM {j} j
            WHERE gridvis_id = {gridvis_id}
        ''').format(
            j=sql.Identifier('janitzas'),
            gridvis_id=sql.Placeholder('gridvis_id')
        )

        sql_query_update: sql.Composable = sql.SQL('''
            UPDATE {janitzas}
            SET
                name = %s,
                description = %s,
                type_name = %s,
                serial_number = %s,
                connection_string = %s,
                ip_address = %s
            WHERE gridvis_id = %s
        ''').format(
            janitzas=sql.Identifier('janitzas')
        )

        sql_query_insert: sql.Composable = sql.SQL('''
            INSERT INTO {janitzas} (
                gridvis_id, name, description,
                commissioning_date,
                type_name, serial_number,
                connection_string, ip_address
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        ''').format(
            janitzas=sql.Identifier('janitzas')
        )

        def upsert_device(device) -> dict[str, int]:
            created: int = 0
            updated: int = 0

            payload_update: tuple[Any] = (
                device['name'], device['description'], device['type_name'],
                device['serial_number'], device['connection_string'],
                device['ip_address'], device['gridvis_id']
            )

            payload_insert: tuple[Any] = (
                device['gridvis_id'], device['name'], device['description'],
                pd.Timestamp.now().date(),
                device['type_name'], device['serial_number'],
                device['connection_string'], device['ip_address']
            )

            with self._conn.cursor() as cur:
                cur.execute(sql_query_get, {
                    'gridvis_id': device['gridvis_id']
                })

                if cur.fetchone():
                    cur.execute(sql_query_update, payload_update)
                    updated += 1
                else:
                    cur.execute(sql_query_insert, payload_insert)
                    created += 1
            
            self._conn.commit()

            return {
                'updated': updated,
                'created': created
            }
            
        for _, row in gridvis_devices.iterrows():
            try:
                stats: dict[str, int] = upsert_device(row)
                updated += stats['updated']
                created += stats['created']
            except Exception as e:
                self._conn.rollback()
                return {'error': f'{e}'}
        
        return {'created': created, 'updated': updated}

    ################################################################
    # Internal methods - utility
    ################################################################

    def _get_range_from_interval(
            self,
            interval: str | int,
            tz: str='Europe/Berlin'
        ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """
        
        """

        INTERVALS: list[str] = [
            'this_hour', 'last_hour', 'last_three_hours',
            'today', 'yesterday', 'last_three_days',
            'this_week', 'last_week', 'last_three_weeks',
            'this_month', 'last_month', 'last_three_months',
            'this_year', 'last_year', 'last_three_years'
        ]

        # Convert timestamp string year
        if re.fullmatch(r'\d{4}', interval):
            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(
                f'{interval}-01-01T00:00:00'
            )
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(years=1)

            return start_ts, end_ts
        
        # Convert timestamp int year
        if type(interval) == int:
            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(
                f'{interval}-01-01T00:00:00'
            )
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(years=1)

            return start_ts, end_ts
        
        # Convert timestamp string month
        if re.fullmatch(r'\d{4}-\d{2}', interval):
            year, month = map(int, interval.split('-'))

            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-01T00:00:00')
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(months=1)

            return start_ts, end_ts
        
        # Convert timestamp string day
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', interval):
            year, month, day = map(int, interval.split('-'))

            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-{day:02}T00:00:00')
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(days=1)

            return start_ts, end_ts
        
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

        raise ValueError(
            f"Interval must be one of {', '.join(INTERVALS)} "
            f"or a datetime string 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', "
            f"got {interval!r}"
        )

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

    def _split_into_months(
            self,
            start_ts: dt.datetime,
            end_ts: dt.datetime,
            tz: str='Europe/Berlin'
        ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Split a time range into individual month intervals.

        This utility method divides a given timezone-aware datetime range into a list of
        (start, end) tuples representing each month within the range. The timestamps are
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

        months: list[tuple[dt.datetime, dt.datetime]] = []

        cur: pd.Timestamp = start_ts

        while cur < end_ts:
            nxt: pd.Timestamp = min(
                (cur + pd.offsets.DateOffset(months=1)).normalize(),
                end_ts
            )
            
            if nxt <= cur:
                nxt = end_ts
            months.append((cur, nxt))
            cur = nxt

        return months

    ################################################################
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn


class JanitzaEnergy:
    TABLE_NAME: str = 'janitza_energy'

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

        days: list[tuple[pd.Timestamp, pd.Timestamp]] = self._split_into_days(
            start_ts=start_ts,
            end_ts=end_ts
        )

        all_stats: list[dict] = []

        for day in days:
            _stats: dict[str, Any] = await self._ingress(
                start_ts=day[0],
                end_ts=day[1]
            )

            all_stats.append(_stats)

        stats_df: pd.DataFrame = pd.DataFrame(all_stats)
        stats_df = stats_df.sum(numeric_only=False)

        stats: dict[str, Any] = stats_df.to_dict()

        self._update_latest_janitza_energy_mv()

        stats['update_time'] = pd.Timestamp.now() - t

        return stats

    def clear(self, confirm: bool=False) -> None:
        """
        Deletes the entire ``janitza_energy`` table.

        This effectively resets the entire janitza_energy layer and should be followed by
        a call to :meth:`update` to restore consistency.

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
        
        cur = self._conn.cursor()

        print(f'Drop table if exists "{self.TABLE_NAME}"...', end='\r')

        cur.execute(f'DROP TABLE IF EXISTS {self.TABLE_NAME}')

        print(f'Drop table if exists "{self.TABLE_NAME}". Done.')

        sql_create_table: sql.Composable = sql.SQL('''
            CREATE UNLOGGED TABLE {table} (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                janitza_id INT NOT NULL REFERENCES janitzas(id),
                start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL,
                energy_active_supplied NUMERIC(18,3),
                energy_active_consumed NUMERIC(18,3),
                energy_reactive_fundamental_supplied NUMERIC(18,3),
                energy_reactive_fundamental_consumed NUMERIC(18,3),
                CONSTRAINT uniq_janitza_period UNIQUE (janitza_id, start_ts)
            )
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        print(f'Creating unlogged table "{self.TABLE_NAME}"...', end='\r')

        cur.execute(sql_create_table)

        print(f'Creating unlogged table "{self.TABLE_NAME}". Done.')

        sql_query: sql.Composable = sql.SQL('''
            CREATE INDEX IF NOT EXISTS idx_{table}_janitza_id ON {table}(janitza_id);
            CREATE INDEX IF NOT EXISTS idx_{table}_start_ts ON {table}(start_ts);
            CREATE INDEX idx_{table}_janitza_start ON {table}(janitza_id, start_ts);
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        print(f'Creating indexes for table "{self.TABLE_NAME}"...', end='\r')

        cur.execute(sql_query)

        print(f'Creating indexes for table "{self.TABLE_NAME}". Done.')

    async def get(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None,
            aggregation: str | None=None
        ) -> pd.DataFrame:
        ALLOWED_AGGREGATIONS: list[str | None] = [
            'year', 'month', 'day', 'hour', 'None', None
        ]

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError('Provide either interval or start_ts/end_ts range, not both')

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError('If interval is not provided, both start_ts and end_ts must be given')

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')
        
        if aggregation not in ALLOWED_AGGREGATIONS:
            raise ValueError(
                f'Aggregation must be one of {', '.join(ALLOWED_AGGREGATIONS)}, '
                f'got {aggregation!r}'
            )

        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)

        if aggregation is None:
            aggregation = 'None'

        table_mapping: dict[str, str] = {
            'year': 'janitza_energy_year',
            'month': 'janitza_energy_month',
            'day': 'janitza_energy_day',
            'hour': 'janitza_energy_hour',
            'None': 'janitza_energy'
        }

        table: str = table_mapping[aggregation]

        interval_aggregation_mapping: dict[str ,str] = {
            'year': '1 year',
            'month': '1 month',
            'day': '1 day',
            'hour': '1 hour',
            'None': '15 minutes'
        }

        interval_aggregation: str = interval_aggregation_mapping[aggregation]

        sql_query: sql.Composable = sql.SQL('''
            WITH params AS (
                SELECT
                    {start_ts} AT TIME ZONE 'Europe/Berlin' AS start_ts,
                    {end_ts} AT TIME ZONE 'Europe/Berlin' AS end_ts,
                    INTERVAL {interval_aggregation} AS step_interval
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
            )
            SELECT
                j.id AS janitza_id,
                j.name AS janitza_name,
                i.start_ts AS start_ts,
                (i.start_ts AT TIME ZONE 'Europe/Berlin' + i.step_interval) AT TIME ZONE 'Europe/Berlin' AS end_ts,
                pe.energy_active_supplied,
                pe.energy_active_consumed,
                pe.energy_reactive_fundamental_supplied,
                pe.energy_reactive_fundamental_consumed
            FROM janitzas j
            CROSS JOIN intervals i
            LEFT JOIN {table} pe
                ON pe.janitza_id = j.id
                AND pe.start_ts = i.start_ts
            ORDER BY j.id, i.start_ts
        ''').format(
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts'),
            interval_aggregation=sql.Literal(interval_aggregation),
            table=sql.Identifier(table)
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                try:
                    cur.execute(sql_query, {'start_ts': start_ts, 'end_ts': end_ts})
                except psycopg2.Error as e:
                    self._conn.rollback()
                    raise RuntimeError(f'Query failed: {e.pgcode} {e.pgerror}')

                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

                return df

        df: pd.DataFrame = await asyncio.to_thread(run_query)

        if df.empty:
            return df

        df = df.astype({
            'energy_active_supplied': 'float64',
            'energy_active_consumed': 'float64',
            'energy_reactive_fundamental_supplied': 'float64',
            'energy_reactive_fundamental_consumed': 'float64'
        })

        aggregation_period_map: dict[str, str] = {
            'year': 'Y',
            'month': 'M',
            'day': 'D'
        }

        df['start_ts'] = pd.to_datetime(df['start_ts'])
        df['end_ts'] = pd.to_datetime(df['end_ts'])

        # add a human-friendly label (day/month/year) that clearly identifies
        # the aggregation period next to the raw start_ts/end_ts timestamps.
        # we skip 'hour' since a label like '14:00' would be ambiguous
        # (could mean 13:00–14:00 or 14:00–15:00).

        if aggregation in aggregation_period_map.keys():
            df[aggregation] = (
                df['start_ts']
                .dt.tz_convert('Europe/Berlin')
                .dt.tz_localize(None)
                .dt.to_period(aggregation_period_map[aggregation])
            )

            df = df.set_index(['janitza_name', aggregation])
        else:
            df = df.set_index(['janitza_name', 'start_ts'])

        return df

    ################################################################
    # Internal methods - update
    ################################################################

    async def _ingress(
            self,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> pd.DataFrame:

        t: pd.Timestamp = pd.Timestamp.now()

        print(f'Running update for janitza_energy for {start_ts} - {end_ts}')

        sql_query: sql.Composable = sql.SQL('''
            WITH params AS (
                SELECT
                    {start_ts} AS start_ts,
                    {end_ts} AS end_ts,
                    INTERVAL '15 min' AS step_size,
                    array[29, 34, 59, 64, 112, 117, 127, 132]::int[] AS janitza_measurement_ids
            ),
            data AS (
                SELECT
                    jmd.*
                FROM janitza_measurement_data jmd, params p
                WHERE janitza_measurement_id = ANY(p.janitza_measurement_ids)
                AND ts >= p.start_ts
                AND ts <= p.end_ts
            )
            SELECT
                d.*
            FROM data d, params p
            ORDER BY d.janitza_id, d.ts, d.janitza_measurement_id
        ''').format(
            start_ts=sql.SQL('{}').format(sql.Literal(start_ts)),
            end_ts=sql.SQL('{}').format(sql.Literal(end_ts))
        )

        def run_query() -> pd.DataFrame:
            with self._conn.cursor() as cur:
                cur.execute(sql_query)
                cols = [desc[0] for desc in cur.description]
                df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

            return df
        
        df: pd.DataFrame = await asyncio.to_thread(run_query)

        load_time: pd.Timedelta = pd.Timestamp.now() - t

        # Postgres query returns ``value`` as object
        df['value'] = pd.to_numeric(df['value'])

        # Convert Wh to kWh
        df['value'] = df['value'].div(1_000)
        df['value'] = df['value'].round(3)

        # Merge 15-minute data into the hour column.
        # The order technically does not matter
        # 15-minute data has priority and will overwrite
        # any existing hourly data.
        # It is only important that only one column exists
        # since both resolve to the same name

        merge_map: dict[int, int] = {
            112: 29,
            117: 34,
            127: 59,
            132: 64,
        }

        df['janitza_measurement_id'] = df['janitza_measurement_id'].replace(merge_map)

        # ----
        # Resolve duplicate meter samples per (ts, janitza_id, janitza_measurement_id).
        # Some devices emit overlapping time resolutions (e.g. 15min + 1h)
        # for the same interval, where both records contain the same or
        # near-identical values.
        #
        # We explicitly prioritize higher resolution data (900s = 15min) over
        # lower resolution (3600s = 1h), since the 15min signal is more granular and
        # considered authoritative when both exist.
        #
        # Strategy:
        # - assign resolution priority (900 preferred over 3600)
        # - sort so preferred rows come first
        # - drop duplicates keeping the preferred record deterministically
        #

        priority = {900: 0, 3600: 1}

        df = df.assign(_p=df['resolution'].map(priority).fillna(99))

        df = df.sort_values('_p').drop(columns=['_p'])

        df = df.drop_duplicates(
            subset=['ts', 'janitza_id', 'janitza_measurement_id'],
            keep='first'
        )

        df = df.drop(columns=['id', 'resolution', 'aggregation_id'])

        # very quick up till here

        # This computation takes about 6 seconds for a day of data and 452 Janitzas
        df = df.groupby(['janitza_id', 'janitza_measurement_id']).apply(
            self._compute, include_groups=False, start_ts=start_ts, end_ts=end_ts
        )

        if df.empty:
            stats: dict[str, Any] = {
                'inserts': 0,
                'updates': 0,
                'insert_time': pd.Timedelta(seconds=0),
                'load_time': load_time,
                'transform_time': pd.Timestamp.now() - t - load_time,
                'total_time': pd.Timestamp.now() - t
            }

            return stats

        df = df.reset_index().pivot(
            index=['janitza_id', 'start_ts', 'end_ts'],
            columns='janitza_measurement_id',
            values='value'
        )

        df.columns.name = None

        col_name_mapping: dict[int, str] = {
            col: self._get_measurement_from_id(col)['value']
            for col in df.columns
        }

        df = df.rename(columns=col_name_mapping)

        df['ActiveEnergySupplied'] = df['ActiveEnergySupplied'].mul(-1)
        df['ReactiveEnergyCap'] = df['ReactiveEnergyCap'].mul(-1)

        transform_time: pd.Timedelta = pd.Timestamp.now() - t - load_time

        stats: dict[str, Any] = await self._write(df)

        stats['load_time'] = load_time
        stats['transform_time'] = transform_time
        stats['total_time'] = stats['load_time']
        stats['total_time'] += stats['transform_time']
        stats['total_time'] += stats['insert_time']

        return stats

    async def _write(self, df: pd.DataFrame) -> dict[str, Any]:
        t: pd.Timestamp = pd.Timestamp.now()

        insert_sql: sql.Composable = sql.SQL('''
            INSERT INTO {table} (
                janitza_id,
                start_ts,
                end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            )
            VALUES %s
            ON CONFLICT (janitza_id, start_ts) DO UPDATE
            SET
                end_ts = EXCLUDED.end_ts,
                energy_active_supplied = EXCLUDED.energy_active_supplied,
                energy_active_consumed = EXCLUDED.energy_active_consumed,
                energy_reactive_fundamental_supplied = EXCLUDED.energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed = EXCLUDED.energy_reactive_fundamental_consumed
            RETURNING CASE WHEN xmax = 0 THEN 'insert' ELSE 'update' END AS action
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        records: list[tuple] = [
            (
                row['janitza_id'],
                row['start_ts'].to_pydatetime(),
                row['end_ts'].to_pydatetime(),
                None if pd.isna(row['ActiveEnergySupplied']) else row['ActiveEnergySupplied'],
                None if pd.isna(row['ActiveEnergyConsumed']) else row['ActiveEnergyConsumed'],
                None if pd.isna(row['ReactiveEnergyCap']) else row['ReactiveEnergyCap'],
                None if pd.isna(row['ReactiveEnergyInd']) else row['ReactiveEnergyInd']
            )
            for _, row in df.reset_index().iterrows()
        ]

        def run_insert() -> list[tuple]:
            with self._conn.cursor() as cur:
                try:
                    rows: list[tuple] = execute_values(
                        cur=cur,
                        sql=insert_sql,
                        argslist=records,
                        fetch=True
                    )
                except Exception as e:
                    self._conn.rollback()
                    raise e
            
            self._conn.commit()
            return rows
        
        rows: list[tuple] = await asyncio.to_thread(run_insert)

        stats: dict[str, int] = {
            'inserts': sum(1 for r in rows if r[0] == 'insert'),
            'updates': sum(1 for r in rows if r[0] == 'update'),
            'insert_time': pd.Timestamp.now() - t
        }

        return stats

    def _compute(self, df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        df = df.set_index('ts')

        vals: pd.Series = df['value'].sort_index()

        freq: str = '15min'

        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        idx: pd.DatetimeIndex = pd.date_range(start_ts, end_ts, freq=freq)
        combined_idx: pd.DatetimeIndex = vals.index.union(idx)
        vals = vals.reindex(combined_idx)

        # Fill the outside bounds with consistent values in case the index
        # does not start at `start_ts` or end at `end_ts`
        # `limit_area='outside'` makes sure that only outside data is filled
        # and not missing values inside the series that are surrounded by data
        vals = vals.bfill(limit_area='outside')
        vals = vals.ffill(limit_area='outside')

        vals = vals.interpolate('time')

        vals = vals.reindex(idx)

        intervals: pd.Series = vals.diff()

        interval_df: pd.DataFrame = intervals.reset_index()
        interval_df = interval_df.rename(columns={
            'index': 'end_ts'
        })
        interval_df['start_ts'] = interval_df['end_ts'] - pd.DateOffset(minutes=15)
        interval_df = interval_df.set_index('start_ts')
        interval_df = interval_df.loc[start_ts:end_ts]

        interval_df['value'] = interval_df['value'].round(3)

        return interval_df

    def _get_measurement_from_id(self, measurement_id: int) -> pd.Series:
        """
        Retrieve a Janitza measurement by its ID.

        :param measurement_id: The ID of the measurement to fetch.
        :type measurement_id: int
        :return: A pandas Series containing the measurement data, indexed by column names.
        :rtype: pd.Series
        """

        sql_query: sql.Composable = sql.SQL('''
            SELECT
                *
            FROM janitza_measurements
            WHERE id = {measurement_id};
        ''').format(
            table=sql.Identifier('janitza_measurements'),
            measurement_id=sql.Placeholder('measurement_id')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query, {'measurement_id': measurement_id})

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        df = df.sort_index()

        return df.iloc[0]

    def _update_latest_janitza_energy_mv(self) -> None:
        sql_query: sql.Composable = sql.SQL('''
            REFRESH MATERIALIZED VIEW {lje}
        ''').format(
            lje=sql.Identifier('latest_janitza_energy')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

        self._conn.commit()

        return

    ################################################################
    # Internal methods - utility
    ################################################################

    def _get_range_from_interval(
            self,
            interval: str | int,
            tz: str='Europe/Berlin'
        ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """
        
        """

        INTERVALS: list[str] = [
            'this_hour', 'last_hour', 'last_three_hours',
            'today', 'yesterday', 'last_three_days',
            'this_week', 'last_week', 'last_three_weeks',
            'this_month', 'last_month', 'last_three_months',
            'this_year', 'last_year', 'last_three_years'
        ]

        # Convert timestamp string year
        if re.fullmatch(r'\d{4}', interval):
            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(
                f'{interval}-01-01T00:00:00'
            )
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(years=1)

            return start_ts, end_ts
        
        # Convert timestamp int year
        if type(interval) == int:
            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(
                f'{interval}-01-01T00:00:00'
            )
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(years=1)

            return start_ts, end_ts
        
        # Convert timestamp string month
        if re.fullmatch(r'\d{4}-\d{2}', interval):
            year, month = map(int, interval.split('-'))

            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-01T00:00:00')
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(months=1)

            return start_ts, end_ts
        
        # Convert timestamp string day
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', interval):
            year, month, day = map(int, interval.split('-'))

            start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-{day:02}T00:00:00')
            start_ts = start_ts.tz_localize(tz)
            end_ts: pd.Timestamp = start_ts + pd.offsets.DateOffset(days=1)

            return start_ts, end_ts
        
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

        raise ValueError(
            f"Interval must be one of {', '.join(INTERVALS)} "
            f"or a datetime string 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', "
            f"got {interval!r}"
        )
    
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
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn


class PlantEnergy:
    TABLE_NAME: str = 'plant_energy'

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
        Update the ``plant_energy`` table with data from Janitza meters.

        Either ``interval`` or ``start_ts``/``end_ts`` range must be provided, not both.

        Runtime tests:

        - Full year causes disk full error
        - Single day around 2 seconds

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

        sql_query: sql.Composable = sql.SQL('''
            WITH data AS (
                SELECT
                    *
                FROM {janitza_energy}
                WHERE start_ts >= {start_ts}
                AND start_ts < {end_ts}
            ),
            plant_ratios AS (
                SELECT
                    md2.id AS main_data_2_id,
                    pois.janitza_id AS janitza_id,
                    md2.nominal_power,
                    md2.commissioning_date
                FROM {plant_main_data} md2
                LEFT JOIN {pois} pois
                    ON md2.poi_id = pois.id
                WHERE pois.janitza_id IS NOT NULL
            ),
            ratio_intervals AS (
                SELECT
                    pr.main_data_2_id AS main_data_2_id,
                    d.start_ts,
                    d.end_ts,
                    d.janitza_id,
                    d.energy_active_supplied,
                    d.energy_active_consumed,
                    d.energy_reactive_fundamental_supplied,
                    d.energy_reactive_fundamental_consumed,
                    pr.nominal_power,
                    COALESCE(NULLIF(
                        SUM(pr.nominal_power) OVER (PARTITION BY d.janitza_id, d.start_ts),
                    0), 1) AS poi_rated_power
                FROM data d
                JOIN plant_ratios pr
                    ON pr.janitza_id = d.janitza_id
                    AND pr.commissioning_date <= d.start_ts
            ),
            plant_energy AS (
                SELECT
                    main_data_2_id,
                    start_ts,
                    end_ts,
                    ROUND(SUM(energy_active_supplied * nominal_power / poi_rated_power), 3) AS plant_energy_active_supplied,
                    ROUND(SUM(energy_active_consumed * nominal_power / poi_rated_power), 3) AS plant_energy_active_consumed,
                    ROUND(SUM(energy_reactive_fundamental_supplied * nominal_power / poi_rated_power), 3) AS plant_energy_reactive_fundamental_supplied,
                    ROUND(SUM(energy_reactive_fundamental_consumed * nominal_power / poi_rated_power), 3) AS plant_energy_reactive_fundamental_consumed
                FROM ratio_intervals
                GROUP BY main_data_2_id, start_ts, end_ts
            )
            SELECT
                main_data_2_id,
                start_ts,
                end_ts,
                plant_energy_active_supplied AS energy_active_supplied,
                plant_energy_active_consumed AS energy_active_consumed,
                plant_energy_reactive_fundamental_supplied AS energy_reactive_fundamental_supplied,
                plant_energy_reactive_fundamental_consumed AS energy_reactive_fundamental_consumed
            FROM plant_energy
        ''').format(
            janitza_energy=sql.Identifier('janitza_energy'),
            plant_main_data=sql.Identifier('main_data_2'),
            pois=sql.Identifier('pois'),
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts')
        )

        # Safe against SQL injection attacks since ``sql`` and
        # ``on_conflict_sql`` are not external.
        insert_sql: sql.Composable = sql.SQL('''
            INSERT INTO {table} (
                main_data_2_id,
                start_ts,
                end_ts,
                energy_active_supplied,
                energy_active_consumed,
                energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed
            )
            {data}
            ON CONFLICT (main_data_2_id, start_ts) DO UPDATE
            SET
                end_ts = EXCLUDED.end_ts,
                energy_active_supplied = EXCLUDED.energy_active_supplied,
                energy_active_consumed = EXCLUDED.energy_active_consumed,
                energy_reactive_fundamental_supplied =
                EXCLUDED.energy_reactive_fundamental_supplied,
                energy_reactive_fundamental_consumed =
                EXCLUDED.energy_reactive_fundamental_consumed
            RETURNING CASE WHEN xmax = 0 THEN 'insert' ELSE 'update' END AS action
        ''').format(
            table=sql.Identifier(self.TABLE_NAME),
            data=sql_query
        )

        def run_insert() -> list[tuple]:
            with self._conn.cursor() as cur:
                cur.execute(insert_sql, {'start_ts': start_ts, 'end_ts': end_ts})
                rows = cur.fetchall()
            
            self._conn.commit()

            return rows
        
        rows: list[tuple] = await asyncio.to_thread(run_insert)

        stats: dict[str, int] = {
            'inserts': sum(1 for r in rows if r[0] == 'insert'),
            'updates': sum(1 for r in rows if r[0] == 'update'),
            'time': pd.Timestamp.now() - t
        }

        custom_stats = await self.custom_plants(
            start_ts=start_ts,
            end_ts=end_ts
        )

        stats['inserts'] += custom_stats['inserts']
        stats['updates'] += custom_stats['updates']
        stats['time'] += custom_stats['time']

        return stats
    
    async def custom_plants(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None
        ) -> dict[str, int | pd.Timedelta]:
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

        stats: dict[str, int | pd.Timedelta] = await self.custom_koethen(
            start_ts=start_ts,
            end_ts=end_ts
        )

        return stats

    async def custom_koethen(
            self,
            start_ts: pd.Timestamp,
            end_ts: pd.Timestamp
        ) -> dict[str, int | pd.Timedelta]:
        t = pd.Timestamp.now()

        sql_query: sql.Composable = sql.SQL('''
            WITH upsert AS (
                INSERT INTO {pe} (
                    main_data_2_id,
                    start_ts,
                    end_ts,
                    energy_active_supplied,
                    energy_active_consumed,
                    energy_reactive_fundamental_supplied,
                    energy_reactive_fundamental_consumed
                )
                SELECT
                    {plant_id},
                    start_ts,
                    end_ts,
                    SUM(energy_active_supplied),
                    SUM(energy_active_consumed),
                    SUM(energy_reactive_fundamental_supplied),
                    SUM(energy_reactive_fundamental_consumed)
                FROM {je}
                WHERE janitza_id IN (17, 152, 153, 154, 155, 156)
                AND start_ts >= {start_ts}
                AND start_ts < {end_ts}
                GROUP BY start_ts, end_ts
                ON CONFLICT (main_data_2_id, start_ts) DO UPDATE
                SET
                    end_ts = EXCLUDED.end_ts,
                    energy_active_supplied = EXCLUDED.energy_active_supplied,
                    energy_active_consumed = EXCLUDED.energy_active_consumed,
                    energy_reactive_fundamental_supplied = EXCLUDED.energy_reactive_fundamental_supplied,
                    energy_reactive_fundamental_consumed = EXCLUDED.energy_reactive_fundamental_consumed
                RETURNING (xmax = 0) AS inserted
            )
            SELECT
                COUNT(*) FILTER (WHERE inserted) AS inserted_count,
                COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
            FROM upsert;
        ''').format(
            je=sql.Identifier('janitza_energy'),
            pe=sql.Identifier('plant_energy'),
            start_ts=sql.Placeholder('start_ts'),
            end_ts=sql.Placeholder('end_ts'),
            plant_id=sql.Placeholder('plant_id')
        )

        def run_insert() -> tuple[int, int]:
            with self._conn.cursor() as cur:
                cur.execute(sql_query, {
                    'start_ts': start_ts,
                    'end_ts': end_ts,
                    'plant_id': 190
                })

                inserts, updates = cur.fetchone()
            
            self._conn.commit()

            return inserts, updates
        
        inserts, updates = await asyncio.to_thread(run_insert)

        stats: dict[str, int | pd.Timedelta] = {
            'inserts': inserts,
            'updates': updates,
            'time': pd.Timestamp.now() - t
        }

        return stats

    async def get(
            self,
            interval: str | None=None,
            start_ts: dt.datetime | None=None,
            end_ts: dt.datetime | None=None,
            agg: str | None=None
        ) -> pd.DataFrame:
        ALLOWED_AGGREGATIONS: list[str | None] = [
            'year', 'month', 'day', 'hour', 'quarter-hour'
        ]
        TABLES: dict[str, str] = {
            'quarter-hour': 'plant_energy',
            'hour': 'plant_energy_hour',
            'day': 'plant_energy_day',
            'month': 'plant_energy_month',
            'year': 'plant_energy_year'
        }
        OFFSETS: dict[str, pd.DateOffset] = {
            'quarter-hour': pd.DateOffset(minutes=15),
            'hour': pd.DateOffset(hours=1),
            'day': pd.DateOffset(days=1),
            'month': pd.DateOffset(months=1),
            'year': pd.DateOffset(years=1)
        }
        TIMEDELTAS: dict[str, pd.Timedelta] = {
            'quarter-hour': pd.Timedelta(minutes=15),
            'hour': pd.Timedelta(hours=1),
            'day': pd.Timedelta(days=1),
            'month': pd.Timedelta(days=31),
            'year': pd.Timedelta(days=365)
        }

        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError('Provide either interval or start_ts/end_ts range, not both')

        if interval is None and (start_ts is None or end_ts is None):
            raise ValueError('If interval is not provided, both start_ts and end_ts must be given')

        if start_ts is not None and start_ts.tzinfo is None:
            raise ValueError('start_ts must be timezone-aware')

        if end_ts is not None and end_ts.tzinfo is None:
            raise ValueError('end_ts must be timezone-aware')
        
        if agg is None:
            agg = 'quarter-hour'
        
        if agg.lower() not in ALLOWED_AGGREGATIONS:
            raise ValueError(
                f'Aggregation must be one of {', '.join(ALLOWED_AGGREGATIONS)}, '
                f'got {agg!r}'
            )

        if interval is not None:
            start_ts, end_ts = self._get_range_from_interval(interval)
        
        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        td: pd.Timedelta = end_ts - start_ts

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

        estimated_records: int = (td.total_seconds() // TIMEDELTAS.get(agg).total_seconds()) * plant_count
        
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
                pe.energy_active_consumed,
                pe.energy_reactive_fundamental_supplied
                AS energy_reactive_fundamental_capacitive,
                pe.energy_reactive_fundamental_consumed
                AS energy_reactive_fundamental_inductive
            FROM {pe} pe
            WHERE pe.start_ts >= {start_ts}
            AND pe.start_ts < {end_ts}
            ORDER BY pe.main_data_2_id, pe.start_ts
        ''').format(
            pe=sql.Identifier(TABLES.get(agg)),
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
            'energy_active_consumed',
            'energy_reactive_fundamental_capacitive',
            'energy_reactive_fundamental_inductive'
        ]

        df[float_cols] = df[float_cols].astype('float64')

        df['plant_id'] = df['plant_id'].astype(int)

        start_idx: pd.DatetimeIndex = pd.date_range(
            start=start_ts,
            end=end_ts,
            freq=OFFSETS.get(agg),
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
        df['end_ts'] = df['start_ts'].dt.tz_convert('Europe/Berlin') + OFFSETS.get(agg)
        df['end_ts'] = df['end_ts'].dt.tz_convert('UTC')
        df = df.set_index(['plant_id', 'start_ts', 'end_ts'])

        return df

    def clear(self) -> None:
        """
        Deletes the entire ``plant_energy`` table.

        This effectively resets the entire plant_energy layer and should be followed by
        a call to :meth:`update` to restore consistency.

        :param confirm: Needs to be set to True for the method to run, defaults to False
        :type confirm: bool, optional
        :raises ValueError: If confirm was not set to True
        """

        cur = self._conn.cursor()

        print(f'Drop table if exists "{self.TABLE_NAME}"...', end='\r')

        cur.execute(f'DROP TABLE IF EXISTS {self.TABLE_NAME} CASCADE')

        print(f'Drop table if exists "{self.TABLE_NAME}". Done.')

        sql_create_table: sql.Composable = sql.SQL('''
            CREATE UNLOGGED TABLE {table} (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                main_data_2_id INT NOT NULL REFERENCES main_data_2(id),
                start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL,
                energy_active_supplied NUMERIC(18,3),
                energy_active_consumed NUMERIC(18,3),
                energy_reactive_fundamental_supplied NUMERIC(18,3),
                energy_reactive_fundamental_consumed NUMERIC(18,3),
                CONSTRAINT uniq_plant_period UNIQUE (main_data_2_id, start_ts)
            )
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        print(f'Creating unlogged table "{self.TABLE_NAME}"...', end='\r')

        cur.execute(sql_create_table)

        print(f'Creating unlogged table "{self.TABLE_NAME}". Done.')

        sql_idx: sql.Composable = sql.SQL('''
            CREATE INDEX IF NOT EXISTS idx_{table}_main_data_2_id ON {table}(main_data_2_id);
            CREATE INDEX IF NOT EXISTS idx_{table}_start_ts ON {table}(start_ts);
            CREATE INDEX idx_{table}_main_data_2_start ON {table}(main_data_2_id, start_ts);
        ''').format(
            table=sql.Identifier(self.TABLE_NAME)
        )

        print(f'Creating indexes for table "{self.TABLE_NAME}"...', end='\r')

        cur.execute(sql_idx)

        print(f'Creating indexes for table "{self.TABLE_NAME}". Done.')

    ################################################################
    # Internal methods - utility
    ################################################################

    def _get_range_from_interval(
            self,
            interval: str | int,
            tz: str='Europe/Berlin'
        ) -> tuple[pd.Timestamp, pd.Timestamp]:
        INTERVALS: list[str] = [
            'this_hour', 'last_hour', 'last_three_hours',
            'today', 'yesterday', 'last_three_days',
            'this_week', 'last_week', 'last_three_weeks',
            'this_month', 'last_month', 'last_three_months',
            'this_year', 'last_year', 'last_three_years'
        ]

        if type(interval) == str and interval not in INTERVALS:
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

        if type(interval) == int:
            start_ts: dt.datetime = pd.Timestamp.fromisoformat(
                f'{interval}-01-01'
            )
            start_ts = start_ts.tz_localize('Europe/Berlin')
            end_ts: dt.datetime = start_ts + pd.offsets.YearBegin(1)

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
    # Internal methods - db utility
    ################################################################

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
