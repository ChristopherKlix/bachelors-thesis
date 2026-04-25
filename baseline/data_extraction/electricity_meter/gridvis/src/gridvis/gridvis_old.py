import json
from pathlib import Path
from typing import Any

import pandas as pd
import datetime as dt
import re
import requests


class FetchError(Exception):
    def __init__(self, status_code: int, body: str, original_exception: Exception):
        self.status_code = status_code
        self.body = body
        self.original_exception = original_exception
        super().__init__(f'FetchError: {status_code}, Body: {body}, Original Exception: {original_exception}')


class GridVisRESTAPI:
    BASE_URL: str = 'https://gridvis.enerparc.local:8080/rest/1'
    PROJECT_OM: str = 'EP_OM_GridVis'
    CERT_PATH: Path = Path('./gridvis_cert.pem')
    HTTP_HEADERS: dict = {
        'Accept': 'application/json'
    }

    def __init__(self, use_dump: bool = False):
        self.use_dump: bool = use_dump
        self.dump_path: Path = Path('gridvis_dump')

    def test(self) -> bool:
        import socket
        import platform
        import subprocess

        domain = 'https://gridvis.enerparc.local'
        ip_address = socket.gethostbyname(domain)

        # Determine the ping command based on the OS
        param = '-n' if platform.system().lower() == 'windows' else '-c'
        command = ['ping', param, '1', ip_address]  # Ping once

        # Execute the command
        return subprocess.call(command) == 0

    def get_all_devices(self) -> pd.DataFrame:
        headers: dict = {
            'Accept': 'application/json'
        }

        url: str = self._construct_url(self.BASE_URL, 'projects', self.PROJECT_OM, 'devices')

        if self.use_dump:
            path: Path = self.dump_path.joinpath('devices.json')

            if path.exists():
                with path.open() as f:
                    json_data = json.load(f)
            else:
                raise FileNotFoundError(f'Dump file {path} does not exist.')
        else:
            try:
                json_data = self._fetch(url=url, headers=headers)
            except Exception as e:
                print(f'An error occurred: {e}')
                raise e

        df: pd.DataFrame = pd.json_normalize(json_data, 'device')
        df = df.set_index('id')
        return df

    def get_data(self, janitza_id: int | str, start: dt.datetime, end: dt.datetime):
        # Prepare datetime for further processing
        # Localize if necessary by assuming UTC
        # and convert to UTC if tz-aware
        if start.tzinfo is None:
            start = pd.Timestamp(start).tz_localize('UTC')

        start = pd.Timestamp(start).tz_convert('UTC')
        request_start = start
        start = start - dt.timedelta(hours=1)

        if end.tzinfo is None:
            end = pd.Timestamp(end).tz_localize('UTC')

        end = pd.Timestamp(end).tz_convert('UTC')
        end = end - dt.timedelta(hours=1)

        # Prepare request parameters and endpoints
        params: dict = {
            'start': f'UTC_{int(start.timestamp() * 1000)}',
            'end': f'UTC_{int(end.timestamp() * 1000)}'
        }

        endpoints: list[(str, str)] = [
            ('ActiveEnergySupplied', 'energy_supplied_kwh'),
            ('ActiveEnergyConsumed', 'energy_consumed_kwh'),
            ('ReactiveEnergy', 'reactive_energy_kvarh')
        ]

        janitza_id = f'{janitza_id}'

        dfs: list[pd.DataFrame] = []

        for endpoint in endpoints:
            df = self._get_values(
                janitza_id=janitza_id,
                headers=self.HTTP_HEADERS,
                params=params,
                endpoint=endpoint
            )

            # For some reason there are duplicate entries in the
            # MSSQL database for GridVis, those duplicate entries
            # are there by mistake and need to be removed
            if df.index.duplicated().any():
                print(f'Duplicates in index found for Janitza #{janitza_id} endpoint: {endpoint}')
                df = df[~df.index.duplicated(keep='first')]

            dfs.append(df)

        # Joins the dfs on the 'start' index and merges the 'end' column as well
        try:
            df: pd.DataFrame = pd.concat(dfs, axis=1, join='inner').loc[:,~pd.concat(dfs, axis=1).columns.duplicated()]
        except pd.errors.InvalidIndexError as e:
            raise IndexError(f'InvalidIndexError for Janitza #{janitza_id}')
        except TypeError as e:
            raise TypeError(f'TypeError for Janitza #{janitza_id}')


        # Localize and convert tz to UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')

        df.index = df.index.tz_convert('UTC')

        # Create absolute data df
        df_absolute = df.copy(deep=True)
        df_absolute.pop('end')

        # Create difference data df
        df_diff = df.copy(deep=True)
        df_diff = df_diff.reset_index()

        start_series = df_diff.pop('start')
        end_series = df_diff.pop('end')

        # Localize and convert tz to UTC
        if end_series.dt.tz is None:
            end_series = end_series.dt.tz_localize('UTC')

        end_series = end_series.dt.tz_convert('UTC')

        # Calculate the difference between consecutive rows
        # Drop the first row which now contains NaN
        # Reindex the df since index 0 was dropped
        df_diff = df_diff.diff().dropna().reset_index(drop=True)

        df_diff.insert(0, 'start', start_series)
        df_diff.insert(0, 'end', end_series)

        df_diff = df_diff.set_index('start')

        # Create relative data df
        df_relative = df_diff.copy(deep=True)

        # Since it is not guaranteed that the
        # REST API request will return the entire
        # range this new index covers start to finish
        complete_index = pd.date_range(
            start=request_start,
            end=end,
            freq='h',
            tz='UTC'
        )
        complete_index.name = 'start'

        # Reindex the df to include all timestamps
        df_absolute = df_absolute.reindex(complete_index)

        # Fill gaps by forward filling
        df_absolute = df_absolute.ffill()
        # Fill missing timestamps before Janitza was commissioned
        df_absolute = df_absolute.fillna(0.0)

        df_relative = df_relative.reindex(complete_index)
        df_relative['end'] = complete_index + pd.Timedelta(hours=1)
        df_relative = df_relative.fillna(0)

        # Since all previous steps should elimated potential
        # negative differences this is to ensure
        # consistency and only positive values for supplied energy
        df_relative['energy_supplied_kwh'] = df_relative['energy_supplied_kwh'].apply(lambda x: x if x >= 0 else 0)
        df_relative['energy_supplied_kwh'] = df_relative['energy_supplied_kwh'].round(3)

        return df_absolute, df_relative

    def get_power(self, janitza_id: int | str, start: dt.datetime, end: dt.datetime):
        # Prepare datetime for further processing
        # Localize if necessary by assuming UTC
        # and convert to UTC if tz-aware
        if start.tzinfo is None:
            start = pd.Timestamp(start).tz_localize('UTC')

        start = pd.Timestamp(start).tz_convert('UTC')
        request_start = start
        start = start - dt.timedelta(minutes=15)

        if end.tzinfo is None:
            end = pd.Timestamp(end).tz_localize('UTC')

        end = pd.Timestamp(end).tz_convert('UTC')
        end = end - dt.timedelta(minutes=15)

        # Prepare request parameters and endpoints
        params: dict = {
            'start': f'UTC_{int(start.timestamp() * 1000)}',
            'end': f'UTC_{int(end.timestamp() * 1000)}'
        }

        endpoint: (str, str) = ('PowerActive', 'power_kw')

        janitza_id = f'{janitza_id}'

        # ----
        url: str = self._construct_url(
            self.BASE_URL,
            'projects',
            self.PROJECT_OM,
            'devices',
            janitza_id,
            'hist',
            'values',
            endpoint[0],
            'SUM13',
            '900'
        )

        try:
            json_data = self._fetch(url=url, params=params, headers=self.HTTP_HEADERS)
        except FetchError as e:
            if e.status_code == 204:
                json_data = None
            else:
                print(f'An error occurred for Janitza #{janitza_id}: {e}')
                exit(-1)

        if json_data is None:
            print(f'No data for Janitza #{janitza_id} endpoint: {endpoint[1]}')
            return None

        df: pd.DataFrame = self._parse_json_to_df(json_data, endpoint[1])

        # Localize and convert tz to UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')

        df.index = df.index.tz_convert('UTC')

        df['power_kw'] = df['power_kw'] * -1

        return df

    def convert_timezone(self, df: pd.DataFrame, tz: str) -> pd.DataFrame:
        # Convert the index if it's a datetime-like index
        if pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = df.index.tz_convert(tz)

        # Convert datetime columns to the specified timezone
        dt_types = ['datetime', 'datetime64']
        pandas_datetime_types = ['datetime64[ns]', 'datetime64[ns, UTC]']
        all_datetime_types = dt_types + pandas_datetime_types

        for col in df.select_dtypes(include=all_datetime_types):
            df[col] = df[col].dt.tz_convert(tz)

        return df

    def _get_values(self, janitza_id: str, headers: dict[str, Any], params: dict[str, Any], endpoint: str):
        url: str = self._construct_url(
            self.BASE_URL,
            'projects',
            self.PROJECT_OM,
            'devices',
            janitza_id,
            'hist',
            'values',
            endpoint[0],
            'SUM13',
            '3600'
        )

        try:
            json_data = self._fetch(url=url, params=params, headers=headers)
        except FetchError as e:
            if e.status_code == 204:
                json_data = None
            else:
                print(f'An error occurred for Janitza #{janitza_id}: {e}')
                exit(-1)

        if json_data is None:
            print(f'No data for Janitza #{janitza_id} endpoint: {endpoint[1]}')
            empty_df: pd.DataFrame = self._generate_empty_df(
                janitza_id=janitza_id,
                params=params,
                energy_column=endpoint[1]
            )
            return empty_df

        df: pd.DataFrame = self._parse_json_to_df(json_data, endpoint[1])

        return df

    def _generate_empty_df(self, janitza_id: str, params: dict[str, Any], energy_column: str) -> pd.DataFrame:
        start_str: str = params.get('start')
        end_str: str = params.get('end')

        if start_str is None or end_str is None:
            raise ValueError(f'Couldn\' generate DataFrame for Janitza #{janitza_id}')

        # start: dt.datetime = dt.datetime.fromisoformat(re.sub(r'^ISO8601_', '', start_str))
        # end: dt.datetime = dt.datetime.fromisoformat(re.sub(r'^ISO8601_', '', end_str))

        start: dt.datetime = dt.datetime.fromtimestamp(int(re.sub(r'^UTC_', '', start_str)) / 1000)
        end: dt.datetime = dt.datetime.fromtimestamp(int(re.sub(r'^UTC_', '', end_str)) / 1000)

        # Generate a range of hourly timestamps
        start_range = pd.date_range(start=start, end=end, freq='h', tz='UTC')

        # Create a DataFrame with 'start' index
        df = pd.DataFrame(index=start_range)
        df.index.name = 'start'

        # Add 'end' column, offset by +1 hour
        df['end'] = df.index + pd.Timedelta(hours=1)

        # Add the energy column filled with 0
        df[energy_column] = 0

        return df

    def _parse_json_to_df(self, json_data: dict[str, Any], energy_column: str) -> pd.DataFrame:
        df: pd.DataFrame = pd.json_normalize(json_data, 'values')

        # Convert 'startTime' and 'endTime' to datetime objects from nanoseconds
        # df['startTime'] = pd.to_datetime(df['startTime'].astype('int64'), unit='ns').dt.floor('s')
        df['start'] = pd.to_datetime(df['endTime'].astype('int64'), unit='ns').dt.floor('s')
        df['start'] = df['start'].dt.tz_localize('UTC')
        df['end'] = df['start'] + dt.timedelta(hours=1)

        # df = df.rename(
        #     columns={
        #         'startTime': 'start',
        #         'endTime': 'end'
        #     }
        # )
        df.pop('startTime')
        df.pop('endTime')

        df = df.set_index('start').drop(columns=['min', 'max'])

        df = df.sort_index()
        df[energy_column] = df.pop('avg')
        df[energy_column] = df[energy_column].astype('float64') / 1_000

        # Convert 'avg' to float
        df[energy_column] = df[energy_column].round(3)

        return df

    def _fetch(
        self,
        url: str,
        params: dict[str, Any] = None,
        headers: dict[str, str] = None
    ) -> dict[str, Any]:
        """
        Fetches JSON data from a REST API and returns it as a dictionary.

        Args:
            url (str): The API endpoint URL.
            params (Dict[str, Any], optional): Optional query parameters to include in the request.
            headers (Dict[str, str], optional): Optional headers to include in the request.

        Returns:
            Dict[str, Any]: The JSON data parsed into a dictionary.

        Raises:
            ValueError: If the response does not contain valid JSON.
            requests.RequestException: For issues with the HTTP request.
        """
        r: requests.Response | None = None

        import pip_system_certs.wrapt_requests

        try:
            r = requests.get(
                url=url,
                params=params,
                headers=headers
            )

            r.raise_for_status()
            json_data = r.json()
            return json_data
        except requests.RequestException as e:
            raise FetchError(r.status_code if r else None, r.text if r else None, e)
        except ValueError as e:
            raise FetchError(r.status_code if r else None, r.text if r else None, e)

    def _construct_url(self, *args: str) -> str:
        separator: str = '/'
        url: str = separator.join(args)
        url = re.sub(r'(?<!:)//+', '/', url)
        return url
