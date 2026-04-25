from typing import Any

import pandas as pd
import datetime as dt

import requests
from requests import TooManyRedirects
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import warnings

from importlib.resources import files
import tempfile

from .enums import PROJECTS, VALUES, TYPES, TIMEBASES


class GridVisRESTAPI:
    def __init__(self):
        self._api_url: str | None = None
        self._project: PROJECTS = PROJECTS.UNSET
        self._allow_self_signed_certificate: bool = False

    ################################################################
    # Public Methods - Configuration
    ################################################################

    def set_project(self, project: PROJECTS):
        if not isinstance(project, PROJECTS):
            raise ValueError(
                f'The argument for "project" must be of type "PROJECTS".'
                f'If the desired project is not available in this enum,'
                f'please contact the author of the package to add the project to the package.'
            )

        self._project = project
    
    def set_api_url(self, url: str) -> None:
        self._api_url = url

    def allow_self_signed_certificate(self, allow: bool):
        self._allow_self_signed_certificate = allow

    ################################################################
    # Public Methods - Data
    ################################################################

    def get_devices(self, include_virtual: bool = False) -> pd.DataFrame:
        url: str = f'{self._api_url}/projects/{self._project.value}/devices'

        params: dict[str, str] = {}

        headers: dict[str, str] = {
            'Accept': 'application/json'
        }

        response: requests.Response = self._http_get(
            url=url,
            params=params,
            headers=headers
        )

        data: dict[str, Any] = response.json()

        df: pd.DataFrame = pd.DataFrame(data.get('device'))

        df = df.rename(columns={
            'connectionString': 'connection_string',
            'id': 'janitza_id',
            'typeDisplayName': 'type_display_name',
            'serialNr': 'serial_number',
            'name': 'janitza_name',
            'type': 'type',
            'description': 'description'
        })

        df = df.set_index('janitza_id')

        if not include_virtual:
            df = df[df['type'] != 'XXXVirtual']
            df = df[df['type'] != 'XXXVirtualKPI']

        return df

    def hist_values(
            self,
            p_id: int | str,
            p_value: VALUES,
            p_type: TYPES,
            p_timebase: TIMEBASES,
            start: dt.datetime,
            end: dt.datetime
        ) -> pd.DataFrame:
        self._validate_args(p_value, p_type, p_timebase, start, end)

        start = self._parse_dt_tz(start, 'start')
        end = self._parse_dt_tz(end, 'end')

        # Adjust start/end to compensate for API shifting provided timestamps by one full interval
        start -= pd.Timedelta(seconds=int(p_timebase.value))
        end -= pd.Timedelta(seconds=int(p_timebase.value))

        start_ns: int = self._dt_to_ts_ns(start)
        end_ns: int = self._dt_to_ts_ns(end)

        # Parse possible int to str
        p_id = f'{p_id}'

        url: str = (f'{self._api_url}/projects/{self._project.value}/devices/{p_id}/hist/values/'
                    f'{p_value.value}/{p_type.value}/{p_timebase.value}')

        params: dict[str, str] = {
            'start': f'UTC_{start_ns}',
            'end': f'UTC_{end_ns}'
        }

        headers: dict[str, str] = {
            'Accept': 'application/json'
        }

        response: requests.Response = self._http_get(
            url=url,
            params=params,
            headers=headers
        )

        if response.status_code == 204:
            raise RuntimeError(
                f'GridVis REST API responded with a "{response.status_code}" status code. '
                f'Make sure the provided parameters for the endpoint exist '
                f'and there is data for the provided interval.\n'
                f'URL: {response.url}\n'
                f'Endpoint: {p_id}/{p_value.value}/{p_type.value}/{p_timebase.value}\n'
                f'Interval: {start} - {end}\n'
                f'Janitza ID: {p_id}\n'
                f'Value: {p_value.value}\n'
                f'Type: {p_type.value}\n'
                f'Timebase: {p_timebase.value}'
            )

        if response.status_code != 200:
            raise RuntimeError(
                f'GridVis REST API responded with a "{response.status_code}" status code. '
                f'Make sure there is no network issue and the GridVis REST API is running.'
            )

        p_meta: dict[str, Any] = self._get_hist_values_meta_data(p_id)

        p_meta = self._get_p_meta(p_meta, p_value, p_type, p_timebase)

        df: pd.DataFrame = self._parse_json_to_df(
            json_data=response.json(),
            p_id=p_id,
            p_value=p_value,
            p_type=p_type,
            p_timebase=p_timebase,
            p_meta=p_meta
        )

        return df
    
    def filter_full_intervals(
            self,
            df: pd.DataFrame,
            interval: dt.timedelta,
            tolerance: dt.timedelta = dt.timedelta(seconds=1)
        ) -> pd.DataFrame:
        if interval > dt.timedelta(hours=1):
            raise ValueError(f'"interval" must be less than dt.timedelta(hours=1).')
        
        # Ensure that the start timestamp is HH:MM:00
        start_second_ok = (df.index.second == 0)

        # Ensures the minute matches the interval
        # e.g.: HH:15:SS for 15 min intervals
        minutes_in_interval: int = interval.seconds // 60
        start_minute_ok = ((df.index.minute % minutes_in_interval) == 0)

        # Ensure that the timedelta between start and end matches the interval
        # in the allowed tolerance
        interval_is_correct = ((df['end'] - df.index - interval).abs() <= tolerance)

        mask = start_second_ok & start_minute_ok & interval_is_correct

        return df[mask]

    ################################################################
    # Interal Methods
    ################################################################

    def _get_p_meta(
            self,
            p_meta: dict[str, Any],
            p_value: VALUES,
            p_type: TYPES,
            p_timebase: TIMEBASES
        ) -> dict[str, Any]:
        try:
            return next(
                v for v in p_meta.get('value')
                if v['valueType']['type'] == p_type.value
                and v['valueType']['value'] == p_value.value
                and v['timebase'] == int(p_timebase.value)
            )
        except StopIteration as _:
            # Raised when no entry for configuration was found.
            # Since data for this endpoint has already been returned
            # there must be meta for that data
            raise RuntimeError(
                f'Meta entry not found for type={p_type.value},'
                f'value={p_value.value}, timebase={p_timebase.value}'
            )

    def _get_hist_values_meta_data(self, p_id: int | str) -> dict[str, Any]:
        url: str = f'{self._api_url}/projects/{self._project.value}/devices/{p_id}/hist/values'

        headers: dict[str, str] = {
            'Accept': 'application/json'
        }

        response: requests.Response = self._http_get(
            url=url,
            headers=headers
        )

        return response.json()

    ################################################################
    # Interal Methods - HTTP
    ################################################################
    
    def _http_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None
    ) -> requests.Response:
        if self._allow_self_signed_certificate:
            urllib3.disable_warnings(InsecureRequestWarning)
        else:
            warnings.simplefilter('default', InsecureRequestWarning)

        verify = not self._allow_self_signed_certificate

        try:
            response: requests.Response = requests.get(
                url=url,
                params=params,
                headers=headers,
                verify=verify
            )
        except ConnectionError as e:
            raise e
        except TimeoutError as e:
            raise e
        except TooManyRedirects as e:
            raise e

        return response

    ################################################################
    # Internal Methods - Parser
    ################################################################

    def _parse_dt_tz(self, ts: dt.datetime, name: str) -> dt.datetime:
        if ts.tzinfo is None:
            print(f'Warning. Provided timestamp for "{name}" is tz-naive. Will be assumed UTC.')
            ts = pd.Timestamp(ts).tz_localize('UTC')

        ts = pd.Timestamp(ts).tz_convert('UTC')

        return ts

    def _parse_json_to_df(
            self,
            json_data: dict[str, Any],
            p_id: str,
            p_value: VALUES,
            p_type: TYPES,
            p_timebase: TIMEBASES,
            p_meta: dict[str, Any]
        ) -> pd.DataFrame:
        df: pd.DataFrame = pd.json_normalize(json_data, 'values')

        # Convert 'startTime' and 'endTime' to datetime objects from nanoseconds
        start_series: pd.Series = pd.to_datetime(df['startTime'].astype('int64'), unit='ns').dt.floor('s')
        start_series = start_series.dt.tz_localize('UTC')

        end_series: pd.Series = pd.to_datetime(df['endTime'].astype('int64'), unit='ns').dt.floor('s')
        end_series = end_series.dt.tz_localize('UTC')

        df.pop('startTime')
        df.pop('endTime')

        df['ts'] = end_series
        df = df.set_index('ts')

        unit: str = p_meta.get('valueType').get('unit')

        cols: pd.Index = df.columns

        cols = pd.Index([f'{p_value.value}_{col}_{unit}' for col in cols])
        df.columns = cols

        for col in cols:
            df[col] = df[col].astype('float64')

        return df

    ################################################################
    # Internal Methods - Validation
    ################################################################

    def _validate_configuration(self):
        if self._project == PROJECTS.UNSET:
            raise ValueError(
                f'No project has been configured.'
                f'Use {self.__class__.__name__}.set_project to set a GridVis Project.'
            )

    def _validate_args(
        self,
        p_value: VALUES,
        p_type: TYPES,
        p_timebase: TIMEBASES,
        start: dt.datetime,
        end: dt.datetime
    ):
        if not isinstance(p_value, VALUES):
            raise ValueError(f'Invalid dtype for "p_value". Must be of type "VALUES".')

        if not isinstance(p_type, TYPES):
            raise ValueError(f'Invalid dtype for "p_type". Must be of type "TYPES".')

        if not isinstance(p_timebase, TIMEBASES):
            raise ValueError(f'Invalid dtype for "p_timebase". Must be of type "TIMEBASES".')

        if not isinstance(start, dt.datetime):
            raise ValueError(f'Invalid dtype for "start". Must be of type "dt.datetime".')

        if not isinstance(end, dt.datetime):
            raise ValueError(f'Invalid dtype for "end". Must be of type "dt.datetime".')

    ################################################################
    # Internal Methods - Util
    ################################################################

    def _dt_to_ts_ns(self, ts: dt.datetime) -> int:
        return int(ts.timestamp() * 1_000)
