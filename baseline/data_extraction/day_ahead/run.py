import os
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd
import requests


class Benchmark:
    BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    BASE_URL: str = 'https://api.energy-charts.info'
    STATS_PATH: Path = Path('stats')

    def __init__(self):
        self.start_ts: pd.Timestamp | None = None
        self.end_ts: pd.Timestamp | None = None

    def run(self) -> None:
        print(f'#' * 64)
        print(f'Benchmark: Day-ahead')
        print(f'Interval:  365 days')
        print(f'#' * 64)
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'2025-01-01')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)

        days: list[pd.Timestamp] = pd.date_range(
            start=start_ts,
            end=end_ts,
            inclusive='left',
            freq='D'
        ).to_list()

        stats: dict[str, list[pd.Timedelta]] = {
            'extract': [],
            'transform': [],
            'load': []
        }

        stats_df: pd.DataFrame = pd.DataFrame.from_dict(stats, orient='columns')
        stats_df.index.name = 'benchmark'

        for day in days:
            day_stats: dict[str, pd.Timedelta] = self.run_etl(day)
            stats_df.loc[day] = day_stats

        stats_df['total'] = stats_df.sum(axis=1)

        def parse_timedelta(td: pd.Timedelta) -> float:
            td = td.value
            td = td / 1_000_000_000
            return td

        for col in stats_df.columns:
            stats_df[col] = stats_df[col].apply(parse_timedelta)
            stats_df[col] = stats_df[col].round(6)
        
        stats_df = stats_df.reset_index()
        stats_df['benchmark'] = stats_df['benchmark'].dt.strftime('%Y-%m-%d')

        os.makedirs(self.STATS_PATH, exist_ok=True)

        filename: str = f'stats_day_ahead.csv'
        file_path: Path = self.STATS_PATH / filename

        print(stats_df)
        print(f'Total Time: {stats_df.sum()['total']}')

        stats_df.to_csv(
            file_path,
            float_format='%.6f',
            index=False
        )

        return stats_df

    def run_etl(self, day: pd.Timestamp) -> dict[str, pd.Timedelta]:
        self._set_interval(day)

        stats: dict[str, pd.Timedelta] = {}

        print(f'=' * 64)
        print(f'Benchmark: Day-ahead')
        print(f'Day:       {day.date()}')
        print(f'=' * 64)
        print('EXTRACT')
        print(f'-' * 64)
        print(f'Running...', end='', flush=True)

        _t: pd.Timestamp = self._now()
        data: dict[str, Any] = self.extract()
        stats['extract'] = self._now() - _t

        print(f'\rDone. {stats['extract']}', flush=True)
        print(f'-' * 64)
        print('TRANSFORM')
        print(f'-' * 64)
        print(f'Running...', end='', flush=True)

        _t = self._now()
        df: pd.DataFrame = self.transform(data)
        stats['transform'] = self._now() - _t

        print(f'\rDone. {stats['transform']}', flush=True)
        print(f'-' * 64)
        print('LOAD')
        print(f'-' * 64)
        print(f'Running...', end='', flush=True)
        
        _t = self._now()
        self.load(df)
        stats['load'] = self._now() - _t

        print(f'\rDone. {stats['load']}', flush=True)
        print(f'=' * 64)
        print(f'Total: {stats['extract'] + stats['transform'] + stats['load']}')
        print(f'=' * 64)

        return stats

    def extract(self) -> dict[str, Any]:
        url: str = f'{self.BASE_URL}/price'

        headers: dict[str, str] = {
            'Accept': 'application/json'
        }

        params: dict[str, str] = {
            'bzn': 'DE-LU',
            'start': f'{self.start_ts.isoformat(timespec='minutes')}',
            'end': f'{self.end_ts.isoformat(timespec='minutes')}'
        }

        response: requests.Response | None = None

        try:
            response = requests.get(
                url=url,
                headers=headers,
                params=params,
                timeout=20
            )
        except requests.exceptions.Timeout as _:
            print(f'API seems busy. Waiting for 60 s.')
            sleep(60)
            return self.extract()
        except Exception as _:
            raise RuntimeError(f'Could not retrieve data.')
        
        if response.headers['content-type'] != 'application/json':
            if response.status_code == 404:
                return None
        
        try:
            data: dict[str, Any] = response.json()
        except requests.exceptions.JSONDecodeError as _:
            raise RuntimeError(f'{response.text}')
        
        return data
        
    def transform(self, data: dict[str, Any]) -> pd.DataFrame:
        df: pd.DataFrame = self._deserialize(data)

        df = self._interpolate_idx(df)

        df = df.reset_index()
        df['end_ts'] = df['start_ts'] + pd.DateOffset(minutes=15)
        df = df.set_index(['start_ts', 'end_ts'])

        return df
    
    def _deserialize(self, data: dict[str, Any]) -> pd.DataFrame:
        ts_list: list[int] = data.get('unix_seconds')
        prices_list: list[float] = data.get('price')

        ts_list_dt: list[pd.Timestamp] = [
            pd.Timestamp.fromtimestamp(ts, tz='Europe/Berlin') for ts in ts_list
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
        start_ts_series = start_ts_series.dt.tz_convert('UTC')

        sdac_ts: pd.Timestamp = pd.Timestamp.fromisoformat('2025-10-01T00:00')
        sdac_ts = sdac_ts.tz_localize('Europe/Berlin')
        hourly: pd.Timedelta = pd.Timedelta(hours=1)
        quarter_hourly: pd.Timedelta = pd.Timedelta(minutes=15)

        resolution_series: pd.Series = start_ts_series.apply(
            lambda ts: hourly if ts < sdac_ts else quarter_hourly
        )

        prices_series: pd.Series = pd.Series(prices_list)
        prices_series = prices_series.astype('float64')
        prices_series = prices_series.div(1_000)
        prices_series = prices_series.mul(100)
        prices_series = prices_series.round(3)
        
        end_ts_series: pd.Series = start_ts_series + resolution_series

        data: dict[str, pd.Series] = {
            'start_ts': start_ts_series,
            'day_ahead': prices_series
        }

        df: pd.DataFrame = pd.DataFrame(data)

        df = df.set_index('start_ts')

        mask: pd.Series = (df.index >= self.start_ts) & (df.index < self.end_ts)
        df = df.loc[mask]

        return df

    def _interpolate_idx(self, df: pd.DataFrame) -> pd.DataFrame:
        idx: pd.DatetimeIndex = pd.date_range(
            start=self.start_ts,
            end=self.end_ts,
            freq='15min',
            inclusive='left',
            name='start_ts'
        )

        combined_idx: pd.DatetimeIndex = df.index.union(idx)

        df = df.reindex(combined_idx)
        df = df.ffill()
        df = df.reindex(idx)

        return df

    def load(self, update_df: pd.DataFrame) -> None:
        filename: str = f'day_ahead_{self.year}.csv'
        file_path: Path = self.BASE_PATH / 'day_ahead' / 'quarter-hour' / filename

        # Step 1: Read the existing file
        # Using a complete file to benchmark max file size
        try:
            df: pd.DataFrame = self._read_file(file_path)
        except FileNotFoundError as _:
            df: pd.DataFrame = update_df

        # Step 2: Modify DataFrame
        df = self._update_df(df, update_df)

        # Step 3: Write DataFrame to file
        self._write_file(df, file_path)
    
    def _read_file(self, file_path: Path) -> pd.DataFrame:
        if not file_path.exists():
            raise FileNotFoundError()
    
        df: pd.DataFrame = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            engine='pyarrow'
        )

        float_cols: list[str] = ['day_ahead']

        df[float_cols] = df[float_cols].apply(pd.to_numeric)
        df[float_cols] = df[float_cols].astype('float64')

        dt_cols: list[str] = ['start_ts', 'end_ts']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        df = df.set_index(dt_cols)

        return df

    def _update_df(self, df: pd.DataFrame, update_df: pd.DataFrame) -> pd.DataFrame:
        df.update(update_df)

        return df

    def _write_file(self, df: pd.DataFrame, file_path: Path) -> None:
        df = self._conform_df(df)

        df = df.reset_index()

        df['start_ts'] = df['start_ts'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['end_ts'] = df['end_ts'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        os.makedirs(file_path.parent, exist_ok=True)

        df.to_csv(
            file_path,
            sep=';',
            decimal=',',
            index=False,
            float_format='%.3f'
        )

    def _conform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        start_ts, end_ts = self._get_year_interval()

        start_ts_idx: pd.DatetimeIndex = pd.date_range(
            start=start_ts,
            end=end_ts,
            freq='15min',
            inclusive='left',
            name='start_ts'
        )

        cols: pd.Index = df.columns
        df = df.reset_index()
        df = df.set_index('start_ts')
        df = df[cols]
        df = df.reindex(start_ts_idx)
        df = df.reset_index()
        df['end_ts'] = df['start_ts'] + pd.DateOffset(minutes=15)
        df = df.set_index(['start_ts', 'end_ts'])

        return df

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now()

    @property
    def year(self) -> int:
        if self.start_ts is None:
            raise ValueError(f'self.start_ts is not set.')

        start_ts: pd.Timestamp = self.start_ts.tz_convert('Europe/Berlin')
        
        return start_ts.year

    def _get_year_interval(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{self.year}-01-01T00:00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)
        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        return start_ts, end_ts

    def _set_interval(self, day: pd.Timestamp) -> None:
        if day.tz is None:
            day = day.tz_localize('Europe/Berlin')
        
        day = day.tz_convert('Europe/Berlin')
        
        self.start_ts = day
        self.end_ts = self.start_ts + pd.DateOffset(days=1)
        self.start_ts = self.start_ts.tz_convert('UTC')
        self.end_ts = self.end_ts.tz_convert('UTC')


def benchmark():
    Benchmark().run()


if __name__ == '__main__':
    benchmark()
