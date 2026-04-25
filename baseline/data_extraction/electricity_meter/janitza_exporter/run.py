from pathlib import Path
import random
import os
from typing import Any, Callable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import pandas as pd
from scipy import interpolate

from gridvis import GridVisRESTAPI
from gridvis import VALUES, TYPES, TIMEBASES, PROJECTS
from devices import DEVICES


class Benchmark:
    BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    STATS_PATH: Path = Path('stats')

    ALL_COLS_GRIDVIS: list[str] = [
        'ActiveEnergySupplied_min_Wh', 'ActiveEnergySupplied_max_Wh',
        'ActiveEnergySupplied_avg_Wh', 'ActiveEnergyConsumed_min_Wh',
        'ActiveEnergyConsumed_max_Wh', 'ActiveEnergyConsumed_avg_Wh',
        'ReactiveEnergyCap_min_varh', 'ReactiveEnergyCap_max_varh',
        'ReactiveEnergyCap_avg_varh', 'ReactiveEnergyInd_min_varh',
        'ReactiveEnergyInd_max_varh', 'ReactiveEnergyInd_avg_varh'
    ]

    COLS_MAPPING: dict[str, str] = {
        'ActiveEnergySupplied_avg_Wh': 'active_energy_supplied',
        'ActiveEnergyConsumed_avg_Wh': 'active_energy_consumed',
        'ReactiveEnergyCap_avg_varh': 'reactive_energy_fundamental_capacitive',
        'ReactiveEnergyInd_avg_varh': 'reactive_energy_fundamental_inductive'
    }

    COLS_SIGNS: dict[str, str] = {
        'active_energy_supplied': '-',
        'active_energy_consumed': '+',
        'reactive_energy_fundamental_capacitive': '-',
        'reactive_energy_fundamental_inductive': '+'
    }

    def __init__(self):
        self.gridvis = GridVisRESTAPI()
        self.gridvis.set_project(PROJECTS.EP_OM_GridVis)
        self.gridvis.set_api_url('https://192.168.36.77:8080/rest/1')
        self.gridvis.allow_self_signed_certificate(True)

        self.start_ts: pd.Timestamp | None = None
        self.end_ts: pd.Timestamp | None = None

    @property
    def year(self) -> int:
        if self.start_ts is None:
            raise ValueError(f'self.start_ts is not set.')

        start_ts: pd.Timestamp = self.start_ts.tz_convert('Europe/Berlin')
        
        return start_ts.year
    
    @property
    def DEVICE_POOL(self) -> list[DEVICES]:
        devices: list[DEVICES] = list(DEVICES)
        
        # Devices with known issues
        exclude_issues: list[DEVICES] = [
            DEVICES.NRU12A,
            DEVICES.PAR41C,
            DEVICES.PAR41B,
            DEVICES.MVL41A,
            DEVICES.MVL21A,
            DEVICES.MVL21B,
            DEVICES.MVL21C,
            DEVICES.MVL21D,
            DEVICES.MVL21E,
            DEVICES.SWS_Kompensationsstation,
            DEVICES.OSL21B,
            DEVICES.MUB0211A,
            DEVICES.CRI21A,
            DEVICES.CRI21B,
            DEVICES.CRI21C,
            DEVICES.CRI21D,
            DEVICES.AKZ11A,
            DEVICES.WEG11A
        ]

        devices = [d for d in devices if d not in exclude_issues]

        # Devices that were commissioning after 2025-01-01
        exclude_commissioning_date: list[DEVICES] = [
            DEVICES.SWS_Kompensationsstation,
            DEVICES.UND41C,
            DEVICES.UND41B,
            DEVICES.GVS11,
            DEVICES.DME41A,
            DEVICES.WST41C,
            DEVICES.EGG0411A,
            DEVICES.EGG0412A,
            DEVICES.EDE11A,
            DEVICES.DME41B,
            DEVICES.DME41C,
            DEVICES.ASB11A,
            DEVICES.SLA31A,
            DEVICES.CAC11A,
            DEVICES.WDE21A,
            DEVICES.WDE21B,
            DEVICES.LAO11A,
            DEVICES.OSL21B,
            DEVICES.OSL21A,
            DEVICES.MUB0211A,
            DEVICES.ATT21A,
            DEVICES.CRI21A,
            DEVICES.CRI21B,
            DEVICES.CRI21C,
            DEVICES.CRI21D,
            DEVICES.CRI11E,
            DEVICES.AKZ11A
        ]

        devices = [d for d in devices if d not in exclude_commissioning_date]

        return devices

    def benchmark_etl(self, model: str, n: int | None = None) -> None:
        day: pd.Timestamp = pd.Timestamp.fromisoformat(f'2025-12-31')

        stats_df: pd.DataFrame = pd.DataFrame.from_dict({
            'extract': [],
            'transform': [],
            'load': []
        }, orient='columns')
        stats_df.index.name = 'benchmark'

        MAX: int = 100
        STEPS: list[int] = [1, 2, 5]
        MAGNITUDES: list[int] = [1, 10, 100, 1_000]
        benchmarks: list[int] = [
            step * mag
            for mag in MAGNITUDES
            for step in STEPS
        ]
        benchmarks = [benchmark for benchmark in benchmarks if benchmark <= MAX]

        if n is not None:
            benchmarks = [n]

        def get_device_list(n: int) -> list[DEVICES]:
            devices: list[DEVICES] = []

            for i in range(n):
                devices.append(self.DEVICE_POOL[i % len(self.DEVICE_POOL)])
            
            return devices
        
        benchmark_funcs: dict[str, Callable] = {
            'sequential': self.run_sequential_etl,
            'threaded': self.run_threaded_etl,
            'hybrid': self.run_hybrid_etl
        }
        
        for benchmark in benchmarks:
            print(f'Running benchmark "{benchmark}"...', end='', flush=True)
            devices: list[DEVICES] = get_device_list(benchmark)

            stats: dict[str, pd.Timedelta] = benchmark_funcs.get(model)(
                devices=devices,
                day=day
            )

            stats_df.loc[benchmark] = stats
            print(f'\rRunning benchmark "{benchmark}" complete', flush=True)
        
        def parse_timedelta(td: pd.Timedelta) -> float:
            td = td.value
            td = td / 1_000_000_000
            return td
        
        stats_df['total'] = stats_df.sum(axis=1)

        for col in stats_df.columns:
            stats_df[col] = stats_df[col].apply(parse_timedelta)
            stats_df[col] = stats_df[col].round(6)
        
        stats_df = stats_df.reset_index()

        os.makedirs(self.STATS_PATH, exist_ok=True)

        filename: str = f'stats_{model}_etl.csv'
        file_path: Path = self.STATS_PATH / filename

        print(stats_df)
        print(f'Total Time: {stats_df.sum()['total']}')

        stats_df.to_csv(
            file_path,
            float_format='%.6f',
            index=False
        )
    
    def extrapolate_stats(self) -> None:
        MAX: int = 10_000
        STEPS: list[int] = [1, 2, 5]
        MAGNITUDES: list[int] = [1, 10, 100, 1_000, 10_000]
        benchmarks: list[int] = [
            step * mag
            for mag in MAGNITUDES
            for step in STEPS
        ]
        benchmarks = [benchmark for benchmark in benchmarks if benchmark <= MAX]
        benchmarks.append(665)
        benchmarks.append(600)
        benchmarks.append(452)
        benchmarks.sort()

        idx: pd.Index = pd.Index(benchmarks, name='benchmark')

        models: list[str] = ['sequential', 'threaded', 'hybrid']

        for model in models:
            filename: str = f'stats_{model}.csv'
            file_path: Path = Path('benchmarks') / 'benchmark_2' / model / filename
            df: pd.DataFrame = pd.read_csv(file_path)
            df = df.set_index('benchmark')

            # df = df.reindex(df.index.union(idx)).sort_index()
            # df = df.interpolate(method='index').loc[idx]

            x: np.ndarray = df.index.to_numpy()
            xn: np.ndarray = np.array(idx)

            extrapolated_df: pd.DataFrame = pd.DataFrame(index=idx)

            for c in df.columns:
                y: np.ndarray = df[c].to_numpy()
                f = interpolate.interp1d(x, y, kind='linear', fill_value='extrapolate')
                extrapolated_df[c] = f(xn)

            filename = f'stats_{model}_extrapolated.csv'
            file_path = Path('benchmarks') / 'benchmark_2' / model / filename
            extrapolated_df = extrapolated_df.round(3)
            extrapolated_df.to_csv(file_path, float_format='%.3f')

    def run_sequential_etl(
            self,
            devices: list[DEVICES],
            day: pd.Timestamp
        ) -> dict[str, pd.Timedelta]:
        total_stats: dict[str, pd.Timedelta] = {
            'extract': pd.Timedelta(0),
            'transform': pd.Timedelta(0),
            'load': pd.Timedelta(0)
        }

        for device in devices:
            stats: dict[str, pd.Timedelta] = self.run_individually(device, day, verbose=False)

            total_stats = {k: total_stats[k] + stats[k] for k in total_stats}

        return total_stats

    def run_threaded_etl(
            self,
            devices: list[DEVICES],
            day: pd.Timestamp
        ) -> pd.Timedelta:
        _t: pd.Timestamp = self._now()

        def _run(device: DEVICES) -> None:
            self.run_individually(device, day, verbose=False)

        with ThreadPoolExecutor() as pool:
            list(pool.map(_run, devices))

        return self._now() - _t

    def run_hybrid_etl(self, devices: list[DEVICES], day: pd.Timestamp) -> pd.Timedelta:
        _t: pd.Timestamp = self._now()

        chunks: list[list[DEVICES]] = [devices[i::4] for i in range(4)]

        with ProcessPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(Benchmark._run_chunk, chunk, day) for chunk in chunks]

            for f in futures:
                f.result()

        return self._now() - _t
    
    @classmethod
    def _run_chunk(cls, devices: list[DEVICES], day: pd.Timestamp) -> pd.Timedelta:
        return cls().run_threaded_etl(devices, day)

    def run_device_for_full_year(
            self,
            device: DEVICES,
            year: int,
            verbose: bool = True,
            n: int | None = None
        ) -> pd.DataFrame:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-01-01')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)

        days: list[pd.Timestamp] = pd.date_range(
            start=start_ts,
            end=end_ts,
            inclusive='left',
            freq='D'
        ).to_list()

        stats: dict[str, Any] = {
            'extract': [],
            'transform': [],
            'load': []
        }

        stats_df: pd.DataFrame = pd.DataFrame.from_dict(stats, orient='columns')
        stats_df.index.name = 'benchmark'

        if n is not None:
            n = n if n <= len(days) else len(days)
            days = days[:n]

        for day in days:
            day_stats: dict[str, pd.Timedelta] = self.run_individually(
                device=device,
                day=day,
                verbose=False
            )
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

        filename: str = f'stats_{device.name}_{year}.csv'
        file_path: Path = self.STATS_PATH / filename

        if verbose:
            print(stats_df)
            print(f'Total Time: {stats_df.sum()['total']}')

        stats_df.to_csv(
            file_path,
            float_format='%.6f',
            index=False
        )

        return stats_df

    def run_individually(
            self,
            device: DEVICES,
            day: pd.Timestamp,
            verbose: bool = True
        ) -> dict[str, pd.Timedelta]:
        self._set_interval(day)

        stats: dict[str, Any] = {}
        
        if verbose:
            print(f'=' * 64)
            print(f'Benchmark: {device}')
            print(f'Day:       {day.date()}')
            print(f'=' * 64)
            print('EXTRACT')
            print(f'-' * 64)
            print(f'Running...', end='', flush=True)

        _t: pd.Timestamp = self._now()
        df: pd.DataFrame = self.extract(device)
        stats['extract'] = self._now() - _t

        if verbose:
            print(f'\rDone. {stats['extract']}', flush=True)
            print(f'-' * 64)
            print('TRANSFORM')
            print(f'-' * 64)
            print(f'Running...', end='', flush=True)

        _t = self._now()
        df = self.transform(df)
        stats['transform'] = self._now() - _t

        if verbose:
            print(f'\rDone. {stats['transform']}', flush=True)
            print(f'-' * 64)
            print('LOAD')
            print(f'-' * 64)
            print(f'Running...', end='', flush=True)

        _t = self._now()
        self.load(df, device)
        stats['load'] = self._now() - _t

        if verbose:
            print(f'\rDone. {stats['load']}', flush=True)
            print(f'=' * 64)
            print(f'Total: {stats['extract'] + stats['transform'] + stats['load']}')
            print(f'=' * 64)

        return stats
    
    def setup_full_year_fast(self, device: DEVICES, year: int) -> None:
        print(f'=' * 64)
        print(f'Setup: {device}')
        print(f'Year:  {year}')

        _t: pd.Timestamp = self._now()
        
        # for month in range(1, 13):
        #     print(f'=' * 64)
        #     print(f'Running {year}-{month}...', end='', flush=True)
        #     start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-{month:02}-01')
        #     start_ts = start_ts.tz_localize('Europe/Berlin')

        #     self.start_ts = start_ts
        #     self.end_ts = self.start_ts + pd.DateOffset(months=1)
        #     self.start_ts = self.start_ts.tz_convert('UTC')
        #     self.end_ts = self.end_ts.tz_convert('UTC')

        #     print(f'\r\033[KRunning {year}-{month} Extract...', end='', flush=True)
        #     df: pd.DataFrame = self.extract(device)

        #     print(f'\r\033[KRunning {year}-{month} Transform...', end='', flush=True)
        #     df = self.transform(df)

        #     print(f'\r\033[KRunning {year}-{month} Load...', end='', flush=True)
        #     self.load(df, device)
            
        #     print(f'\r\033[KRunning {year}-{month} completed.', flush=True)
        #     print(f'-' * 64)

        print(f'=' * 64)
        print(f'Running {year}...', end='', flush=True)

        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-01-01')
        start_ts = start_ts.tz_localize('Europe/Berlin')

        self.start_ts = start_ts
        self.end_ts = self.start_ts + pd.DateOffset(years=1)
        self.start_ts = self.start_ts.tz_convert('UTC')
        self.end_ts = self.end_ts.tz_convert('UTC')

        print(f'\r\033[KRunning {year} Extract...', end='', flush=True)
        df: pd.DataFrame = self.extract(device)

        print(f'\r\033[KRunning {year} Transform...', end='', flush=True)
        df = self.transform(df)

        print(f'\r\033[KRunning {year} Load...', end='', flush=True)
        self.load(df, device)
        
        print(f'\r\033[KRunning {year} completed.', flush=True)
        print(f'-' * 64)

        print(f'Setup completed.')
        print(f'Total Time: {self._now() - _t}')
        print(f'=' * 64)

    def extract(self, device: DEVICES) -> pd.DataFrame:
        active_energy_supplied_df: pd.DataFrame = self.gridvis.hist_values(
            p_id=device.value,
            p_value=VALUES.ActiveEnergySupplied,
            p_type=TYPES.SUM13,
            p_timebase=TIMEBASES.HOUR,
            start=self.start_ts,
            end=self.end_ts + pd.DateOffset(hours=1)
        )

        active_energy_consumed_df: pd.DataFrame = self.gridvis.hist_values(
            p_id=device.value,
            p_value=VALUES.ActiveEnergyConsumed,
            p_type=TYPES.SUM13,
            p_timebase=TIMEBASES.HOUR,
            start=self.start_ts,
            end=self.end_ts + pd.DateOffset(hours=1)
        )

        reactive_energy_fundamental_capacitive_df: pd.DataFrame = self.gridvis.hist_values(
            p_id=device.value,
            p_value=VALUES.ReactiveEnergyCap,
            p_type=TYPES.SUM13,
            p_timebase=TIMEBASES.HOUR,
            start=self.start_ts,
            end=self.end_ts + pd.DateOffset(hours=1)
        )

        reactive_energy_fundamental_inductive_df: pd.DataFrame = self.gridvis.hist_values(
            p_id=device.value,
            p_value=VALUES.ReactiveEnergyInd,
            p_type=TYPES.SUM13,
            p_timebase=TIMEBASES.HOUR,
            start=self.start_ts,
            end=self.end_ts + pd.DateOffset(hours=1)
        )

        df: pd.DataFrame = pd.concat([
            active_energy_supplied_df,
            active_energy_consumed_df,
            reactive_energy_fundamental_capacitive_df,
            reactive_energy_fundamental_inductive_df
        ], axis=1, join='outer', sort=True)

        df = df.sort_index()

        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._map_columns(df)

        # Round Wh values to full integers
        # Keep it as float for interpolation
        df = df.round(0)

        df = self._interpolate_idx(df)

        df = self._compute_energy(df)

        df = self._set_sign(df)
        
        # Convert Wh to kWh
        df = df.div(1_000)

        # Round to Wh resolution
        df = df.round(3)

        return df
    
    def load(self, update_df: pd.DataFrame, device: DEVICES) -> None:
        filename: str = f'{device.name}_{self.year}.csv'
        file_path: Path = self.BASE_PATH / 'janitza_energy' / 'quarter-hour' / device.name / filename

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
    
    def _set_interval(self, day: pd.Timestamp) -> None:
        if day.tz is None:
            day = day.tz_localize('Europe/Berlin')
        
        day = day.tz_convert('Europe/Berlin')
        
        self.start_ts = day
        self.end_ts = self.start_ts + pd.DateOffset(days=1)
        self.start_ts = self.start_ts.tz_convert('UTC')
        self.end_ts = self.end_ts.tz_convert('UTC')
    
    def _read_file(self, file_path: Path) -> pd.DataFrame:
        if not file_path.exists():
            raise FileNotFoundError()
    
        df: pd.DataFrame = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            engine='pyarrow'
        )

        float_cols: list[str] = list(self.COLS_MAPPING.values())

        df[float_cols] = df[float_cols].apply(pd.to_numeric)
        df[float_cols] = df[float_cols].astype('float64')

        dt_cols: list[str] = ['start_ts', 'end_ts']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        df = df.set_index(dt_cols)

        return df

    def _update_df(self, df: pd.DataFrame, update_df: pd.DataFrame) -> pd.DataFrame:
        df.update(update_df)

        return df

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

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[self.COLS_MAPPING.keys()]
        df = df.rename(columns=self.COLS_MAPPING)

        return df
    
    def _interpolate_idx(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[~df.index.duplicated(keep='last')]

        idx: pd.DatetimeIndex = pd.date_range(
            start=self.start_ts,
            end=self.end_ts,
            freq='15min',
            inclusive='both',
            name='ts'
        )

        combined_idx: pd.DatetimeIndex = df.index.union(idx)

        df = df.reindex(combined_idx)
        df = df.interpolate('time')
        df = df.reindex(idx)

        return df

    def _compute_energy(self, df: pd.DataFrame) -> pd.DataFrame:
        cols: list[str] = list(self.COLS_MAPPING.values())

        df[cols] = df[cols].diff()
        df[cols] = df[cols].shift(-1)

        start_ts_idx: pd.DatetimeIndex = pd.date_range(
            start=self.start_ts,
            end=self.end_ts,
            freq='15min',
            inclusive='left',
            name='start_ts'
        )

        df = df.reindex(start_ts_idx)

        df = df.reset_index()

        df['end_ts'] = df['start_ts'] + pd.DateOffset(minutes=15)

        df = df.set_index(['start_ts', 'end_ts'])

        return df

    def _set_sign(self, df: pd.DataFrame) -> pd.DataFrame:
        cols: list[str] = list(self.COLS_MAPPING.values())

        def apply_sign(col: pd.Series) -> pd.Series:
            sign: str = self.COLS_SIGNS.get(col.name)
            col = col.mul(-1 if sign == '-' else 1)

            return col
        
        df[cols] = df[cols].abs()
        df[cols] = df[cols].apply(apply_sign, axis='index')

        return df

    def _get_year_interval(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{self.year}-01-01T00:00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)
        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        return start_ts, end_ts

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now()


def benchmark_1():
    devices: list[DEVICES] = [
        DEVICES.EHN21A,
        # DEVICES.EHN21B,
        # DEVICES.UWE41A,
        # DEVICES.UWE41B
    ]

    _t: pd.Timestamp = pd.Timestamp.now()

    for device in devices:
        print(f'Running "{device.name}"...', end='', flush=True)

        Benchmark().run_device_for_full_year(device, 2025, verbose=True)

        print(f'\rCompleted "{device.name}"', flush=True)

    print(f'Total Time: {pd.Timestamp.now() - _t}')


def benchmark_2():
    models: list[str] = ['sequential']

    for model in models:
        print(f'Running "{model}"...', end='', flush=True)

        Benchmark().benchmark_etl(model, 50)

        print(f'\rCompleted "{model}"', flush=True)


def setup_benchmark_2():
    devices: list[DEVICES] = list(DEVICES)

    def filter_devices(devices: list[DEVICES], exclude: list[DEVICES]) -> list[DEVICES]:
        return [d for d in devices if d not in exclude]
    
    devices = filter_devices(
        devices, [DEVICES.NRU12A]
    )

    for device in devices:
        try:
            Benchmark().setup_full_year_fast(device, 2025)
        except Exception as _:
            print(f'Error while processing "{device.name}"')


if __name__ == '__main__':
    Benchmark().extrapolate_stats()
