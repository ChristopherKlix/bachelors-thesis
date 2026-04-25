import os
from pathlib import Path
import random

import pandas as pd
from yaspin import yaspin


class Benchmark:
    _BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    _JANITZA_ENERGY: Path = _BASE_PATH / 'janitza_energy'
    _FILEIO: Path = _BASE_PATH / 'fileio'

    _STATS_PATH: Path = Path(r'stats')
    _METRICS_PATH: Path = Path(r'metrics')

    _YEAR: int = 2025

    _FLOAT_COLS: list[str] = [
        'active_energy_supplied',
        'active_energy_consumed',
        'reactive_energy_fundamental_capacitive',
        'reactive_energy_fundamental_inductive'
    ]
    _DT_COLS: list[str] = ['start_ts', 'end_ts']
    _AGGS: list[str] = ['quarter-hour', 'hour', 'day', 'month', 'year']
    
    def __init__(self):
        self.janitzas: dict[str, list[str]] = {
            agg: [] for agg in self._AGGS
        }

    def run(self, n: int) -> None:
        with yaspin(text=f'Setting up benchmark', color='yellow') as spinner:
            self.setup()
            spinner.ok('✅ ')

        for agg in self._AGGS:
            with yaspin(text=f'Running benchmark {agg}', color='yellow') as spinner:
                stats_df: pd.DataFrame = self.run_agg(agg, n)
                self.export_stats(stats_df, agg)
                spinner.ok('✅ ')

    def compute_metrics(self) -> None:
        os.makedirs(self._METRICS_PATH, exist_ok=True)

        with yaspin(text=f'Computing Metrics for "READ"', color='yellow') as spinner:
            self.compute_metrics_mode('read')
            spinner.ok('✅ ')

        with yaspin(text=f'Computing Metrics for "WRITE"', color='yellow') as spinner:
            self.compute_metrics_mode('write')
            spinner.ok('✅ ')

    def compute_metrics_mode(self, mode: str) -> None:
        all_stats: dict[str, pd.DataFrame] = {
            agg: [] for agg in self._AGGS
        }
        
        for agg in all_stats.keys():
            file_path: Path = self._STATS_PATH / f'stats_{agg}.csv'
            df: pd.DataFrame = pd.read_csv(file_path)
            df = df.set_index('janitza')
            all_stats[agg] = df

        metrics_df: pd.DataFrame = pd.DataFrame.from_dict({
            'sum': [],
            'mean': [],
            'std': [],
            'cv': [],
            'median': [],
            'min': [],
            'max': []
        }, orient='columns')
        metrics_df.index.name = 'aggregation'

        agg_series: dict[str, pd.Series] = {}

        for agg in all_stats.keys():
            agg_series[agg] = all_stats[agg].reset_index()[mode]

        stats_df: pd.DataFrame = pd.DataFrame.from_dict(agg_series)

        metrics_df['sum'] = stats_df.sum().round(3)
        metrics_df['mean'] = stats_df.mean().round(3)
        metrics_df['std'] = stats_df.std().round(3)
        metrics_df['cv'] = (stats_df.std() / stats_df.mean() * 100).round(2)
        metrics_df['median'] = stats_df.median().round(3)
        metrics_df['min'] = stats_df.min().round(3)
        metrics_df['max'] = stats_df.max().round(3)

        file_path: Path = self._METRICS_PATH / f'metrics_{mode}.csv'

        metrics_df.to_csv(file_path)
    
    def compute_extrapolation(self) -> None:
        os.makedirs(self._METRICS_PATH, exist_ok=True)

        with yaspin(text=f'Computing Extrapolation for "READ"', color='yellow') as spinner:
            self.compute_extrapolation_mode('read')
            spinner.ok('✅ ')

        with yaspin(text=f'Computing Extrapolation for "WRITE"', color='yellow') as spinner:
            self.compute_extrapolation_mode('write')
            spinner.ok('✅ ')
    
    def compute_extrapolation_mode(self, mode: str) -> None:
        MAX: int = 10_000
        STEPS: list[int] = [1, 2, 5]
        MAGNITUDES: list[int] = [1, 10, 100, 1_000, 10_000]
        benchmarks: list[int] = [
            step * mag
            for mag in MAGNITUDES
            for step in STEPS
        ]
        benchmarks = [benchmark for benchmark in benchmarks if benchmark <= MAX]
        benchmarks.append(452)
        benchmarks.append(290)
        benchmarks.append(665)
        benchmarks.sort()

        file_path: Path = self._METRICS_PATH / f'metrics_{mode}.csv'
        metrics_df: pd.DataFrame = pd.read_csv(file_path)
        metrics_df = metrics_df.set_index('aggregation')

        idx: pd.Index = pd.Index(benchmarks, name='file_count')

        df: pd.DataFrame = pd.DataFrame.from_dict({
            agg: [] for agg in metrics_df.index
        }, orient='columns')
        df = df.reindex(idx)

        for agg in metrics_df.index:
            mean: float = metrics_df.at[agg, 'mean']
            df[agg] = df.index.to_series().mul(mean).round(3)

        file_path = self._METRICS_PATH / f'extrapolation_{mode}.csv'
        df.to_csv(file_path)
    
    def run_agg(self, agg: str, n: int) -> pd.DataFrame:
        stats_df: pd.DataFrame = pd.DataFrame.from_dict({
            'read': [],
            'write': []
        }, orient='columns')
        stats_df.index.name = 'janitza'

        janitzas: list[str] = random.sample(self.janitzas[agg], n)

        for janitza in janitzas:
            stats: dict[str, pd.Timedelta] = {}
            _t: pd.Timestamp = self._now()
            df: pd.DataFrame = self._read_file(janitza, agg)
            stats['read'] = self._now() - _t

            _t = self._now()
            self._write_file(df, janitza, agg)
            stats['write'] = self._now() - _t

            stats_df.loc[janitza] = stats
        
        return stats_df

    def export_stats(self, df: pd.DataFrame, agg: str) -> None:
        filename: str = f'stats_{agg}.csv'
        file_path: Path = self._STATS_PATH / filename

        def parse_timedelta(td: pd.Timedelta) -> float:
            td = td.value
            td = td / 1_000_000_000
            return td
        
        df['total'] = df.sum(axis=1)

        for col in df.columns:
            df[col] = df[col].apply(parse_timedelta)
            df[col] = df[col].round(6)
        
        df = df.reset_index()

        os.makedirs(self._STATS_PATH, exist_ok=True)

        df.to_csv(
            file_path,
            float_format='%.6f',
            index=False
        )

    def setup(self) -> None:
        for agg in self.janitzas.keys():
            self.janitzas[agg] = self._read_janitzas(agg)

    def _read_janitzas(self, agg: str) -> list[str]:
        path: Path = self._JANITZA_ENERGY / agg
        return [p.name for p in list(path.iterdir())]

    def _read_file(self, janitza_name: str, agg: str) -> pd.DataFrame:
        filename: str = f'{janitza_name}_{self._YEAR}.csv'
        file_path: Path = self._JANITZA_ENERGY / agg / janitza_name / filename

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        df: pd.DataFrame = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            engine='pyarrow'
        )

        df[self._FLOAT_COLS] = df[self._FLOAT_COLS].apply(pd.to_numeric)
        df[self._FLOAT_COLS] = df[self._FLOAT_COLS].astype('float64')

        df[self._DT_COLS] = df[self._DT_COLS].apply(pd.to_datetime)

        df = df.set_index(self._DT_COLS)

        return df

    def _write_file(self, df: pd.DataFrame, janitza_name: str, agg: str) -> None:
        filename: str = f'{janitza_name}_{self._YEAR}.csv'
        file_path: Path = self._FILEIO / agg / janitza_name / filename

        os.makedirs(file_path.parent, exist_ok=True)

        df = df.reset_index()

        df['start_ts'] = df['start_ts'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['end_ts'] = df['end_ts'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        df.to_csv(
            file_path,
            sep=';',
            decimal=',',
            index=False,
            float_format='%.3f'
        )

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now()


def run_benchmark_0() -> None:
    Benchmark().run(100)
    Benchmark().compute_metrics()
    Benchmark().compute_extrapolation()


if __name__ == '__main__':
    run_benchmark_0()
