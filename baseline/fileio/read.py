import os
from pathlib import Path
import random
from typing import Any
import pandas as pd
import numpy as np


class Benchmark:
    # EXPORT_PATH: Path = Path(r'\\Enerparc\Hamburg\Enerparc\18 OM\1810 Portfolio Analyse\omr_csv_rawdata\CKL_Test')
    EXPORT_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    STATS_PATH: Path = Path(r'stats')

    def __init__(self):
        pass

    def run(self, agg: str, n: int | None = None) -> None:
        MAX: int = 10_000
        MAX: int = 10

        if agg == 'quarter-hour':
            MAX = 1_000
        
        STEPS: list[int] = [1, 2, 5]
        MAGNITUDES: list[int] = [1, 10, 100, 1_000, 10_000]
        benchmarks: list[int] = [
            step * mag
            for mag in MAGNITUDES
            for step in STEPS
        ]
        benchmarks = [run for run in benchmarks if run <= MAX]

        if n is not None:
            benchmarks = [n]

        total_stats: dict[str, Any] = {
            'read': [],
            'parse': [],
            'total': []
        }

        total_stats_df: pd.DataFrame = pd.DataFrame.from_dict(total_stats, orient='columns')
        total_stats_df.index.name = 'benchmark'

        for benchmark in benchmarks:
            print(f'Running benchmark "{benchmark}"...', end='', flush=True)
            stats_df: pd.DataFrame = self.run_benchmark(benchmark, agg)
            totals = stats_df.sum(axis=0).to_dict()
            total_stats_df.loc[benchmark] = totals
            print(f'\rRunning benchmark "{benchmark}" complete', flush=True)

        if agg == 'quarter-hour' and n is None:
            total_stats_df = self.extrapolate(total_stats_df, [2000, 5000, 10000])

        print()
        print(total_stats_df)

        stats_path: Path = self.STATS_PATH
        os.makedirs(stats_path, exist_ok=True)
        stats_file_path: Path = stats_path / f'stats_{agg}.csv'

        if stats_file_path.exists():
            update_df: pd.DataFrame = pd.read_csv(stats_file_path)
            update_df = update_df.set_index('benchmark')
            idx = update_df.index.union(total_stats_df.index)
            update_df = update_df.reindex(idx)
            update_df.update(total_stats_df)
            total_stats_df = update_df

        total_stats_df.to_csv(
            stats_file_path,
            float_format='%.6f'
        )

    def extrapolate(self, agg: str, new_idx: list[int] = [2000, 5000, 10000]) -> None:
        stats_path: Path = self.STATS_PATH
        stats_file_path: Path = stats_path / f'stats_{agg}.csv'

        if not stats_file_path.exists():
            raise FileNotFoundError(f'File {stats_file_path} does not exist.')
        
        df: pd.DataFrame = pd.read_csv(stats_file_path)
        df = df.set_index('benchmark')

        df = df.drop(new_idx, axis=0)

        x: np.ndarray = df.index.to_numpy(dtype=int)
        x_new: np.ndarray = np.array(new_idx, dtype=int)

        out: dict[str, np.ndarray] = {}

        for col in df.columns:
            y: np.ndarray = df[col].to_numpy(dtype=float)
            m, b = np.polyfit(x, y, 1)
            out[col] = m * x_new + b

        extrapolated_df: pd.DataFrame = pd.DataFrame(
            out,
            index=pd.Index(x_new, name='benchmark', dtype=int)
        )

        df = pd.concat([df, extrapolated_df], axis=0)
        
        df.to_csv(
            stats_file_path,
            float_format='%.6f'
        )

    def run_benchmark(self, benchmark: int, agg: str) -> pd.DataFrame:
        stats_df: pd.DataFrame = self.read_files(benchmark, agg)
        cols: list[str] = ['read', 'parse', 'total']

        for col in cols:
            stats_df[col] = stats_df[col].apply(self.parse_timedelta)
            stats_df[col] = stats_df[col].round(6)

        stats_path: Path = self.STATS_PATH / f'stats_{agg}'
        os.makedirs(stats_path, exist_ok=True)
        stats_file_path: Path = stats_path / f'stats_{agg}_{benchmark}.csv'

        stats_df.to_csv(
            stats_file_path,
            float_format='%.6f'
        )

        return stats_df

    def read_file(self, file_path: Path) -> dict[str, Any]:
        stats: dict[str, Any] = {}
        t: pd.Timestamp = self.now()

        df: pd.DataFrame = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            engine='pyarrow'
        )

        stats['read'] = self.now() - t
        t = self.now()

        float_cols: list[str] = [
            'energy_active_supplied',
            'energy_active_consumed',
            'energy_reactive_fundamental_supplied',
            'energy_reactive_fundamental_consumed'
        ]

        df[float_cols] = df[float_cols].apply(pd.to_numeric)
        df[float_cols] = df[float_cols].astype('float64')

        dt_cols: list[str] = ['start_ts', 'end_ts']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        df = df.set_index(dt_cols)
        stats['parse'] = self.now() - t

        return stats

    def read_files(self, n: int, agg: str) -> pd.DataFrame:
        stats: dict[str, Any] = {
            'read': [],
            'parse': [],
            'total': []
        }

        stats_df: pd.DataFrame = pd.DataFrame.from_dict(stats, orient='columns')
        stats_df.index.name = 'file'

        path: Path = self.EXPORT_PATH / 'export' / f'export_{agg}'

        files: list[Path] = list(path.glob('*.csv'))
        subset: list[Path] = random.choices(files, k=n)

        for file in subset:
            t: pd.DataFrame = self.now()
            file_stats: dict[str, Any] = self.read_file(file)
            file_stats['total'] = self.now() - t
            stats_df.loc[len(stats_df)] = file_stats

        return stats_df

    def parse_timedelta(self, td: pd.Timedelta) -> float:
        td = td.value
        td = td / 1_000_000_000
        return td

    def now(self) -> pd.Timestamp:
        return pd.Timestamp.now()
    

def main():
    # Benchmark().run('year')
    # Benchmark().run('month')
    # Benchmark().run('day')
    # Benchmark().run('hour')
    Benchmark().run('quarter-hour', 100)
    # Benchmark().run('quarter-hour', 500)
    # Benchmark().run('quarter-hour', 1000)
    # Benchmark().extrapolate('quarter-hour')


if __name__ == '__main__':
    main()
