import os
from pathlib import Path
import random

import pandas as pd
from yaspin import yaspin


class Benchmark:
    _BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    _JANITZA_ENERGY: Path = _BASE_PATH / 'janitza_energy'
    _STATS_PATH: Path = Path(r'stats')
    _YEAR: int = 2025
    _FLOAT_COLS: list[str] = [
        'active_energy_supplied',
        'active_energy_consumed',
        'reactive_energy_fundamental_capacitive',
        'reactive_energy_fundamental_inductive'
    ]
    _DT_COLS: list[str] = ['start_ts', 'end_ts']
    _AGG_MAPPING: dict[str, str] = {
        'quarter-hour': '15T',
        'hour': 'h',
        'day': 'D',
        'month': 'ME',
        'year': 'YE'
    }
    _OFFSET_MAPPING: dict[str, pd.DateOffset] = {
        'quarter-hour': pd.DateOffset(minutes=15),
        'hour': pd.DateOffset(hours=1),
        'day': pd.DateOffset(days=1),
        'month': pd.DateOffset(months=1),
        'year': pd.DateOffset(years=1)
    }
    
    def __init__(self):
        self.janitzas: list[str] = []

    def compute_metrics(self) -> None:
        with yaspin(text='Computing Metrics', color='yellow') as spinner:
            aggs = ['hour', 'day', 'month', 'year']

            metrics_path: Path = self._STATS_PATH / 'metrics'

            os.makedirs(metrics_path, exist_ok=True)

            total_metrics_df: pd.DataFrame = pd.DataFrame.from_dict({
                'sum': [],
                'mean': [],
                'std': [],
                'cv': [],
                'median': [],
                'min': [],
                'max': []
            }, orient='columns')
            total_metrics_df.index.name = 'aggregation'

            all_data_df: pd.DataFrame = pd.DataFrame.from_dict({
                'total' :[]
            }, orient='columns')

            for agg in aggs:
                file_path: Path = self._STATS_PATH / f'{agg}.csv'
                df: pd.DataFrame = pd.read_csv(
                    file_path
                )
                df = df.set_index('janitza_name')
                all_data_df = pd.concat([all_data_df, df.reset_index()['total']])
                
                metrics_df: pd.DataFrame = pd.DataFrame.from_dict({
                    'sum': [],
                    'mean': [],
                    'std': [],
                    'cv': [],
                    'median': [],
                    'min': [],
                    'max': []
                }, orient='columns')
                metrics_df.index.name = 'step'

                metrics_df['sum'] = df.sum().round(3)
                metrics_df['mean'] = df.mean().round(3)
                metrics_df['std'] = df.std().round(3)
                metrics_df['cv'] = (df.std() / df.mean() * 100).round(2)
                metrics_df['median'] = df.median().round(3)
                metrics_df['min'] = df.min().round(3)
                metrics_df['max'] = df.max().round(3)

                file_path: Path = metrics_path / f'{agg}.csv'

                metrics_df.to_csv(file_path)
                total_metrics_df.loc[agg] = metrics_df.loc['total']

            all_data_df = all_data_df.reset_index()
            total_metrics_df.loc['total'] = {
                'sum': all_data_df['total'].sum().round(3),
                'mean': all_data_df['total'].mean().round(3),
                'std': all_data_df['total'].std().round(3),
                'cv': (all_data_df['total'].std() / all_data_df['total'].mean() * 100).round(2),
                'median': all_data_df['total'].median().round(3),
                'min': all_data_df['total'].min().round(3),
                'max': all_data_df['total'].max().round(3),
            }

            file_path: Path = metrics_path / f'total.csv'
            total_metrics_df.to_csv(file_path)

            spinner.ok('✅ ')

    def extrapolate_scalability(self) -> None:
        with yaspin(text='Computing Extrapolation', color='yellow') as spinner:
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
            benchmarks.append(452)
            benchmarks.sort()

            idx: pd.Index = pd.Index(benchmarks, name='plant_count')

            df: pd.DataFrame = pd.DataFrame.from_dict({
                'hour': [],
                'day': [],
                'month': [],
                'year': []
            }, orient='columns')
            df = df.reindex(idx)

            metrics_path: Path = self._STATS_PATH / 'metrics'
            file_path: Path = metrics_path / f'total.csv'

            stats_df: pd.DataFrame = pd.read_csv(file_path)
            stats_df = stats_df.set_index('aggregation')

            aggs = ['hour', 'day', 'month', 'year']

            for agg in aggs:
                mean: float = stats_df.at[agg, 'mean']
                df[agg] = df.index.to_series().mul(mean).round(3)

            df['total'] = df.sum(axis=1).round(3)

            file_path = metrics_path / f'extrapolation.csv'
            df.to_csv(file_path)

        spinner.ok('✅ ')

    def run(self, n: int) -> None:
        with yaspin(text='Loading Source Janitzas', color='yellow') as spinner:
            self.janitzas = self._get_all_available_janitzas()
            janitzas: list[str] = random.sample(self.janitzas, n)

            spinner.ok('✅ ')

        aggs = ['hour', 'day', 'month', 'year']

        _t: pd.Timestamp = self._now()

        for agg in aggs:
            with yaspin(text=f'Aggregating {agg}', color='yellow') as spinner:
                stats_df: pd.DataFrame = self.run_aggregation(janitzas, agg)
                self.export_stats(stats_df, agg)

                spinner.ok('✅ ')

        print(f'Total Time: {self._now() - _t}')

    def run_aggregation(self, janitzas: list[str], agg: str) -> pd.DataFrame:
        stats_df: pd.DataFrame = pd.DataFrame.from_dict({
            'extract': [],
            'transform': [],
            'load': []
        }, orient='columns')
        stats_df.index.name = 'janitza_name'

        for janitza in janitzas:
            stats_df.loc[janitza] = self.aggregate_janitza(janitza, agg)

        return stats_df

    def aggregate_janitza(self, janitza_name: str, agg: str) -> dict[str, pd.Timedelta]:
        stats: dict[str, pd.Timedelta] = {}
        source_agg: str = self._get_source_agg(agg)

        # Extract
        _t: pd.Timestamp = self._now()
        df: pd.DataFrame = self._read_file(janitza_name, source_agg)
        stats['extract'] = self._now() - _t

        # Transform
        _t = self._now()
        df = df.reset_index()
        df = df.drop(columns=['end_ts'])

        df['start_ts'] = df['start_ts'].dt.tz_convert('Europe/Berlin')
        df = df.set_index('start_ts')
        
        agg_df: pd.DataFrame = df.resample(self._AGG_MAPPING[agg]).sum()

        agg_df = agg_df.reset_index()

        if agg in ['quarter-hour', 'hour']:
            agg_df['start_ts'] = agg_df['start_ts'].dt.tz_convert('UTC')
            agg_df['end_ts'] = agg_df['start_ts'] + self._OFFSET_MAPPING[agg]
        else:
            agg_df['end_ts'] = agg_df['start_ts'] + self._OFFSET_MAPPING[agg]
            agg_df['start_ts'] = agg_df['start_ts'].dt.tz_convert('UTC')
            agg_df['end_ts'] = agg_df['end_ts'].dt.tz_convert('UTC')

        agg_df = agg_df.set_index(self._DT_COLS)
        stats['transform'] = self._now() - _t

        # Load
        _t = self._now()
        self._write_file(agg_df, janitza_name, agg)
        stats['load'] = self._now() - _t

        return stats

    def export_stats(self, df: pd.DataFrame, agg: str) -> None:
        filename: str = f'{agg}.csv'
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

    def _get_next_agg(self, agg: str) -> str:
        aggs: dict[str, str] = {
            'quarter-hour': 'hour',
            'hour': 'day',
            'day': 'month',
            'month': 'year'
        }

        return aggs.get(agg)
    
    def _get_source_agg(self, agg: str) -> str:
        aggs: dict[str, str] = {
            'hour': 'quarter-hour',
            'day': 'hour',
            'month': 'day',
            'year': 'month'
        }

        return aggs.get(agg)

    def _get_all_available_janitzas(self) -> list[str]:
        path: Path = self._JANITZA_ENERGY / 'quarter-hour'

        return [p.name for p in path.iterdir() if p.is_dir(follow_symlinks=False)]

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
        file_path: Path = self._JANITZA_ENERGY / agg / janitza_name / filename

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

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now()


def run_benchmark_4() -> None:
    # Benchmark().run(n=100)

    # Benchmark().compute_metrics()

    Benchmark().extrapolate_scalability()


if __name__ == '__main__':
    run_benchmark_4()
