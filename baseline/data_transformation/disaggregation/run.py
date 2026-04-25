import os
from pathlib import Path
import random

import numpy as np
import pandas as pd
from yaspin import yaspin


class Benchmark:
    _BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    _JANITZA_ENERGY: Path = _BASE_PATH / 'janitza_energy'
    _PLANT_ENERGY: Path = _BASE_PATH / 'plant_energy'
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
    
    def __init__(self):
        self.plants_df: pd.DataFrame = self._read_plants_main_data()
        self.janitzas: list[str] = self._get_all_janitzas_at_pois()

    def compute_connected_plants_metrics(self) -> None:
        df: pd.DataFrame = pd.DataFrame.from_dict({
            'plant_count': []
        }, orient='columns')
        df.index.name = 'janitza_name'

        for janitza in self.janitzas:
            plants_df: pd.DataFrame = self._get_all_connected_plants(janitza)
            df.loc[janitza] = {'plant_count': len(plants_df.index)}

            if len(plants_df.index) > 4:
                print(f'Janitza "{janitza}" has {len(plants_df.index)} connected plants.')

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

        print(metrics_df)

    def compute_metrics(self) -> None:
        with yaspin(text='Computing Metrics', color='yellow') as spinner:
            os.makedirs(self._METRICS_PATH, exist_ok=True)

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


            file_path: Path = self._STATS_PATH / f'stats.csv'
            df: pd.DataFrame = pd.read_csv(
                file_path
            )
            df = df.set_index('benchmark')

            metrics_df['sum'] = df.sum().round(3)
            metrics_df['mean'] = df.mean().round(3)
            metrics_df['std'] = df.std().round(3)
            metrics_df['cv'] = (df.std() / df.mean() * 100).round(2)
            metrics_df['median'] = df.median().round(3)
            metrics_df['min'] = df.min().round(3)
            metrics_df['max'] = df.max().round(3)

            file_path: Path = self._METRICS_PATH / f'metrics.csv'

            metrics_df.to_csv(file_path)

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
            benchmarks.append(290)
            benchmarks.sort()

            idx: pd.Index = pd.Index(benchmarks, name='janitza_count')

            df: pd.DataFrame = pd.DataFrame.from_dict({
                'ratio': [],
                'read': [],
                'compute': [],
                'load': [],
                'total': []
            }, orient='columns')
            df = df.reindex(idx)

            file_path: Path = self._METRICS_PATH / f'metrics.csv'

            stats_df: pd.DataFrame = pd.read_csv(file_path)
            stats_df = stats_df.set_index('step')

            steps = ['ratio', 'read', 'compute', 'load', 'total']

            for step in steps:
                mean: float = stats_df.at[step, 'mean']
                df[step] = df.index.to_series().mul(mean).round(3)

            file_path = self._METRICS_PATH / f'extrapolation.csv'
            df.to_csv(file_path)

        spinner.ok('✅ ')

    def run(self, n: int) -> None:
        stats_df: pd.DataFrame = pd.DataFrame.from_dict({
            'ratio': [],
            'read': [],
            'compute': [],
            'load': []
        }, orient='columns')
        stats_df.index.name = 'benchmark'

        janitzas: list[str] = random.sample(self.janitzas, n)

        for janitza in janitzas:
            try:
                stats: dict[str, pd.Timedelta] = self.run_disaggregation(janitza)
                stats_df.loc[janitza] = stats
            except Exception as _:
                continue

        self.export_stats(stats_df)

    def export_stats(self, df: pd.DataFrame) -> None:
        filename: str = f'stats.csv'
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

    def run_disaggregation(self, janitza_name: str) -> dict[str, pd.Timedelta]:
        stats: dict[str, pd.Timedelta] = {
            'ratio': pd.Timedelta(0.0),
            'read': pd.Timedelta(0.0),
            'compute': pd.Timedelta(0.0),
            'load': pd.Timedelta(0.0)
        }

        connected_plants: pd.DataFrame = self._get_all_connected_plants(janitza_name)

        print(f'Janitza "{janitza_name}" has {len(connected_plants.index)} plants conneted.')

        with yaspin(text=f'Computing Ratio DataFrame', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            ratio_df: pd.DataFrame = self._compute_ratio_df(janitza_name)
            stats['ratio'] = self._now() - _t
            spinner.ok('✅ ')

        with yaspin(text=f'Loading Janitza Quarter-Hour Time-Series', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            df: pd.DataFrame = self._read_file(janitza_name, 'quarter-hour')
            stats['read'] = self._now() - _t
            spinner.ok('✅ ')

        for plant_id in connected_plants.index:
            with yaspin(text=f'Compute Disaggregation for Plant {plant_id}', color='yellow') as spinner:
                _t: pd.Timestamp = self._now()
                plant_df = df.copy()
                plant_df = plant_df.mul(ratio_df[plant_id], axis=0)
                plant_df = plant_df.round(3)
                stats['compute'] += self._now() - _t

                spinner.ok('✅ ')
            
            with yaspin(text=f'Exporting Plant Energy for Plant {plant_id}', color='yellow') as spinner:
                _t: pd.Timestamp = self._now()
                plant_name: str = connected_plants.at[plant_id, 'name']
                self._write_plant_energy_file(plant_df, plant_name)
                stats['load'] += self._now() - _t

                spinner.ok('✅ ')

        return stats
    
    def _write_plant_energy_file(self, df: pd.DataFrame, plant_name: str) -> None:
        filename: str = f'{plant_name}_{self._YEAR}.csv'
        agg: str = 'quarter-hour'
        file_path: Path = self._PLANT_ENERGY / agg / plant_name / filename

        os.makedirs(file_path.parent, exist_ok=True)

        df = self._conform_df(df)
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

    def _compute_ratio_df(self, janitza_name: str) -> pd.DataFrame:
        connected_plants: pd.DataFrame = self._get_all_connected_plants(janitza_name)

        idx: pd.DatetimeIndex = self._get_full_year_idx(self._YEAR)
        ratio_df: pd.DataFrame = pd.DataFrame(index=idx, columns=connected_plants.index)

        for plant_id in connected_plants.index:
            ratio_df[plant_id] = connected_plants.at[plant_id, 'nominal_power']

        # for ts in ratio_df.index:
        #     for plant_id in connected_plants.index:
        #         commissioning_date: pd.Timestamp = connected_plants.at[plant_id, 'commissioning_date']

        #         if ts.tz_convert('Europe/Berlin') < commissioning_date.tz_localize('Europe/Berlin'):
        #             ratio_df.at[ts, plant_id] = float('nan')

        # ensure both are timezone-aware
        ts_vals: np.ndarray = ratio_df.index.tz_convert('Europe/Berlin').to_numpy()
        comm_vals: np.ndarray = connected_plants['commissioning_date'].dt.tz_localize('Europe/Berlin').to_numpy()

        # create broadcast mask
        mask: np.ndarray = ts_vals[:, None] < comm_vals[None, :]

        # assign 0.0 using mask
        ratio_df[:] = ratio_df.where(~mask, 0.0)

        ratio_df['total_nominal_power'] = ratio_df.sum(axis=1)

        for plant_id in connected_plants.index:
            ratio_df[plant_id] = ratio_df[plant_id].div(ratio_df['total_nominal_power'])

        ratio_df = ratio_df.drop(columns=['total_nominal_power'])

        ratio_df = ratio_df.reset_index()
        ratio_df['end_ts'] = ratio_df['start_ts'] + pd.DateOffset(minutes=15)
        ratio_df = ratio_df.set_index(['start_ts', 'end_ts'])
    
        return ratio_df

    def _get_full_year_idx(self, year: int) -> pd.DatetimeIndex:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{year}-01-01')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)
        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        idx: pd.DatetimeIndex = pd.date_range(
            start=start_ts,
            end=end_ts,
            inclusive='left',
            freq='15min',
            name='start_ts'
        )

        return idx

    def _get_all_connected_plants(self, janitza_name: str) -> pd.DataFrame:
        return self.plants_df[self.plants_df['janitza_name'] == janitza_name].copy()

    def _get_all_janitzas_at_pois(self) -> list[str]:
        janitzas: pd.Series = self.plants_df['janitza_name'].drop_duplicates()

        return janitzas.to_list()

    def _read_plants_main_data(self) -> pd.DataFrame:
        filename: str = 'plants_main_data.csv'
        file_path: Path = Path('.') / filename

        df: pd.DataFrame = pd.read_csv(
            file_path,
            engine='pyarrow'
        )

        df = df.set_index('id')

        dt_cols: list[str] = ['commissioning_date', 'feed_in_date']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        return df

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

    def _get_year_interval(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{self._YEAR}-01-01T00:00:00')
        start_ts = start_ts.tz_localize('Europe/Berlin')
        end_ts: pd.Timestamp = start_ts + pd.DateOffset(years=1)
        start_ts = start_ts.tz_convert('UTC')
        end_ts = end_ts.tz_convert('UTC')

        return start_ts, end_ts

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now()



def run_benchmark_5() -> None:
    # Benchmark().run(n=100)
    # Benchmark().run_disaggregation('SDD41A')

    # Benchmark().compute_metrics()

    # Benchmark().extrapolate_scalability()

    Benchmark().compute_connected_plants_metrics()


if __name__ == '__main__':
    run_benchmark_5()
