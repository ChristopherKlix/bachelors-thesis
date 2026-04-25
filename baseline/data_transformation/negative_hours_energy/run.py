import os
from pathlib import Path
import random

import pandas as pd
from yaspin import yaspin


class Benchmark:
    _BASE_PATH: Path = Path(r'/mnt/y/18 OM/1810 Portfolio Analyse/omr_csv_rawdata/CKL_Test')
    _PLANT_ENERGY: Path = _BASE_PATH / 'plant_energy'
    _PLANT_NEGATIVE_HOURS_ENERGY: Path = _BASE_PATH / 'plant_negative_hours_energy'
    _DAY_AHEAD: Path = _BASE_PATH / 'day_ahead'
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
        self.day_ahead_df: pd.DataFrame = self._read_day_ahead_time_series_data()

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
            benchmarks.append(665)
            benchmarks.sort()

            idx: pd.Index = pd.Index(benchmarks, name='janitza_count')

            df: pd.DataFrame = pd.DataFrame.from_dict({
                'read': [],
                'compute': [],
                'load': [],
                'total': []
            }, orient='columns')
            df = df.reindex(idx)

            file_path: Path = self._METRICS_PATH / f'metrics.csv'

            stats_df: pd.DataFrame = pd.read_csv(file_path)
            stats_df = stats_df.set_index('step')

            steps = ['read', 'compute', 'load', 'total']

            for step in steps:
                mean: float = stats_df.at[step, 'mean']
                df[step] = df.index.to_series().mul(mean).round(3)

            file_path = self._METRICS_PATH / f'extrapolation.csv'
            df.to_csv(file_path)

        spinner.ok('✅ ')

    def run(self, n: int) -> None:
        stats_df: pd.DataFrame = pd.DataFrame.from_dict({
            'read': [],
            'compute': [],
            'load': []
        }, orient='columns')
        stats_df.index.name = 'benchmark'

        all_plant_ids: list[int] = self.plants_df.index.to_list()
        all_plant_ids = [
            plant_id
            for plant_id
            in all_plant_ids
            if (
                self._PLANT_ENERGY /
                'quarter-hour' /
                self.plants_df.at[plant_id, 'name'] /
                f'{self.plants_df.at[plant_id, 'name']}_{self._YEAR}.csv'
            ).is_file()
        ]

        plant_ids: list[int] = random.sample(all_plant_ids, n)

        for plant_id in plant_ids:
            try:
                stats: dict[str, pd.Timedelta] = self.run_computation(plant_id)
                stats_df.loc[plant_id] = stats
            except FileNotFoundError as _:
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

    def run_computation(self, plant_id: int) -> dict[str, pd.Timedelta]:
        stats: dict[str, pd.Timedelta] = {
            'read': pd.Timedelta(0.0),
            'compute': pd.Timedelta(0.0),
            'load': pd.Timedelta(0.0)
        }

        with yaspin(text=f'Reading Plant Energy Quarter-Hour Time-Series', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            plant_df: pd.DataFrame = self._read_file(plant_id, 'quarter-hour')
            stats['read'] = self._now() - _t

            spinner.ok('✅ ')

        with yaspin(text=f'Computing Negative Hours', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            nh_df: pd.DataFrame = self._compute_negative_hours()
            stats['compute'] += self._now() - _t

            spinner.ok('✅ ')

        with yaspin(text=f'Applying Negative Hours Mask', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            plant_df = self._compute_negative_hours_energy(
                plant_id=plant_id,
                plant_df=plant_df,
                nh_df=nh_df
            )
            stats['compute'] += self._now() - _t

            spinner.ok('✅ ')
            
        with yaspin(text=f'Exporting Plant Negative Hours Energy for Plant {plant_id}', color='yellow') as spinner:
            _t: pd.Timestamp = self._now()
            plant_name: str = self.plants_df.at[plant_id, 'name']
            self._write_plant_negative_hours_energy_file(plant_df, plant_name)
            stats['load'] = self._now() - _t

            spinner.ok('✅ ')

        return stats
    
    def _compute_negative_hours_energy(self, plant_id: int, plant_df: pd.DataFrame, nh_df: pd.DataFrame) -> pd.DataFrame:
        commissioning_date: pd.Timestamp = self.plants_df.at[plant_id, 'commissioning_date']

        rule: float = self._get_rule(
            commissioning_date=commissioning_date,
            year=self._YEAR
        )

        plant_df = plant_df[['active_energy_supplied']]
        plant_df['day_ahead'] = nh_df['day_ahead']
        plant_df['negative_hours'] = nh_df['negative_hours']

        if rule == 0.25:
            plant_df['neg_null'] = plant_df['active_energy_supplied'].where(plant_df['day_ahead'] < 0)
        else:
            plant_df['neg_null'] = plant_df['active_energy_supplied'].where(plant_df['negative_hours'] >= rule)
        
        return plant_df

    def _get_rule(
            self,
            commissioning_date: pd.Timestamp,
            year: int,
            bid_submission_date: pd.Timestamp | None = None,
            is_innausv: bool = False
        ) -> float:
        if is_innausv:
            return 1

        if bid_submission_date is not None:
            bid_submission_date.tz_localize(None)

        commissioning_date = commissioning_date.tz_localize(None)

        relevant_date = bid_submission_date or commissioning_date

        # EEG 2023 13. Änderung (Solarspitzengesetz)
        if relevant_date >= pd.Timestamp.fromisoformat('2025-02-25'):
            return 0.25
        
        # EEG 2023
        if relevant_date >= pd.Timestamp.fromisoformat('2023-01-01'):
            eeg_2023: dict[int, float] = {
                2023: 4,
                2024: 3,
                2025: 3,
                2026: 2
            }

            if year in eeg_2023.keys():
                return eeg_2023[year]
            else:
                return 1
        
        # EEG 2021
        if relevant_date >= pd.Timestamp.fromisoformat('2021-01-01'):
            return 4
        
        # EEG 2017 (from 2016-01-01)
        if relevant_date >= pd.Timestamp.fromisoformat('2017-01-01') and commissioning_date >= pd.Timestamp.fromisoformat('2016-01-01'):
            return 6
        
        # EEG 2014 (from 2016-01-01)
        if relevant_date >= pd.Timestamp.fromisoformat('2017-01-01') and commissioning_date >= pd.Timestamp.fromisoformat('2016-01-01'):
            return 6
        
        # EEG 2017 & 2014 (before 2016-01-01)
        return float('nan')

    def _compute_negative_hours(self) -> pd.DataFrame:
        df: pd.DataFrame = self.day_ahead_df.copy()
        df = df.reset_index()
        df = df.set_index('start_ts')
        df = df.drop(columns=['end_ts'])

        df = df.resample('h').mean()

        neg_mask: pd.Series = df['day_ahead'] < 0

        # group consecutive negatives
        grp = (neg_mask != neg_mask.shift()).cumsum()

        # consecutive negative count (0 if not negative)
        df['consecutive'] = neg_mask.groupby(grp).cumsum() * neg_mask

        # max consecutive count within each negative block
        df['negative_hours'] = df['consecutive'].where(
            neg_mask
        ).groupby(
            grp
        ).transform(
            'max'
        ).fillna(
            0
        ).astype(
            int
        )

        qh_df: pd.DataFrame = self.day_ahead_df.copy()
        qh_df = qh_df.reset_index()
        qh_df = qh_df.set_index('start_ts')

        qh_df[['consecutive']] = qh_df.index.to_series().dt.floor('h').map(
            df['consecutive']
        ).to_frame(name='consecutive')

        qh_df[['negative_hours']] = qh_df.index.to_series().dt.floor('h').map(
            df['negative_hours']
        ).to_frame(name='negative_hours')

        # ensure integer type
        qh_df[['consecutive', 'negative_hours']] = qh_df[
            ['consecutive', 'negative_hours']
        ].fillna(0).astype(int)

        qh_df = qh_df.reset_index()
        qh_df = qh_df.set_index(['start_ts', 'end_ts'])

        return qh_df
    
    def _write_plant_negative_hours_energy_file(self, df: pd.DataFrame, plant_name: str) -> None:
        filename: str = f'{plant_name}_{self._YEAR}.csv'
        agg: str = 'quarter-hour'
        file_path: Path = self._PLANT_NEGATIVE_HOURS_ENERGY / agg / plant_name / filename

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
    
    def _read_day_ahead_time_series_data(self) -> pd.DataFrame:
        filename: str = 'day_ahead_2025.csv'
        agg: str = 'quarter-hour'
        file_path: Path = self._DAY_AHEAD / agg / filename

        df: pd.DataFrame = pd.read_csv(
            file_path,
            sep=';',
            decimal=',',
            engine='pyarrow'
        )

        dt_cols: list[str] = ['start_ts', 'end_ts']

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        df = df.set_index(dt_cols)

        return df

    def _read_file(self, plant_id: int, agg: str) -> pd.DataFrame:
        plant_name: str = self.plants_df.at[plant_id, 'name']
        filename: str = f'{plant_name}_{self._YEAR}.csv'
        file_path: Path = self._PLANT_ENERGY / agg / plant_name / filename

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


def run_benchmark_6() -> None:
    # Benchmark().run(n=100)

    Benchmark().compute_metrics()

    Benchmark().extrapolate_scalability()


if __name__ == '__main__':
    run_benchmark_6()
