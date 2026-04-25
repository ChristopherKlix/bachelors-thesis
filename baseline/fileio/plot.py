from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from scipy.interpolate import interp1d


class BenchmarkPlot:
    STATS_PATH: Path = Path(r'stats')
    Y_MAX: int = 60*60 + 1
    Y_LIM: int = Y_MAX + 60
    X_LIM: int = 10_000 + 1_000
    X_LIM_LOG: int = 20_000
    FIG_SIZE: tuple[int, int] = (16, 8)
    PLOT_DPI: int = 300
    COLORS_5: dict[str, str] = {
        '1': '#003f5c',
        '2': '#58508d',
        '3': '#bc5090',
        '4': '#ff6361',
        '5': '#ffa600'
    }

    COLORS_8: dict[str, str] = {
        '1': '#003f5c',
        '2': '#2f4b7c',
        '3': '#665191',
        '4': '#a05195',
        '5': '#d45087',
        '6': '#f95d6a',
        '7': '#ff7c43',
        '8': '#ffa600'
    }
    
    def __init__(self):
        matplotlib.rcParams['font.family'] = 'sans-serif'
        matplotlib.rcParams['font.sans-serif'] = ['Latin Modern Sans']
        matplotlib.rcParams['font.serif'] = ['Latin Modern Roman']
        matplotlib.rcParams['font.monospace'] = ['Latin Modern Mono']

    def run(self) -> None:
        print(f'-' * 64)
        print(f'Plotting: all (log)')
        print()

        aggs: list[str] = ['year', 'month', 'day', 'hour', 'quarter-hour']

        def load_agg(agg: str) -> pd.Series:
            df: pd.DataFrame = self.load_stats(agg)

            series: pd.Series = df['total']
            series.name = agg

            return series
        
        series: list[pd.Series] = [load_agg(agg) for agg in aggs]

        df: pd.DataFrame = pd.DataFrame(series)
        df = df.T

        print(df)

        self.plot_all_log(df)

        print(f'-' * 64)
        print(f'Plotting: all (linear)')
        print()

        df = self.interpolate(df, 10_000, 500)

        print(df)

        self.plot_all_linear(df)

    def interpolate(self, df: pd.DataFrame, xmax: int, interval: int) -> pd.DataFrame:
        new_idx: np.ndarray = np.linspace(0, xmax, int(xmax/interval)+1)
        df_interp = pd.DataFrame(index=new_idx, columns=df.columns, dtype=float)

        for col in df.columns:
            f = interp1d(df.index, df[col], kind='linear', fill_value='extrapolate')
            df_interp[col] = f(new_idx)

        df_interp = df_interp.round(6)

        return df_interp

    def run_agg(self, agg: str) -> None:
        print(f'-' * 64)
        print(f'Plotting: {agg} (log)')
        print()

        df: pd.DataFrame = self.load_stats(agg)
        df = df.rename(columns={
            'read': 'Parsing',
            'parse': 'Deserialization',
            'total': 'Total (including overhead)'
        })

        df = df.drop(columns='Total (including overhead)')

        print(df)

        self.plot_agg_log(df, agg)

        print(f'-' * 64)
        print(f'Plotting: {agg} (linear)')
        print()

        df = self.interpolate(df, 10_000, 500)

        print(df)

        self.plot_agg_linear(df, agg)

    def plot_agg_log(self, df: pd.DataFrame, agg: str) -> None:
        colors = {
            'Parsing': '#58508d',
            'Deserialization': '#bc5090',
            'Total (including overhead)': '#003f5c'
        }

        markers = {
            'Parsing': 'o',
            'Deserialization': 's',
            'Total (including overhead)': 'D'
        }

        linestyles = {
            'Parsing': '-',
            'Deserialization': '-',
            'Total (including overhead)': '-.'
        }

        fig, ax = plt.subplots(figsize=self.FIG_SIZE)

        for col in df.columns:
            ax.plot(
                df.index,
                df[col],
                label=col,
                color=colors.get(col, 'black'),
                marker=markers.get(col, 'o'),
                linestyle=linestyles.get(col, '-')
            )

        df_max: float = df.max().max()
        y_tick_major: int = 60
        y_tick_minor: int = 30

        while df_max / y_tick_major > 10:
            y_tick_major *= 2
            y_tick_minor *= 2

        y_max: int = int((int(df_max / y_tick_major) + 1) * y_tick_major) + 1
        y_lim: int = int((int(df_max / y_tick_major) + 2) * y_tick_major) + 1

        print(f'Max Value: {df_max}    Ymax: {y_max}    Ylim: {y_lim}    Y Tick Major: {y_tick_major}    Y Tick Minor: {y_tick_minor}')

        ax.set_xscale('log')
        ax.set_xticks(df.index)
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
        ax.tick_params(axis='x', which='minor', length=3, color='gray')

        ax.set_yticks(range(0, 1001, 100))
        ax.set_yticks(range(0, 1001, 50), minor=True)
        ax.set_yticks(range(0, y_max, y_tick_major))
        ax.set_yticks(range(0, y_max, y_tick_minor), minor=True)
        ax.tick_params(axis='y', which='minor', length=3, color='gray')

        ax.set_ylim(0, y_lim)
        ax.set_xlim(right=self.X_LIM_LOG)
        ax.set_xlabel('Plant Count')
        ax.set_ylabel('Time (seconds)')

        ax.set_title(
            f'Benchmark Timings for "{agg.capitalize()}" Aggregation File Read',
            fontsize=16,
            fontweight='bold',
            pad=30
        )

        ax.axvline(
            x=600,
            color='#ff6361',
            linestyle='--',
            linewidth=1.5,
            label='System Plant Count'
        )

        ax.grid(True, which='major', ls='--', alpha=0.7)
        ax.grid(True, which='minor', ls=':', alpha=0.4)
        ax.legend()

        plt.savefig(f'benchmark_{agg}_log.png', dpi=self.PLOT_DPI)

    def plot_agg_linear(self, df: pd.DataFrame, agg: str) -> None:
        colors = {
            'Parsing': '#58508d',
            'Deserialization': '#bc5090',
            'Total (including overhead)': '#003f5c'
        }

        markers = {
            'Parsing': 'o',
            'Deserialization': 's',
            'Total (including overhead)': 'D'
        }

        linestyles = {
            'Parsing': '-',
            'Deserialization': '-',
            'Total (including overhead)': '-.'
        }

        fig, ax = plt.subplots(figsize=self.FIG_SIZE)

        for col in df.columns:
            ax.plot(
                df.index,
                df[col],
                label=col,
                color=colors.get(col, 'black'),
                marker=markers.get(col, 'o'),
                linestyle=linestyles.get(col, '-')
            )

        df_max: float = df.max().max()
        y_tick_major: int = 60
        y_tick_minor: int = 30

        while df_max / y_tick_major > 10:
            y_tick_major *= 2
            y_tick_minor *= 2

        y_max: int = int((int(df_max / y_tick_major) + 1) * y_tick_major) + 1
        y_lim: int = int((int(df_max / y_tick_major) + 2) * y_tick_major) + 1

        print(f'Max Value: {df_max}    Ymax: {y_max}    Ylim: {y_lim}    Y Tick Major: {y_tick_major}    Y Tick Minor: {y_tick_minor}')

        ax.set_xscale('linear')

        # X ticks
        ax.xaxis.set_major_locator(plt.MultipleLocator(1000))
        ax.xaxis.set_major_formatter(plt.ScalarFormatter())
        ax.xaxis.set_minor_locator(plt.MultipleLocator(100))
        ax.tick_params(axis='x', which='minor', length=3, color='gray')

        # Y ticks
        ax.set_yticks(range(0, y_max, y_tick_major))
        ax.set_yticks(range(0, y_max, y_tick_minor), minor=True)
        ax.tick_params(axis='y', which='minor', length=3, color='gray')
        ax.set_ylim(bottom=0, top=y_lim)
        ax.set_xlim(left=0, right=self.X_LIM)

        ax.set_xlabel('Plant Count')
        ax.set_ylabel('Time (seconds)')

        ax.set_title(
            f'Benchmark Timings for "{agg.capitalize()}" Aggregation File Read',
            fontsize=16,
            fontweight='bold',
            pad=30
        )

        ax.axvline(
            x=600,
            color='#ff6361',
            linestyle='--',
            linewidth=1.5,
            label='System Plant Count'
        )

        ax.grid(True, which='major', ls='--', alpha=0.7)
        ax.grid(True, which='minor', ls=':', alpha=0.4)
        ax.legend()

        plt.savefig(f'benchmark_{agg}_linear.png', dpi=self.PLOT_DPI)

    def plot_all_log(self, df: pd.DataFrame) -> None:
        colors = {
            'year': self.COLORS_5.get('1'),
            'month': self.COLORS_5.get('2'),
            'day': self.COLORS_5.get('3'),
            'hour': self.COLORS_5.get('4'),
            'quarter-hour': self.COLORS_5.get('5'),
        }

        marker: str = 's'
        markers = {
            'year': marker,
            'month': marker,
            'day': marker,
            'hour': marker,
            'quarter-hour': marker
        }

        linestyle: str = '-'
        linestyles = {
            'year': linestyle,
            'month': linestyle,
            'day': linestyle,
            'hour': linestyle,
            'quarter-hour': linestyle
        }

        fig, ax = plt.subplots(figsize=self.FIG_SIZE)

        for col in df.columns:
            ax.plot(
                df.index,
                df[col],
                label=col,
                color=colors.get(col, 'black'),
                marker=markers.get(col, 'o'),
                linestyle=linestyles.get(col, '-')
            )

        df_max: float = df.max().max()
        y_tick_major: int = 60
        y_tick_minor: int = 30

        while df_max / y_tick_major > 10:
            y_tick_major *= 2
            y_tick_minor *= 2

        y_max: int = int((int(df_max / y_tick_major) + 1) * y_tick_major) + 1
        y_lim: int = int((int(df_max / y_tick_major) + 2) * y_tick_major) + 1

        ax.set_xscale('log')
        ax.set_xticks(df.index)
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
        ax.tick_params(axis='x', which='minor', length=3, color='gray')

        ax.set_yticks(range(0, y_max, y_tick_major))
        ax.set_yticks(range(0, y_max, y_tick_minor), minor=True)
        ax.tick_params(axis='y', which='minor', length=3, color='gray')

        ax.set_ylim(0, y_lim)
        ax.set_xlim(right=self.X_LIM_LOG)
        ax.set_xlabel('Plant Count')
        ax.set_ylabel('Time (seconds)')

        ax.set_title(
            f'Benchmark Timings for All Aggregations File Read (Total)',
            fontsize=16,
            fontweight='bold',
            pad=30
        )

        ax.axvline(
            x=600,
            color='#ff6361',
            linestyle='--',
            linewidth=1.5,
            label='System Plant Count'
        )

        ax.grid(True, which='major', ls='--', alpha=0.7)
        ax.grid(True, which='minor', ls=':', alpha=0.4)
        ax.legend()

        plt.savefig(f'benchmark_all_total_log.png', dpi=self.PLOT_DPI)

    def plot_all_linear(self, df: pd.DataFrame) -> None:
        colors = {
            'year': self.COLORS_5.get('1'),
            'month': self.COLORS_5.get('2'),
            'day': self.COLORS_5.get('3'),
            'hour': self.COLORS_5.get('4'),
            'quarter-hour': self.COLORS_5.get('5'),
        }

        marker: str = 's'
        markers = {
            'year': marker,
            'month': marker,
            'day': marker,
            'hour': marker,
            'quarter-hour': marker
        }

        linestyle: str = '-'
        linestyles = {
            'year': linestyle,
            'month': linestyle,
            'day': linestyle,
            'hour': linestyle,
            'quarter-hour': linestyle
        }

        fig, ax = plt.subplots(figsize=self.FIG_SIZE)

        for col in df.columns:
            ax.plot(
                df.index,
                df[col],
                label=col,
                color=colors.get(col, 'black'),
                marker=markers.get(col, 'o'),
                linestyle=linestyles.get(col, '-')
            )
        
        df_max: float = df.max().max()
        y_tick_major: int = 60
        y_tick_minor: int = 30

        while df_max / y_tick_major > 10:
            y_tick_major *= 2
            y_tick_minor *= 2

        y_max: int = int((int(df_max / y_tick_major) + 1) * y_tick_major) + 1
        y_lim: int = int((int(df_max / y_tick_major) + 2) * y_tick_major) + 1

        ax.set_xscale('linear')

        # X ticks
        ax.xaxis.set_major_locator(plt.MultipleLocator(1000))
        ax.xaxis.set_major_formatter(plt.ScalarFormatter())
        ax.xaxis.set_minor_locator(plt.MultipleLocator(100))
        ax.tick_params(axis='x', which='minor', length=3, color='gray')

        # Y ticks
        ax.set_yticks(range(0, y_max, y_tick_major))
        ax.set_yticks(range(0, y_max, y_tick_minor), minor=True)
        ax.tick_params(axis='y', which='minor', length=3, color='gray')
        ax.set_ylim(bottom=0, top=y_lim)
        ax.set_xlim(left=0, right=self.X_LIM)

        ax.set_xlabel('Plant Count')
        ax.set_ylabel('Time (seconds)')

        ax.set_title(
            f'Benchmark Timings for All Aggregations File Read (Total)',
            fontsize=16,
            fontweight='bold',
            pad=30
        )

        ax.axvline(
            x=600,
            color='#ff6361',
            linestyle='--',
            linewidth=1.5,
            label='System Plant Count'
        )

        ax.grid(True, which='major', ls='--', alpha=0.7)
        ax.grid(True, which='minor', ls=':', alpha=0.4)
        ax.legend()

        plt.savefig(f'benchmark_all_total_linear.png', dpi=self.PLOT_DPI)

    def load_stats(self, agg: str) -> pd.DataFrame:
        filename: str = f'stats_{agg}.csv'
        file_path: Path = self.STATS_PATH / filename

        df: pd.DataFrame = pd.read_csv(file_path)

        float_cols: list[str] = ['read', 'parse', 'total']
        int_cols: list[str] = ['benchmark']

        df[float_cols] = df[float_cols].astype('float64')
        df[int_cols] = df[int_cols].astype('int64')

        df = df.set_index('benchmark')

        return df


def main():
    BenchmarkPlot().run_agg('year')
    BenchmarkPlot().run_agg('month')
    BenchmarkPlot().run_agg('day')
    BenchmarkPlot().run_agg('hour')
    BenchmarkPlot().run_agg('quarter-hour')
    BenchmarkPlot().run()


if __name__ == '__main__':
    main()
