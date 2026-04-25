from pathlib import Path
import pandas as pd


file_path: Path = Path ('benchmarks/benchmark_3/benchmark_3.csv')

df: pd.DataFrame = pd.read_csv(file_path)

df = df.set_index('benchmark')

stats_df: pd.DataFrame = pd.DataFrame.from_dict({
    'extract': [],
    'transform': [],
    'load': [],
    'total': []
}, orient='index')
stats_df.index.name = 'step'

stats_df['sum'] = df.sum()
stats_df['mean'] = df.mean()
stats_df['std'] = df.std()
stats_df['cv'] = stats_df['std'].div(stats_df['mean'])
stats_df['median'] = df.median()
stats_df['min'] = df.min()
stats_df['max'] = df.max()

stats_df = stats_df.round(3)

print(stats_df)

stats_df.to_csv('benchmarks/benchmark_3/stats_benchmark_3.csv')
