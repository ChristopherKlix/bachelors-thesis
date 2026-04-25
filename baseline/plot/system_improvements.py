from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from scipy.interpolate import interp1d


matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['font.sans-serif'] = ['Latin Modern Sans']
matplotlib.rcParams['font.serif'] = ['Latin Modern Roman']
matplotlib.rcParams['font.monospace'] = ['Latin Modern Mono']

HEIGHT: int = 5
FIG_SIZE: tuple[int, int] = (10, HEIGHT)
PLOT_DPI: int = 300

COLORS = {
    'Electricity Meter': "#222E50",
    'Day-ahead': "#222E50",
    'Meter Energy Aggregations': "#426B69",
    'Plant Energy Aggregations': "#426B69",
    'Plant Energy Disaggregation': "#8BB174",
    'Negative Hours Energy': "#B5CA8D"
}

BASELINE = {
    'Electricity Meter': pd.Timedelta('00:42:57'),
    'Day-ahead': pd.Timedelta('00:00:05'),
    'Meter Energy Aggregations': pd.Timedelta('00:22:19'),
    'Plant Energy Aggregations': pd.Timedelta('00:32:50'),
    'Plant Energy Disaggregation': pd.Timedelta('00:39:09'),
    'Negative Hours Energy': pd.Timedelta('00:22:11')
}

BEDROCK = {
    'Electricity Meter': pd.Timedelta('00:07:05'),
    'Day-ahead': pd.Timedelta('00:00:05'),
    'Meter Energy Aggregations': pd.Timedelta('00:01:34'),
    'Plant Energy Aggregations': pd.Timedelta('00:01:02'),
    'Plant Energy Disaggregation': pd.Timedelta('00:00:01.8'),
    'Negative Hours Energy': pd.Timedelta('00:00:00.235')
}


def plot(
    data_1: dict[str, pd.Timedelta],
    data_2: dict[str, pd.Timedelta],
    colors: dict[str, str],
    title: str,
    file_path: Path
) -> None:
    ks: list[str] = list(data_1.keys())

    v1: list[float] = [data_1[k].total_seconds() / 60 for k in ks]
    v2: list[float] = [data_2[k].total_seconds() / 60 for k in ks]

    def _fmt(td: pd.Timedelta) -> str:
        s_total: float = td.total_seconds()
        m: int = int(s_total) // 60
        s: int = int(s_total) % 60

        if s_total < 5:
            ms: int = int((s_total - int(s_total)) * 1000)
            return f'{m:02d}:{s:02d}.{ms:03d}'
        return f'{m:02d}:{s:02d}'

    fig, ax = plt.subplots(figsize=FIG_SIZE)

    y: np.ndarray = np.arange(len(ks))
    h: float = 0.35

    bars1 = ax.barh(
        y - h / 2,
        v1,
        height=h,
        color=[colors[k] for k in ks],
        label='Baseline'
    )

    bars2 = ax.barh(
        y + h / 2,
        v2,
        height=h,
        color=[colors[k] for k in ks],
        alpha=0.4,
        label='Bedrock'
    )

    ax.set_yticks(y)
    ax.set_yticklabels(ks)
    ax.invert_yaxis()

    ax.set_title(title, loc='left', fontsize=16, fontweight='bold', pad=30)

    ax.set_xlabel('Runtime (min)', loc='left', labelpad=10)
    ax.xaxis.set_label_position('top')
    ax.xaxis.tick_top()

    mx: int = int(max(v1 + v2)) + 1
    ax.set_xticks(range(0, mx + 1, 1))
    ax.xaxis.grid(True, color='lightgray', linewidth=0.8)
    ax.set_axisbelow(True)

    for s in ['top', 'right', 'left', 'bottom']:
        ax.spines[s].set_visible(False)

    pad: float = 0.1
    min_w: float = 3.0

    # labels series 1
    for i, (v, k) in enumerate(zip(v1, ks)):
        td: pd.Timedelta = data_1[k]
        x: float = pad if v > min_w else v + 0.05

        ax.text(
            x,
            y[i] - h / 2,
            _fmt(td),
            va='center',
            ha='left',
            fontweight='bold',
            color='white' if v > min_w else 'black'
        )
    
    def _legend_y(h: float) -> float:
        return -0.21 * h + 2.16

    # labels series 2
    for i, (v, k) in enumerate(zip(v2, ks)):
        td: pd.Timedelta = data_2[k]
        x: float = pad if v > min_w else v + 0.05

        ax.text(
            x,
            y[i] + h / 2,
            _fmt(td),
            va='center',
            ha='left',
            fontweight='bold',
            color='white' if v > min_w else 'black'
        )

    ax.legend(
        loc='upper left',
        bbox_to_anchor=(0, 1.23),
        ncol=2,
        frameon=False
    )

    plt.subplots_adjust(top=0.62)
    plt.tight_layout()
    plt.savefig(file_path, format='png', dpi=PLOT_DPI)
    plt.close(fig)


plot(
    BASELINE,
    BEDROCK,
    COLORS,
    'Runtimes of The Baseline System vs. The Bedrock System',
    Path('chart_system_improvements.png')
)
