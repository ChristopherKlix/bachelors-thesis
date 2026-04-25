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

HEIGHT: int = 3
FIG_SIZE: tuple[int, int] = (10, HEIGHT)
PLOT_DPI: int = 300

COLORS = {
    'Hour': "#222E50",
    'Day': "#426B69",
    'Month': "#8BB174",
    'Year': "#B5CA8D"
}

METER_ENERGY_AGG = {
    'Hour': pd.Timedelta('00:09:29'),
    'Day': pd.Timedelta('00:04:50'),
    'Month': pd.Timedelta('00:04:04'),
    'Year': pd.Timedelta('00:03:56')
}

PLANT_ENERGY_AGG = {
    'Hour': pd.Timedelta('00:13:57'),
    'Day': pd.Timedelta('00:07:07'),
    'Month': pd.Timedelta('00:05:59'),
    'Year': pd.Timedelta('00:05:47')
}


def plot(
    data: dict[str, pd.Timedelta],
    colors: dict[str, str],
    title: str,
    file_path: Path
) -> None:
    ks: list[str] = list(data.keys())
    vs_min: list[float] = [v.total_seconds() / 60 for v in data.values()]

    def _fmt(td: pd.Timedelta) -> str:
        s: int = int(td.total_seconds())
        m: int = s // 60
        s = s % 60
        return f'{m:02d}:{s:02d}'

    fig, ax = plt.subplots(figsize=FIG_SIZE)

    bars = ax.barh(
        ks,
        vs_min,
        color=[colors[k] for k in ks],
        label=ks
    )

    # order top -> bottom
    ax.invert_yaxis()

    # title + subtitle style spacing
    ax.set_title(
        title,
        loc='left',
        fontsize=16,
        fontweight='bold',
        pad=30
    )

    # axis label above ticks
    ax.set_xlabel('Runtime (min)', loc='left', labelpad=10)
    ax.xaxis.set_label_position('top')
    ax.xaxis.tick_top()

    # grid (vertical minute lines)
    mx: int = int(max(vs_min)) + 1
    ax.set_xticks(range(0, mx + 1, 1))
    ax.xaxis.grid(True, color='lightgray', linewidth=0.8)
    ax.set_axisbelow(True)

    # remove spines for clean look
    for s in ['top', 'right', 'left', 'bottom']:
        ax.spines[s].set_visible(False)

    # value labels at end of bars (MM:SS)
    # for i, (v, td) in enumerate(zip(vs_min, data.values())):
    #     ax.text(
    #         v + 0.05,
    #         i,
    #         _fmt(td),
    #         va='center',
    #         ha='left',
    #         fontweight='bold'
    #     )
    pad: float = 0.1
    min_w: float = 0.6

    for i, (v, td) in enumerate(zip(vs_min, data.values())):
        x: float = pad if v > min_w else v + 0.05
        ha: str = 'left' if v > min_w else 'left'

        ax.text(
            x,
            i,
            _fmt(td),
            va='center',
            ha=ha,
            fontweight='bold',
            color='white' if v > min_w else 'black'
        )

    # legend top-left inline
    legend_y: float = 1.32 + (HEIGHT - 4) * -0.4
    def _legend_y(h: float) -> float:
        return -0.21 * h + 2.16

    ax.legend(
        ks,
        loc='upper left',
        bbox_to_anchor=(0, _legend_y(HEIGHT)),
        ncol=len(ks),
        frameon=False,
        handlelength=1,
        handleheight=1
    )
    plt.subplots_adjust(top=0.62)

    plt.tight_layout()
    plt.savefig(file_path, format='png', dpi=PLOT_DPI)
    plt.close(fig)


plot(
    METER_ENERGY_AGG,
    COLORS,
    'Runtimes of Aggregations for Meter Energy',
    Path('chart_meter_energy_aggregations.png')
)

plot(
    PLANT_ENERGY_AGG,
    COLORS,
    'Runtimes of Aggregations for Plant Energy',
    Path('chart_plant_energy_aggregations.png')
)
