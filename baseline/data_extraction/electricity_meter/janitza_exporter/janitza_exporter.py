from pathlib import Path

import pandas as pd
import datetime as dt

from aggregation import AGGREGATION
from devices import DEVICES
from series import SERIES
from gridvis import GridVisRESTAPI, PROJECTS


class JanitzaExporter:
    def __init__(self):
        self.gridvis = GridVisRESTAPI()
        self.gridvis.set_project(PROJECTS.EP_OM_GridVis)
        self.gridvis.set_api_url('https://192.168.36.77:8080/rest/1')
        self.gridvis.allow_self_signed_certificate(True)

        self.start: dt.datetime | None = None
        self.end: dt.datetime | None = None
        self.devices: list[DEVICES] = []
        self.series: list[SERIES] = []
        self.aggregation: AGGREGATION = AGGREGATION.NONE

        self.path: Path = Path('.')
        self.sep: str = ','
        self.decimal: str = '.'
        self.float_format: str = '%.3f'

        self.filename_include_agg: bool = True
        self.filename_simplify: bool = True

        devices: pd.DataFrame = self.gridvis.get_devices()
        self.devices: pd.DataFrame = devices[devices['type'].isin(['JanitzaUMG604', 'JanitzaUMG508'])]

    ################################################################
    # Public Methods
    ################################################################

    def run(
        self,
        start: dt.datetime,
        end: dt.datetime,
        path: Path | str,
        sep: str = ',',
        decimal: str = '.',
        float_format: str = '%.3f',
        devices: list[DEVICES] | DEVICES = list(DEVICES),
        series: list[SERIES] | SERIES = SERIES.ALL,
        aggregation: AGGREGATION = AGGREGATION.NONE
    ):
        # For default argument
        # if devices == DEVICES.ALL:
        #     devices = []
        #     for device in list(DEVICES):
        #         devices.append(device)

        #     devices.remove(DEVICES.ALL)

        # For DEVICES parameter type
        if isinstance(devices, DEVICES):
            devices = [devices]

        for device in devices:
            print(device)

        self.start = start
        self.end = end
        self.series = series
        self.aggregation = aggregation

        if isinstance(path, str):
            path = Path(path)

        self.path = path
        self.sep = sep
        self.decimal = decimal
        self.float_format = float_format

        for device in devices:
            df: pd.DataFrame = self._retrieve_data(device)
            self._export_data(device, df)

    ################################################################
    # Config Methods
    ################################################################

    def config_filename(
        self,
        include_agg: bool | None = None,
        simplify: bool | None = None
    ):
        """Configures the exporter

        Simplifying the filename:

        Filename always contains the start and end timestamps in ISO 8601 format.
        When simplify is set to True, the exporter will simplify certain ranges, that exactly match
        common time ranges, e.g.:

        Exactly one year: 2024-01-01 00:00+00:00_2025-01-01 00:00+00:00 -> 2024

        Exactly one month: 2024-01-01 00:00+00:00_2024-02-01 00:00+00:00 -> 2024-01

        Simplify is set to True by default.

        :param include_agg: flag whether the aggregation should be included in the filename
        :param simplify: flag whether the filename should be simplified (see above)
        :return: itself so methods can be chained
        """
        if include_agg is not None:
            self.filename_include_agg = include_agg

        if simplify is not None:
            self.filename_simplify = simplify

        return self

    ################################################################
    # Internal Methods
    ################################################################

    def _retrieve_data(self, device: DEVICES) -> pd.DataFrame:
        print(f'Getting data for Janitza #{device.value}...', end='\r')

        _, relative_df = self.gridvis.get_data(device.value, self.start, self.end)
        df: pd.DataFrame = relative_df.drop(columns=['end'])
        df.index = df.index.tz_convert('Europe/Berlin')

        # Needs to be updated after resampling
        end: pd.Series = df.index.to_series(index=df.index)

        if self.aggregation == AGGREGATION.YEAR:
            df = df.resample('YS').sum().round(3)
            end: pd.Series = df.index.to_series(index=df.index)
            end = end + pd.DateOffset(years=1)

        if self.aggregation == AGGREGATION.MONTH:
            df = df.resample('MS').sum().round(3)
            end: pd.Series = df.index.to_series(index=df.index)
            end = end + pd.DateOffset(months=1)

        if self.aggregation == AGGREGATION.DAY:
            df = df.resample('D').sum().round(3)
            end: pd.Series = df.index.to_series(index=df.index)
            end = end + pd.Timedelta(hours=24)

        if self.aggregation == AGGREGATION.NONE:
            end = end + pd.Timedelta(hours=1)

        df.insert(0, 'end', end)

        df.index = df.index.tz_convert('UTC')
        df['end'] = df['end'].dt.tz_convert('UTC')

        print(f'Getting data for Janitza #{device.value}. Done.')

        return df

    def _export_data(self, device: DEVICES, df: pd.DataFrame):
        print(f'Exporting data for Janitza #{device.value}...', end='\r')

        janitza_name: str = self.devices.at[device.value, 'name']

        dest: Path = self.path / janitza_name

        dest.mkdir(parents=True, exist_ok=True)

        filename: str = f'{self._generate_filename(janitza_name)}.csv'
        file_path: Path = dest / filename

        df.to_csv(
            file_path,
            sep=self.sep,
            decimal=self.decimal,
            float_format=self.float_format
        )

        print(f'Exporting data for Janitza #{device.value}. Done.')

    def _generate_filename(self, device_name: str) -> str:
        common_range: str | None = None
        td: dt.timedelta = self.end - self.start

        if self.start.hour == 0:
            if td == dt.timedelta(hours=24):
                common_range = 'd'

        if self.start.day == 1:
            if self.end.day == 1:
                if 28 <= td.days <= 31:
                    common_range = 'm'

        if self.start.month == 1:
            if self.end.month == 1:
                if 364 <= td.days <= 366:
                    common_range = 'Y'

        aggregation_str: str = ''

        if self.aggregation == AGGREGATION.YEAR:
            aggregation_str = '_Y'
        if self.aggregation == AGGREGATION.MONTH:
            aggregation_str = '_m'
        if self.aggregation == AGGREGATION.DAY:
            aggregation_str = '_d'

        # Override aggregation suffix with filename config
        if not self.filename_include_agg:
            aggregation_str = ''

        # Override simplifying time range
        if not self.filename_simplify:
            return f'{device_name}_{self.start}_{self.end}{aggregation_str}'

        if common_range == 'd':
            return f'{device_name}_{self.start.strftime("%Y-%m-%d")}{aggregation_str}'

        if common_range == 'm':
            return f'{device_name}_{self.start.strftime("%Y-%m")}{aggregation_str}'

        if common_range == 'Y':
            return f'{device_name}_{self.start.strftime("%Y")}{aggregation_str}'

        if self.start.time() == dt.time(0) and self.end.time() == dt.time(0):
            return f'{device_name}_{self.start.strftime("%Y-%m-%d")}_{self.end.strftime("%Y-%m-%d")}{aggregation_str}'

        return f'{device_name}_{self.start}_{self.end}{aggregation_str}'
