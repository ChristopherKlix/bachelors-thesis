"""
fabric.py
=========

"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import asyncio

from settings import settings


class MainData:
    def __init__(self):
        self._conn = self._get_db_connection()

    async def get_plants(self):
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                plants.id,
                plants.name,
                -- plants.park_id,
                parks.name AS park_name,
                plants.see_number,
                plants.eeg_key,
                plants.eeg_number,
                plants.report_id,
                plants.nominal_power,
                plants.rated_power,
                plants.commissioning_date,
                plants.feed_in_date,
                plants.country,
                plants.federal_state,
                plants.zip_code,
                plants.longitude,
                plants.latitude,
                plants.elevation,
                plants.azimuth,
                plants.slope,
                plants.controller_name,
                plants.bank,
                plants.om,
                plants.reporting,
                plants.plant_janitza_id,
                -- plants.poi_id,
                -- plants.spv_id,
                -- plants.portfolio_id,
                portfolios.name AS portfolio_name,
                pois.name AS poi_name,
                pois.sel_number AS poi_sel_number,
                pois.san_number AS poi_san_number,
                pois.janitza_id AS poi_janitza_id,
                st.short_name AS station_type_short_name,
                -- go.id AS grid_operator_id,
                go.name AS grid_operator_name,
                go.snb_number AS grid_operator_snb_number,
                got.short_name AS grid_operator_type_name,
                spvs.short_name AS spv_short_name,
                spvs.name AS spv_name
            FROM {plants} plants
            LEFT JOIN {parks}
                ON plants.park_id = parks.id
            LEFT JOIN {portfolios}
                ON plants.portfolio_id = portfolios.id
            LEFT JOIN {pois} pois
                ON plants.poi_id = pois.id
            LEFT JOIN {st} st
                ON pois.station_type_id = st.id
            LEFT JOIN {go} go
                ON pois.grid_operator_id = go.id
            LEFT JOIN {got} got
                ON go.grid_operator_type_id = got.id
            LEFT JOIN {spvs} spvs
                ON plants.spv_id = spvs.id
            ORDER BY name ASC
        ''').format(
            plants=sql.Identifier('main_data_2'),
            parks=sql.Identifier('parks'),
            portfolios=sql.Identifier('portfolios'),
            pois=sql.Identifier('pois'),
            st=sql.Identifier('station_types'),
            go=sql.Identifier('grid_operators'),
            got=sql.Identifier('grid_operator_types'),
            spvs=sql.Identifier('spvs_current')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        float_cols = [
            'nominal_power', 'rated_power',
            'latitude', 'longitude',
            'elevation', 'slope', 'azimuth'
        ]

        df[float_cols] = df[float_cols].astype('float64')

        async def safe_get(pid):
            if pid is None or (isinstance(pid, float) and pd.isna(pid)):
                return None
            return await self.get_janitza_name(int(pid))

        df['plant_janitza_name'] = await asyncio.gather(
            *[safe_get(pid) for pid in df['plant_janitza_id']]
        )
        df['poi_janitza_name'] = await asyncio.gather(
            *[safe_get(pid) for pid in df['poi_janitza_id']]
        )

        df = df.drop(columns=['plant_janitza_id', 'poi_janitza_id'])

        df = df.loc[df['name'] != 'Büttel Batterie']

        column_label_mapping: dict[str, str] = {
            'name': 'Plant Name',
            'park_name': 'Park Name',
            'see_number': 'SEE Number',
            'eeg_number': 'EEG Number',
            'eeg_key': 'EEG Key',
            'report_id': 'Report Name',
            'nominal_power': 'Nominal Power (kWp)',
            'rated_power': 'Rated Power (kWp)',
            'commissioning_date': 'Commissioning Date',
            'feed_in_date': 'Feed-In Date',
            'country': 'Country',
            'federal_state': 'Federal State',
            'zip_code': 'ZIP Code',
            'longitude': 'Longitude',
            'latitude': 'Latitude',
            'elevation': 'Elevation (m)',
            'azimuth': 'Azimuth',
            'slope': 'Slope',
            'controller_name': 'Controller Name',
            'bank': 'Bank',
            'om': 'O&M',
            'reporting': 'Reporting',
            'portfolio_name': 'Ownership Portfolio Name',
            'poi_name': 'POI Name',
            'poi_sel_number': 'POI SEL Number',
            'poi_san_number': 'POI SAN Number',
            'station_type_short_name': 'Station Type Short Name',
            'grid_operator_name': 'Grid Operator Name',
            'grid_operator_snb_number': 'Grid Operator SNB Number',
            'grid_operator_type_name': 'Grid Operator Type',
            'spv_short_name': 'SPV Short Name',
            'spv_name': 'SPV Name',
            'plant_janitza_name': 'Plant Janitza Name',
            'poi_janitza_name': 'POI Janitza Name'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df
    
    async def get_parks(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id,
                name,
                ems_name,
                ems_id
            FROM {parks}
        ''').format(
            parks=sql.Identifier('parks')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        column_label_mapping: dict[str, str] = {
            'name': 'Park Name',
            'ems_name': 'EMS Name',
            'ems_id': 'EMS ID'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df

    async def get_janitzas(self):
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id,
                name,
                description,
                commissioning_date,
                decommissioning_date,
                type_name,
                serial_number,
                connection_string,
                ip_address
            FROM {janitzas}
        ''').format(
            janitzas=sql.Identifier('janitzas')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        column_label_mapping: dict[str, str] = {
            'name': 'Janitza Name',
            'description': 'Description',
            'commissioning_date': 'Commissioning Date',
            'decommissioning_date': 'Decommissioning Date',
            'type_name': 'Type',
            'serial_number': 'Serial Number',
            'connection_string': 'Connection String',
            'ip_address': 'IP Address'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df
    
    async def get_pois(self):
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                pois.id,
                pois.name,
                pois.sel_number,
                pois.san_number,
                pois.rated_power,
                pois.nominal_voltage,
                                            
                -- pois.voltage_level_id,
                vl.name AS voltage_level_name,
                vl.short_name AS voltage_level_short_name,

                -- pois.grid_operator_id,
                go.name AS grid_operator_name,
                go.snb_number AS grid_operator_snb_number,
                go.acer_code AS grid_operator_acer_code,
                got.short_name AS grid_operator_type,

                -- pois.station_type_id,
                st.name AS station_type_name,
                st.short_name AS station_type_short_name,

                -- pois.janitza_id
                j.name AS janitza_name
            FROM {pois} pois
            LEFT JOIN {vl} vl
                ON pois.voltage_level_id = vl.id
            LEFT JOIN {go} go
                ON pois.grid_operator_id = go.id
            LEFT JOIN {got} got
                ON go.grid_operator_type_id = got.id
            LEFT JOIN {st} st
                ON pois.station_type_id = st.id
            LEFT JOIN {j} j
                ON pois.janitza_id = j.id
        ''').format(
            pois=sql.Identifier('pois'),
            vl=sql.Identifier('voltage_levels'),
            go=sql.Identifier('grid_operators'),
            got=sql.Identifier('grid_operator_types'),
            st=sql.Identifier('station_types'),
            j=sql.Identifier('janitzas')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        column_label_mapping: dict[str, str] = {
            'name': 'POI Name',
            'sel_number': 'SEL Number',
            'san_number': 'SAN Number',
            'rated_power': 'Rated Power (kW)',
            'nominal_voltage': 'Nominal Voltage (V)',
            'voltage_level_name': 'Voltage Level Name',
            'voltage_level_short_name': 'Voltage Level Short Name',
            'grid_operator_name': 'Grid Operator Name',
            'grid_operator_snb_number': 'Grid Operator SNB Number',
            'grid_operator_acer_code': 'Grid Operator ACER Code',
            'grid_operator_type': 'Grid Operator Type',
            'station_type_name': 'Station Type Name',
            'station_type_short_name': 'Station Type Short Name',
            'janitza_name': 'Janitza Name'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df

    async def get_spvs(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                id,
                name,
                short_name,
                abr_number,
                incorporation_date,
                dissolution_date,
                legal_form,
                owned
            FROM {spvs}
        ''').format(
            spvs=sql.Identifier('spvs_current')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        column_label_mapping: dict[str, str] = {
            'name': 'SPV Name',
            'short_name': 'SPV Short Name',
            'abr_number': 'ABR Number',
            'incorporation_date': 'Incorporation Date',
            'dissolution_date': 'Dissolution Date',
            'legal_form': 'Legal Form',
            'owned': 'Owned'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        df['SPV Short Name Alternative'] = df['SPV Short Name'].str.replace(
            'SLSI',
            'Sunnic LSI',
            regex=False
        )

        return df

    async def get_grid_operators(self) -> pd.DataFrame:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                go.id,
                go.name,
                go.display_name,
                go.snb_number,
                go.acer_code,
                go.service_start,
                go.service_end,
                got.name AS grid_operator_type_name,
                got.short_name AS grid_operator_type_short_name
            FROM {go} go
            LEFT JOIN {got} got
                ON go.grid_operator_type_id = got.id
        ''').format(
            go=sql.Identifier('grid_operators'),
            got=sql.Identifier('grid_operator_types')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        df = df.set_index('id')

        column_label_mapping: dict[str, str] = {
            'name': 'Grid Operator Name',
            'display_name': 'Grid Operator Display Name',
            'snb_number': 'SNB Number',
            'acer_code': 'ACER Code',
            'service_start': 'Service Start',
            'service_end': 'Service End',
            'grid_operator_type_name': 'Type Name',
            'grid_operator_type_short_name': 'Type Short Name',
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df

    async def get_janitza_name(self, janitza_id: int) -> str:
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                name
            FROM {janitzas}
            WHERE id = {janitza_id}
        ''').format(
            janitzas=sql.Identifier('janitzas'),
            janitza_id=sql.Placeholder('janitza_id')
        )

        janitza_name: str | None = None

        with self._conn.cursor() as cur:
            cur.execute(sql_query, {'janitza_id': janitza_id})
            result = cur.fetchone()
            if result:
                janitza_name = result[0]

        if janitza_name is None:
            raise KeyError(f'No Janitza with janitza_id "{janitza_id}" exists.')

        return janitza_name

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn



class PlantEnergy:
    def __init__(self):
        self._conn = self._get_db_connection()

    async def get(self, agg: str) -> pd.DataFrame:
        if agg != 'month':
            raise NotImplementedError()
        
        sql_query: sql.Composable = sql.SQL('''
            SELECT
                pe.main_data_2_id AS plant_id,
                plants.name AS plant_name,
                plants.see_number AS plant_see_number,
                pe.start_ts,
                pe.end_ts,
                pe.energy_active_supplied,
                pe.energy_active_consumed,
                pe.energy_reactive_fundamental_supplied
                AS energy_reactive_fundamental_capacitive,
                pe.energy_reactive_fundamental_consumed
                AS energy_reactive_fundamental_inductive
            FROM {pe} pe
            LEFT JOIN {plants} plants
                ON pe.main_data_2_id = plants.id
        ''').format(
            pe=sql.Identifier('plant_energy_month'),
            plants=sql.Identifier('main_data_2')
        )

        with self._conn.cursor() as cur:
            cur.execute(sql_query)

            cols = [desc[0] for desc in cur.description]
            df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

        float_cols = [
            'energy_active_supplied',
            'energy_active_consumed',
            'energy_reactive_fundamental_capacitive',
            'energy_reactive_fundamental_inductive'
        ]

        df[float_cols] = df[float_cols].astype('float64')
        df['plant_id'] = df['plant_id'].astype(int)

        dt_cols = [
            'start_ts', 'end_ts'
        ]

        df[dt_cols] = df[dt_cols].apply(pd.to_datetime)

        for col in dt_cols:
            df[col] = df[col].dt.tz_convert('UTC')
            df[f'{col}_europe_berlin'] = df[col].dt.tz_convert('Europe/Berlin')
            df[col] = df[col].dt.tz_localize(None)

        df = df.loc[df['plant_id'] != 494]

        column_label_mapping: dict[str, str] = {
            'plant_name': 'Plant Name',
            'plant_see_number': 'SEE Number',
            'start_ts': 'Start (UTC)',
            'end_ts': 'End (UTC)',
            'start_ts_europe_berlin': 'Start (Europe/Berlin)',
            'end_ts_europe_berlin': 'End (Europe/Berlin)',
            'energy_active_supplied': 'Energy Active Supplied (kWh)',
            'energy_active_consumed': 'Energy Active Consumed (kWh)',
            'energy_reactive_fundamental_capacitive': 'Energy Reactive Fundamental Capacitive (kvarh)',
            'energy_reactive_fundamental_inductive': 'Energy Reactive Fundamental Inductive (kvarh)'
        }

        df = df[column_label_mapping.keys()]

        df = df.rename(columns=column_label_mapping)

        return df

    def _get_db_connection(self):
        conn = psycopg2.connect(settings.db.dsn)
    
        return conn
