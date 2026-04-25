"""
tasks.py
========

"""

# Standard library
import asyncio
from typing import Any

# Third-party
import pandas as pd
import psycopg2
from psycopg2 import sql

# Local modules
from market_price_data import MarketPriceData
from ingress_janitza import Janitza, JanitzaEnergy, PlantEnergy
from ingress_negative_hours import NegativeHours
from jhe import JHE

from settings import settings


def log(*args, **kwargs) -> None:
    print(f'INFO:     ', end='', flush=True)
    print(*args, **kwargs, flush=True)


async def job_day_ahead():
    now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')
    next_run: pd.Timestamp = (now + pd.Timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    log(f'Registering job: "day ahead" - next run: {next_run}')

    await asyncio.sleep((next_run - now).total_seconds())

    while True:
        await _run_job_market_prices()
        
        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')
        next_run: pd.Timestamp = (now + pd.Timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        log(f'Registering job: "day ahead" - next run: {next_run}')

        await asyncio.sleep((next_run - now).total_seconds())


async def _run_job_market_prices():
    log(f'--------------------------------')
    log(f'Running day ahead job')
    log(f'--------------------------------')
    market = MarketPriceData()

    try:
        stats: dict[str, Any] = await market.start_ingress('tomorrow')
    except Exception as e:
        log(f'Error in day ahead job: {e}')
        log(f'--------------------------------')
        return

    log(f'\033[92mCompleted day ahead job.\033[0m')
    log(f'--------------------------------')
    log(f'Task summary')
    log(f'--------------------------------')
    log(f'Inserts: {stats['inserts']}')
    log(f'Updates: {stats['updates']}')
    log(f'--------------------------------')
    log(f'Time request: {stats['time_request']}')
    log(f'Time write: {stats['time_write']}')
    log(f'--------------------------------')
    log(f'Time total: {stats['time_total']}')
    log(f'--------------------------------')


async def job_energy():
    MINUTE: int = 15

    now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

    next_run: pd.Timestamp = (now + pd.Timedelta(hours=1)).replace(
        minute=15, second=0, microsecond=0
    )

    if now.minute < (MINUTE - 1):
        next_run = now.replace(minute=15, second=0, microsecond=0)

    log(f'Registering job: "energy"    - next run: {next_run}')

    await asyncio.sleep((next_run - now).total_seconds())

    while True:
        try:
            await _run_job_energy()
        except Exception as e:
            log(f'\033[91mError in Janitza Update Job:\033[0m\n{e}')

        now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')
        next_run: pd.Timestamp = (now + pd.Timedelta(hours=1)).replace(
            minute=15, second=0, microsecond=0
        )
        log(f'Registering job: "energy"    - next run: {next_run}')
        
        await asyncio.sleep((next_run - now).total_seconds())


async def _run_job_energy():
    start_time: pd.Timestamp = pd.Timestamp.now('UTC')
    total_stats: dict[str, pd.Timestamp | pd.Timedelta] = {
        'start_ts': start_time
    }

    log(f'--------------------------------')
    log(f'Running Janitza Update Job')
    log(f'--------------------------------')
    
    janitza = Janitza()
    janitza_energy = JanitzaEnergy()
    plant_energy = PlantEnergy()

    stats: dict[str, Any] = {
        'time_ingress_yesterday': None,
        'time_ingress_today': None,
        'time_update_last_three_hours': None,
        'time_update_yesterday': None,
        'inserts_last_three_hours': None,
        'updates_last_three_hours': None,
        'inserts_yesterday': None,
        'updates_yesterday': None
    }

    await janitza.start_ingress_devices()

    # Ingest data from yesterday at 12 AM, 1 AM, and 2 AM
    if pd.Timestamp.now('Europe/Berlin').hour < 3:
        ingress_stats: dict[str, Any] = await janitza.start_ingress('yesterday')
        stats['time_ingress_yesterday'] = ingress_stats['total_time']

    _t: pd.Timestamp = pd.Timestamp.now()
    ingress_stats: dict[str, Any] = await janitza.start_ingress('today')
    stats['time_ingress_today'] = ingress_stats['total_time']

    update_stats: dict[str, Any] = await janitza_energy.update(
        interval='last_three_hours'
    )
    total_stats['data_extraction-electricity_meter'] = pd.Timestamp.now() - _t

    stats['time_update_last_three_hours'] = update_stats['update_time']
    stats['inserts_last_three_hour'] = update_stats['inserts']
    stats['updates_last_three_hour'] = update_stats['updates']

    # Update data from yesterday at 12 AM, 1 AM, and 2 AM
    if pd.Timestamp.now('Europe/Berlin').hour < 3:
        update_stats: dict[str, Any] = await janitza_energy.update(
            interval='yesterday'
        )
        update_stats: dict[str, Any] = await plant_energy.update(
            interval='today'
        )
        stats['time_update_yesterday'] = update_stats['update_time']
        stats['inserts_yesterday'] = update_stats['inserts']
        stats['updates_yesterday'] = update_stats['updates']

    total_time: pd.Timedelta = stats['time_ingress_today']
    total_time += stats['time_update_last_three_hours']

    if stats['time_ingress_yesterday'] is not None:
        total_time += stats['time_ingress_yesterday']

    if stats['time_update_yesterday'] is not None:
        total_time += stats['time_update_yesterday']

    log(f'--------------------------------')
    log(f'\033[92mCompleted Janitza update job.\033[0m')
    log(f'--------------------------------')
    log(f'Task summary')
    log(f'--------------------------------')
    log(f'- INGRESS ----------------------')
    log(f'--------------------------------')
    log(f'Time ingress yesterday:       {stats['time_ingress_yesterday']}')
    log(f'Time ingress today:           {stats['time_ingress_today']}')
    log(f'--------------------------------')
    log(f'- UPDATE -----------------------')
    log(f'--------------------------------')
    log(f'Inserts yesterday:            {stats['inserts_yesterday']}')
    log(f'Updates yesterday:            {stats['updates_yesterday']}')
    log(f'--------------------------------')
    log(f'Inserts last three hours:     {stats['inserts_last_three_hours']}')
    log(f'Updates last three hours:     {stats['updates_last_three_hours']}')
    log(f'--------------------------------')
    log(f'Time update yesterday:        {stats['time_update_yesterday']}')
    log(f'Time update last three hours: {stats['time_update_last_three_hours']}')
    log(f'--------------------------------')
    log(f'Time total:                   {total_time}')
    log(f'--------------------------------')

    log(f'Updating plant_energy \'this_week\'')
    await plant_energy.update('this_week')

    log(f'Updating plant_energy \'today\'')
    _t = pd.Timestamp.now()
    await plant_energy.update('today')
    total_stats['data_transformation-plant_energy'] = pd.Timestamp.now() - _t

    log(f'Updating negative_hours_energy \'today\'')
    _t = pd.Timestamp.now()
    negative_hours = NegativeHours()
    await negative_hours.update('today')
    total_stats['data_transformation-negative_hours_energy'] = pd.Timestamp.now() - _t

    
    log(f'Updating max_power_active_consumed')
    await JHE().update_max_power_active_consumed()






    log(f'--------------------------------')
    log(f'Updating janitza_energy aggregation cache')
    log(f'--------------------------------')

    from janitza_energy_aggregations import JanitzaEnergyAggregations

    rule_hour_include_last_month: bool = False

    rule_day_include_last_month: bool = False

    rule_month_include_last_year: bool = False

    now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

    # Update hour and day energy for the previous month once a day at midnight
    if now.hour < 1:
        rule_hour_include_last_month = True
        rule_day_include_last_month = True

    # Update month energy for the previous year once a day at midnight in January
    if now.month == 1 and now.hour < 1:
        rule_month_include_last_year = True

    _t = pd.Timestamp.now()
    await JanitzaEnergyAggregations().start_auto_update(
        rule_hour_include_last_month=rule_hour_include_last_month,
        rule_day_include_last_month=rule_day_include_last_month,
        rule_month_include_last_year=rule_month_include_last_year
    )
    total_stats['data_transformation-janitza_energy_aggregations'] = pd.Timestamp.now() - _t







    log(f'--------------------------------')
    log(f'Updating plant_energy aggregation cache')
    log(f'--------------------------------')

    from plant_energy_aggregations import PlantEnergyAggregations

    rule_hour_include_last_month: bool = False

    rule_day_include_last_month: bool = False

    rule_month_include_last_year: bool = False

    now: pd.Timestamp = pd.Timestamp.now('Europe/Berlin')

    # Update hour and day energy for the previous month once a day at midnight
    if now.hour < 1:
        rule_hour_include_last_month = True
        rule_day_include_last_month = True

    # Update month energy for the previous year once a day at midnight in January
    if now.month == 1 and now.hour < 1:
        rule_month_include_last_year = True

    _t = pd.Timestamp.now()
    await PlantEnergyAggregations().start_auto_update(
        rule_hour_include_last_month=rule_hour_include_last_month,
        rule_day_include_last_month=rule_day_include_last_month,
        rule_month_include_last_year=rule_month_include_last_year
    )
    total_stats['data_transformation-plant_energy_aggregations'] = pd.Timestamp.now() - _t







    

    log(f'--------------------------------')
    log(f'Updating BLE cache')
    log(f'--------------------------------')
    log(f'Updating ble_negative_hours_energy_day')

    from ble import BLE

    await BLE().update_ble_negative_hours_energy_cache()

    log(f'Updating update_ble_day_ahead_cache')

    await BLE().update_ble_day_ahead_cache()

    log(f'--------------------------------')
    log(f'Done.')
    log(f'--------------------------------')
    total_stats['end_ts'] = pd.Timestamp.now('UTC')

    log_stats(total_stats)


def log_stats(stats: dict[str, pd.Timedelta]) -> None:
    sql_query: sql.Composable = sql.SQL('''
        INSERT INTO {pipeline_logs} (
            start_ts,
            end_ts,
            data_extraction_electricity_meter,
            data_transformation_plant_energy,
            data_transformation_negative_hours_energy,
            data_transformation_janitza_energy_aggregations,
            data_transformation_plant_energy_aggregations
        ) VALUES (
            %(start_ts)s,
            %(end_ts)s,
            %(data_extraction_electricity_meter)s,
            %(data_transformation_plant_energy)s,
            %(data_transformation_negative_hours_energy)s,
            %(data_transformation_janitza_energy_aggregations)s,
            %(data_transformation_plant_energy_aggregations)s
        )
    ''').format(
        pipeline_logs=sql.Identifier('pipeline_logs')
    )

    params: dict = {
        k.replace('-', '_'): (
            v.to_pytimedelta() if hasattr(v, 'to_pytimedelta')
            else v.to_pydatetime() if isinstance(v, pd.Timestamp)
            else v
        )
        for k, v in stats.items()
    }

    conn = _get_db_connection()

    with conn.cursor() as cur:
        cur.execute(sql_query, params)
    
    conn.commit()


def _get_db_connection():
    conn = psycopg2.connect(settings.db.dsn)

    return conn
