"""
jhe.py
======

"""

# Standard library
import io
import datetime as dt
import re

# Third-party
from typing import Annotated
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request, status
from fastapi.responses import PlainTextResponse
from fastapi.responses import Response
from pydantic import AfterValidator

# Local modules
from auth import _auth
from cache import Cache
from routers.util import Router


router: APIRouter = Router(
    tags=['JHE'],
    prefix='/jhe',
    dependencies=[Depends(_auth)],
    allowed_users=['j.heim']
)


cache = Cache()


def _validator_janitza_energy_aggregation(aggregation: str) -> str:
    available_aggregations: list[str] = ['None', 'hour', 'day', 'month']
    
    if not aggregation in available_aggregations:
        raise ValueError(
            f'Invalid "aggregation" format, it must be one of "{(', '.join(available_aggregations))}"'
        )
    
    return aggregation


def _validator_plant_energy_aggregation(aggregation: str) -> str:
    available_aggregations: list[str] = ['None', 'hour', 'day', 'month', 'year']
    
    if not aggregation in available_aggregations:
        raise ValueError(
            f'Invalid "aggregation" format, it must be one of "{(', '.join(available_aggregations))}"'
        )
    
    return aggregation


def _compute_intervals(start_ts: dt.datetime, end_ts: dt.datetime, aggregation: str) -> int:
    start_ts = pd.Timestamp(start_ts).tz_convert('Europe/Berlin')
    end_ts = pd.Timestamp(end_ts).tz_convert('Europe/Berlin')

    resolution_map: dict[str, dt.timedelta] = {
        'None': pd.Timedelta(minutes=15),
        'hour': pd.Timedelta(hours=1),
        'day': pd.Timedelta(hours=24)
    }

    if aggregation in resolution_map:
        interval_count: int = int((end_ts - start_ts) / resolution_map[aggregation])
    elif aggregation == 'month':
        interval_count: int = (end_ts.year - start_ts.year) * 12 + (end_ts.month - start_ts.month) + 1
    elif aggregation == 'year':
        interval_count: int = end_ts.year - start_ts.year
    else:
        raise ValueError(f'Unsupported aggregation: {aggregation}')

    return interval_count


@router.get(path='/main-data')
async def get_main_data(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from jhe import JHE

    response: str = JHE().get_main_data()

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')


@router.get(path='/mini-bess')
async def get_mini_bess(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from jhe import JHE

    df: pd.DataFrame = JHE().get_mini_bess()

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    return Response(content=response, status_code=200)


@router.get(path='/janitza-energy')
async def get_janitza_energy(
        request: Request,
        user_agent: Annotated[str | None, Header(include_in_schema=False)],
        accept: Annotated[str | None, Header(include_in_schema=False)],
        start_ts: dt.datetime | None = Query(
            None,
            title='Start Timestamp',
            description=(
                'The start of the interval in ISO 8601 format. '
                'If not provided, defaults to the beginning of the year. '
                'Assumes Europe/Berlin timezone if no tz info is given.'
            ),
            example='2025-01-01T00:00:00',
        ),
        end_ts: dt.datetime | None = Query(
            None,
            title='End Timestamp',
            description=(
                'The end of the interval in ISO 8601 format. '
                'If not provided, defaults to now. '
                'Assumes Europe/Berlin timezone if no tz info is given.'
            ),
            example='2025-01-15T00:00:00',
        ),
        aggregation: Annotated[
            str | None,
            Query(
                title='Aggregation',
                description=(
                    'The time interval to aggregate data: "hour", "day", or "month". '
                    'Aggregation is calculated using Europe/Berlin timezone. '
                    'If the last interval extends beyond the end timestamp, it is truncated.'
                ),
                example='day'
            ),
            AfterValidator(_validator_janitza_energy_aggregation)
        ] = None
    ) -> PlainTextResponse:
    user = request.state.user
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    if start_ts is None:
        start_ts = pd.Timestamp.fromisoformat('2025-01-01T00:00:00')
        start_ts = pd.Timestamp(start_ts).tz_localize('Europe/Berlin')
        start_ts = pd.Timestamp(start_ts).tz_convert('UTC')

    if start_ts.tzinfo is None:
        start_ts = pd.Timestamp(start_ts).tz_localize('Europe/Berlin')
        start_ts = pd.Timestamp(start_ts).tz_convert('UTC')

    if end_ts is None:
        end_ts = pd.Timestamp.utcnow()

    if end_ts.tzinfo is None:
        end_ts = pd.Timestamp(end_ts).tz_localize('Europe/Berlin')
        end_ts = pd.Timestamp(end_ts).tz_convert('UTC')

    max_records: int = 2**19
    max_records: int = 2**21

    interval_count = _compute_intervals(
        start_ts=start_ts,
        end_ts=end_ts,
        aggregation=aggregation
    )

    from ingress_janitza import Janitza, JanitzaEnergy

    janitza = Janitza()
    janitza_energy = JanitzaEnergy()
    
    janitzas_count: int = len(janitza.get_devices_df().index)

    max_records_exceed_error_msg: str = f'The requested interval exceeds the maximum allowed number of records of {max_records}. ' \
        f'You requested {interval_count} intervals for {janitzas_count} Janitzas ' \
        f'which equals {interval_count * janitzas_count} records. ' \
        f'Try a smaller interval or lower resolution.'

    if (interval_count * janitzas_count) > max_records:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=max_records_exceed_error_msg,
            headers={'WWW-Authenticate': 'Basic'}
        )

    df: pd.DataFrame = await janitza_energy.get(
        start_ts=start_ts,
        end_ts=end_ts,
        aggregation=aggregation
    )

    df = df.reset_index()

    col_formatter = {
        'energy_active_supplied': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_active_consumed': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_reactive_fundamental_supplied': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_reactive_fundamental_consumed': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ',')
    }

    for col, fn in col_formatter.items():
        df[col] = df[col].apply(fn)

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')


@router.get(path='/plant-energy')
async def get_plant_energy(
        request: Request,
        user_agent: Annotated[str | None, Header(include_in_schema=False)],
        accept: Annotated[str | None, Header(include_in_schema=False)],
        start_ts: dt.datetime | None = Query(
            None,
            title='Start Timestamp',
            description=(
                'The start of the interval in ISO 8601 format. '
                'If not provided, defaults to the beginning of the year. '
                'Assumes Europe/Berlin timezone if no tz info is given.'
            ),
            example='2025-01-01T00:00:00',
        ),
        end_ts: dt.datetime | None = Query(
            None,
            title='End Timestamp',
            description=(
                'The end of the interval in ISO 8601 format. '
                'If not provided, defaults to now. '
                'Assumes Europe/Berlin timezone if no tz info is given.'
            ),
            example='2025-01-15T00:00:00',
        ),
        aggregation: Annotated[
            str | None,
            Query(
                title='Aggregation',
                description=(
                    'The time interval to aggregate data: "hour", "day", or "month". '
                    'Aggregation is calculated using Europe/Berlin timezone. '
                    'If the last interval extends beyond the end timestamp, it is truncated.'
                ),
                example='day'
            ),
            AfterValidator(_validator_plant_energy_aggregation)
        ] = None,
        blacklist: bool=Query(True)
    ) -> PlainTextResponse:
    user = request.state.user
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    if start_ts is None:
        start_ts = pd.Timestamp.fromisoformat('2025-01-01T00:00:00')
        start_ts = pd.Timestamp(start_ts).tz_localize('Europe/Berlin')
        start_ts = pd.Timestamp(start_ts).tz_convert('UTC')

    if start_ts.tzinfo is None:
        start_ts = pd.Timestamp(start_ts).tz_localize('Europe/Berlin')
        start_ts = pd.Timestamp(start_ts).tz_convert('UTC')

    if end_ts is None:
        end_ts = pd.Timestamp.fromisoformat('2026-01-01T00:00:00')

    if end_ts.tzinfo is None:
        end_ts = pd.Timestamp(end_ts).tz_localize('Europe/Berlin')
        end_ts = pd.Timestamp(end_ts).tz_convert('UTC')

    max_records: int = 2**20

    interval_count = _compute_intervals(
        start_ts=start_ts,
        end_ts=end_ts,
        aggregation=aggregation
    )

    from jhe import JHE

    main_data_2_df: pd.DataFrame = pd.read_csv(io.StringIO(JHE().get_main_data()))
    plant_count: int = len(main_data_2_df.index)

    max_records_exceed_error_msg: str = f'The requested interval exceeds the maximum allowed number of records of {max_records}. ' \
        f'You requested {interval_count} intervals for {plant_count} plants ' \
        f'which equals {interval_count * plant_count} records. ' \
        f'Try a smaller interval or lower resolution.'

    if (interval_count * plant_count) > max_records:
        print(f'Request exceeds record limit: {interval_count * plant_count}/{max_records}')
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=max_records_exceed_error_msg,
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    from ingress_janitza import PlantEnergy

    plant_energy = PlantEnergy()
    
    df: pd.DataFrame = await plant_energy.get_legacy(
        start_ts=start_ts,
        end_ts=end_ts,
        aggregation=aggregation
    )

    df = df.reset_index()

    blacklist_see_numbers: list[str] = [
        'SEE978554382050',
        'SEE983469210725',
        'SEE946342489205',
        'SEE931054312422',
        'SEE979327918605',
        'SEE968366210313',
        'SEE955852581811',
        'SEE950523480752',
        'SEE948842802188',
        'SEE959843387611',
        'SEE927843297186',
        'SEE905317227136'
    ]

    if blacklist:
        df = df[~df['see_number'].isin(blacklist_see_numbers)]

    col_formatter = {
        'energy_active_supplied': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_active_consumed': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_reactive_fundamental_supplied': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ','),
        'energy_reactive_fundamental_consumed': lambda x: '' if pd.isna(x) else f'{x:.3f}'.replace('.', ',')
    }

    for col, fn in col_formatter.items():
        df[col] = df[col].apply(fn)

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')


@router.get(path='/plant-max-power-active-consumed')
async def get_plant_max_power_active_consumed(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from jhe import JHE

    df: pd.DataFrame = JHE().get_max_power_active_consumed()

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    return Response(content=response, status_code=200)
