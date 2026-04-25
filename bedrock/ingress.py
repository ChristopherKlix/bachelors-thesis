"""
ingress.py
==========

"""

# Standard library
import datetime as dt
import io
from io import StringIO
import re
from typing import Annotated, Any
from urllib.parse import quote

# Third-party
import pandas as pd
from fastapi import APIRouter, Header, Query, Depends, Request, HTTPException, status
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.responses import Response, HTMLResponse, StreamingResponse
from pydantic import BaseModel, AfterValidator

# Local modules
from auth import _auth
from cache import Cache, CacheItem


cache = Cache()


################################################################
# Routers
################################################################

router = APIRouter(
    dependencies=[Depends(_auth)]
)

router_public = APIRouter()

router_ble = APIRouter(
    tags=['BLE New'],
    prefix='/ble',
    dependencies=[Depends(_auth)]
)

router_jhe = APIRouter(
    tags=['JHE Old'],
    prefix='/jhe',
    dependencies=[Depends(_auth)]
)

router_admin = APIRouter(
    prefix='/admin',
    dependencies=[Depends(_auth)]
)

start_time: dt.datetime = dt.datetime.now()

################################################################
# Public
################################################################

def get_uptime() -> dt.timedelta:
    td: dt.timedelta = dt.datetime.now() - start_time
    td = pd.to_timedelta(int(pd.Timedelta(td).total_seconds()), unit='s')
    return td

@router_public.get('/', response_class=HTMLResponse)
def root():
    with open('web/index.html') as f:
        return f.read()

@router_public.get('/status')
def get_status():
    return JSONResponse(content={
        'status': {
            'state': 'maintenance',
            'message': 'This service is currently under maintenance.',
            'since': f'{start_time}'
        },
        'description': 'Provides endpoints for the communication with the Task Ninja',
        'version': '1.0.0',
        'name': 'Task Ninja Bridge',
        'router': 'task-ninja-bridge',
        'uptime': f'{get_uptime()}'
    })

@router_public.get('/mastr')
async def get_mastr():
    from mastr import MaStR

    df: pd.DataFrame = await MaStR().get()

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False
    )

    df['longitude'] = df['longitude'].astype('float64')
    df['latitude'] = df['latitude'].astype('float64')
    df['gross_power'] = df['gross_power'].astype('float64')
    df['rated_power'] = df['rated_power'].astype('float64')
    df['inverter_power'] = df['inverter_power'].astype('float64')
    df['area'] = df['area'].astype('float64')

    df['remote_control'] = df['remote_control'].astype('float64')
    df['orientation'] = df['orientation'].astype('float64')
    df['remote_control_direct_marketer'] = df['remote_control_direct_marketer'].astype('float64')
    df['connection_high_voltage'] = df['connection_high_voltage'].astype('float64')

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    return Response(content=response, status_code=200, media_type='text/csv')

################################################################
# Router Admin
################################################################

class UserModelIn(BaseModel):
    username: str
    password: str | None = None


@router_admin.get('/users/{username}')
def get_router_admin_user(request: Request, username: str) -> Response:
    from auth import get_user_from_db, User

    user: User | None = get_user_from_db(username=username)

    if user is None:
        response: dict[str, str] = {
            'detail': f'user "{username}" does not exist.'
        }
        return JSONResponse(content=response, status_code=404)

    last_login: str | None = None

    if user.last_login is not None:
        last_login = user.last_login.isoformat()

    response: dict[str, str] = {
        'id': f'{user.user_id}',
        'username': user.username,
        'password_hash': user.password_hash,
        'created_on': user.created_on.isoformat(),
        'last_login': last_login
    }

    return JSONResponse(content=response)

@router_admin.post('/users')
def post_router_admin_users(
        request: Request,
        user: UserModelIn
    ) -> Response:
    from auth import create_user

    response: dict[str, Any] = create_user(
        username=user.username,
        password=user.password
    )

    return JSONResponse(content=response)

@router_admin.post('/users/create-defaults')
def post_router_admin_users_create_defaults(request: Request) -> Response:
    whitelisted_users: list[tuple[str, str]] = [
        ('admin', 'k/>7/2eG_F5R'),
        ('c.klix', 'z_4;P74V7bN}'),
        ('b.lehmann', '@d|N?04W37Pe'),
        ('om.taskscheduler', '>TI0u1BN6;!j'),
        ('j.heim', 'Rv3V9<L67@7l'),
        ('p.ngo', 'R,Oc5B!*0S2V')
    ]

    responses: list[dict[str, Any]] = []

    from auth import create_user

    for user in whitelisted_users:
        response: dict[str, Any] = create_user(
            username=user[0],
            password=user[1]
        )
        responses.append(response)

    return JSONResponse(content=responses)

################################################################
# Router BLE
################################################################

ble_main_data_example: str = """report_id;see_number;name;park;janitza_name;rated_power;commissioning_date
DE.001.01;SEE920747776405;Kleinaitingen;Kleinaitingen;KLA11A;4865.400;2010-07-01
DE.002.01;SEE910505982469;Klettbach;Klettbach;KLT11A;1906.200;2010-12-22
DE.003.02;SEE973265793768;Heinersdorf - BA1;Heinersdorf;HDF11A;1587.190;2010-12-28
DE.003.04;SEE901023493537;Heinersdorf - BA2.1;Heinersdorf;HDF12A;2450.580;2010-12-28
...
"""

STANDARD_RESPONSES = {
    401: {'description': 'Not Authorized.'},
    400: {'description': 'Bad Request.'},
    500: {'description': 'Internal Server Error.'},
    422: {'description': 'Validation error.'}
}

class CSVResponse(PlainTextResponse):
    media_type = 'text/csv'

ROUTER_BLE_MAIN_DATA_SUMMARY: str = 'Retrieve main data'
ROUTER_BLE_MAIN_DATA_DESCRIPTION: str = (
    'Returns the main data for all plants specific to the fields BLE requested for their Power BI app.\n\n'
    '- No query parameters.\n'
    '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
    '- Data source: coalesced from the Marktstammdatenregister (MaStR) whenever available, '
    'and supplemented with legacy Excel data from "main_2.0.xlsx".'
)

ROUTER_JHE_PLANT_ENERGY_SUMMARY: str = 'Retrieve plant energy measurements'
ROUTER_JHE_PLANT_ENERGY_DESCRIPTION: str = (
    'Returns energy measurement data from the Janitza meters split on plant level.\n\n'
    '- Optional query params: `start_ts`, `end_ts` (ISO 8601 format).\n'
    '- If no timezone is provided, Europe/Berlin is assumed for both start and end.\n'
    '- Defaults: `start_ts` = beginning of year, `end_ts` = now.\n'
    '- Aggregation supported: `hour`, `day`, `month`.\n'
    '- Aggregations (`hour`, `day`, `month`) are calculated using Europe/Berlin time.\n'
    '- If the aggregation extends beyond the end timestamp, the last interval is truncated.\n'
    '  *Example:* `2025-01-01T00:00:00` → `2025-01-15T00:00:00`, agg=`month` '
    'only aggregates data up to the 15th.'
)

META = {
    'ble': {
        'router': router_ble,
        'paths': {
            '/main-data': {
                'summary': ROUTER_BLE_MAIN_DATA_SUMMARY,
                'description': ROUTER_BLE_MAIN_DATA_DESCRIPTION,
                'response_class': CSVResponse,
                'responses': STANDARD_RESPONSES
            }
        }
    },
    'jhe': {
        'router': router_jhe,
        'paths': {
            '/plant-energy': {
                'summary': ROUTER_JHE_PLANT_ENERGY_SUMMARY,
                'description': ROUTER_JHE_PLANT_ENERGY_DESCRIPTION,
                'response_class': CSVResponse,
                'repsonses': STANDARD_RESPONSES
            }
        }
    }
}

def GET(router: str, path: str):
    _router: APIRouter = META.get(router).get('router')
    meta: dict[str, Any] = META.get(router).get('paths').get(path)

    return _router.get(
        path=path,
        summary=meta.get('summary'),
        description=meta.get('description'),
        response_class=meta.get('response_class'),
        responses=meta.get('responses')
    )


ble_plant_energy_example: str = """report_id;name;park;see_number;janitza_name;start_ts;end_ts;energy
DE.001.01;Kleinaitingen;Kleinaitingen;SEE920747776405;KLA11A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;9756,672
DE.002.01;Klettbach;Klettbach;SEE910505982469;KLT11A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;1314,816
DE.003.02;Heinersdorf - BA1;Heinersdorf;SEE973265793768;HDF11A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;698,368
DE.003.04;Heinersdorf - BA2.1;Heinersdorf;SEE901023493537;HDF12A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;837,754
...
"""

ble_main_data_example: str = """report_id;see_number;name;park;janitza_name;rated_power;commissioning_date
DE.001.01;SEE920747776405;Kleinaitingen;Kleinaitingen;KLA11A;4865.400;2010-07-01
DE.002.01;SEE910505982469;Klettbach;Klettbach;KLT11A;1906.200;2010-12-22
DE.003.02;SEE973265793768;Heinersdorf - BA1;Heinersdorf;HDF11A;1587.190;2010-12-28
DE.003.04;SEE901023493537;Heinersdorf - BA2.1;Heinersdorf;HDF12A;2450.580;2010-12-28
...
"""

ble_negative_hours_energy_example: str = """see_number;month;start_ts;end_ts;energy_active_supplied;energy_active_supplied_neg_null
SEE900054722814;2025-01;2024-12-31 23:00:00+00:00;2025-01-31 23:00:00+00:00;55178,826;-0,000
SEE900054722814;2025-02;2025-01-31 23:00:00+00:00;2025-02-28 23:00:00+00:00;133068,239;-0,000
SEE900054722814;2025-03;2025-02-28 23:00:00+00:00;2025-03-28 23:00:00+00:00;340755,178;-0,000
SEE900054722814;2025-04;2025-03-31 22:00:00+00:00;2025-04-30 22:00:00+00:00;401594,376;-0,000
...
"""

# /pq/ble/main-data
@router.get(
    '/pq/ble/main-data',
    tags=['BLE Old'],
    summary='Retrieve main data',
    description=(
        'Returns the main data for all plants specific to the fields BLE requested for their Power BI app.\n\n'
        '- No query parameters.\n'
        '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
        '- Data source: coalesced from the Marktstammdatenregister (MaStR) whenever available, '
        'and supplemented with legacy Excel data from "main_2.0.xlsx".'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': ble_main_data_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
async def get_ble_main_data(request: Request) -> Response:
    from ble import BLE

    df: pd.DataFrame = await BLE().get_ble_main_data()

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False
    )

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')

# /pq/ble/energy
@router.get(
    '/pq/ble/energy',
    tags=['BLE Old'],
    summary='Retrieve plant energy measurements',
    description=(
        'Returns energy measurement data from the Janitza meters split on plant level for the current year up to now.\n\n'
        '- No query parameters required.\n'
        '- Aggregation is always `day`.\n'
        '- Europe/Berlin timezone is assumed for all timestamps and aggregations.\n'
        '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
        '- Main data is coalesced from the Marktstammdatenregister and supplemented with legacy Excel data.\n'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': ble_plant_energy_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
async def get_ble_energy(
        request: Request,
        blacklist: bool=Query(True)
    ) -> PlainTextResponse:
    from ble import BLE

    df: pd.DataFrame = await BLE().get_ble_energy()

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

################################################################
# BLE
################################################################

# /pq/ble/negative-hours-energy
@router.get(
    '/pq/ble/negative-hours-energy',
    tags=['BLE Old'],
    summary='Retrieve negative hours plant energy measurements',
    description=(
        '***New***\n\n'
        'Returns energy measurement data from the Janitza meters split on plant level '
        'including the negative hours computation.\n\n'
        'Data source: ``plant_energy``, ``main_data_2``\n\n'
        '⏱ **Note: Response time can be 50-60 seconds.**\n'
        '- No query parameters allowed.\n'
        '- Always returns data for the current year\n'
        '- Defaults: `start_ts` = beginning of year, `end_ts` = beginning of next year.\n'
        '- Aggregation supported: `day`\n'
        '- Aggregations (`day`) are calculated using Europe/Berlin time.\n\n'
        '⚠️ Only for internal use.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={**STANDARD_RESPONSES, 200: {
        'description': 'CSV data using German notation (semicolon separator, comma decimal).',
        'content': {'text/csv': {'example': ble_negative_hours_energy_example}}
    }}
)
@router.get(
    '/pq/ble/negative-hours-energy-2',
    tags=['BLE Old'],
    summary='Retrieve negative hours plant energy measurements',
    description=(
        '***New***\n\n'
        'Returns energy measurement data from the Janitza meters split on plant level '
        'including the negative hours computation.\n\n'
        'Data source: ``plant_energy``, ``main_data_2``\n\n'
        '⏱ **Note: Response time can be 50-60 seconds.**\n'
        '- No query parameters allowed.\n'
        '- Always returns data for the current year\n'
        '- Defaults: `start_ts` = beginning of year, `end_ts` = beginning of next year.\n'
        '- Aggregation supported: `day`\n'
        '- Aggregations (`day`) are calculated using Europe/Berlin time.\n\n'
        '⚠️ Only for internal use.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={**STANDARD_RESPONSES, 200: {
        'description': 'CSV data using German notation (semicolon separator, comma decimal).',
        'content': {'text/csv': {'example': ble_negative_hours_energy_example}}
    }}
)
async def get_ble_negative_hours(
        request: Request,
        user_agent: Annotated[str | None, Header(include_in_schema=False)],
        accept: Annotated[str | None, Header(include_in_schema=False)]
    ) -> StreamingResponse:
    CACHE_NAME: str = 'ble_negative_hours_energy'
    CACHE_EXPIRES_IN: pd.Timedelta = pd.Timedelta(minutes=5)

    user = request.state.user

    allowed_users: list[str] = ['b.lehmann', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from ingress_negative_hours import NegativeHours
    from ble import BLE

    t = pd.Timestamp.now()

    global cache

    cache_item: CacheItem | None = None

    try:
        cache_item: CacheItem = cache.get(CACHE_NAME)
    except KeyError as _:
        cache_item = None

    async def get_stream() -> io.BytesIO:
        print(f'INFO:     Caching {CACHE_NAME}')
        cache_expiration_ts: pd.Timestamp = pd.Timestamp.now('Europe/Berlin') + CACHE_EXPIRES_IN
        print(f'INFO:     Cache expires {cache_expiration_ts.round('s')}')

        df: pd.DataFrame = await BLE().get_ble_negative_hours_energy()

        df = df.reset_index()

        from serializer import Serializer

        stream: io.BytesIO = Serializer.df_to_csv(
            df=df,
            dtype_map={
                'energy_active_supplied': 'float64',
                'energy_active_supplied_neg_null': 'float64'
            },
            places_map={
                'energy_active_supplied': 3,
                'energy_active_supplied_neg_null': 3
            },
            locale='DE'
        )

        size_bytes = stream.getbuffer().nbytes
        print(f'INFO:     Stream size: {size_bytes / 1024:.2f} KB')

        return stream

    if cache_item is None:
        stream: io.BytesIO = await get_stream()
        cache_item = CacheItem(
            value=stream,
            name=CACHE_NAME,
            expires_in=CACHE_EXPIRES_IN
        )
        cache.add(cache_item)
    
    if cache_item.data is None:
        stream: io.BytesIO = await get_stream()
        cache_item.data = stream

    cache_item.data.seek(0)

    current_year: int = pd.Timestamp.now('Europe/Berlin').year
    filename: str = f'{current_year}_negative_hours_energy.csv'

    time_total: pd.Timedelta = pd.Timestamp.now() - t

    quoted_filename: str = quote(filename)

    content_disposition: str = (
        f'attachment; filename="{filename}"; filename*=UTF-8\'\'{quoted_filename}'
    )

    headers: dict[str, str] = {
        'Content-Disposition': content_disposition,
        "Cache-Control": "public, max-age=900",
        'Time': f'{time_total}'
    }

    def iter_csv(stream: io.BytesIO, chunk_size: int = 8192):
        stream.seek(0)
        while chunk := stream.read(chunk_size):
            yield chunk

    response = StreamingResponse(
        iter_csv(cache_item.data),
        media_type="text/csv",
        headers=headers
    )

    return response

EXAMPLE_BLE_DAY_AHEAD: str = """start_ts;end_ts;day_ahead
2024-12-31 23:00:00+00:00;2024-12-31 23:15:00+00:00;0,216
2024-12-31 23:15:00+00:00;2024-12-31 23:30:00+00:00;0,216
2024-12-31 23:30:00+00:00;2024-12-31 23:45:00+00:00;0,216
2024-12-31 23:45:00+00:00;2025-01-01 00:00:00+00:00;0,216
...
"""

# /pq/ble/day-ahead
@router.get(
    '/pq/ble/day-ahead',
    tags=['BLE Old'],
    summary='Get Day Ahead market prices',
    description=(
        '***New***\n\n'
        'Returns the Day Ahead prices either in 15 min or hourly resolution\n\n'
        'Data source: ``market_prices_qh``, ``market_prices_h``\n\n'
        '- Optional query params: ``resolution``.\n'
        '- Always returns data for the current year\n'
        '- Defaults: ``start_ts`` = beginning of year, ``end_ts`` = beginning of next year.\n'
        '- Resolutions supported: ``900``, ``3600``\n\n'
        '⚠️ Only for internal use.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={**STANDARD_RESPONSES, 200: {
        'description': 'CSV data using German notation (semicolon separator, comma decimal).',
        'content': {'text/csv': {'example': EXAMPLE_BLE_DAY_AHEAD}}
    }}
)
async def get_ble_day_ahead(
        request: Request,
        user_agent: Annotated[str | None, Header(include_in_schema=False)],
        accept: Annotated[str | None, Header(include_in_schema=False)],
        resolution: int = Query(
            900,
            title='Resolution of the prices',
            description=(
                '15min or 1h — '
                'Time interval of each price point in seconds: 900 (15min) or 3600 (1h). '
                'Hourly values are averages over 4×15min periods but are valid market prices.'
            ),
            example='900',
        )
    ) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['b.lehmann', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from ble import BLE

    df: pd.DataFrame = await BLE().get_ble_day_ahead(resolution)

    df = df.reset_index()

    col_formatter = {
        'day_ahead': lambda x: f'{x:.3f}'.replace('.', ',')
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

    return Response(content=response, status_code=200)

EXAMPLE_BLE_ALLOCATIONS: str = """see_number;year;start_ts;end_ts;commissioning_date;bid_submission_date;relevant_date;mapped_eeg_rule
SEE900054722814;2025;2024-12-31 23:00:00+00:00;2025-12-31 23:00:00+00:00;2012-11-30;;2012-11-30;
SEE900069397786;2025;2024-12-31 23:00:00+00:00;2025-12-31 23:00:00+00:00;2020-11-30;;2020-11-30;6
SEE900161595301;2025;2024-12-31 23:00:00+00:00;2025-12-31 23:00:00+00:00;2013-09-30;;2013-09-30;
SEE900217345379;2025;2024-12-31 23:00:00+00:00;2025-12-31 23:00:00+00:00;2020-05-06;2018-11-01;2018-11-01;6
...
"""

# /pq/ble/allocations
@router.get(
    '/pq/ble/allocations',
    tags=['BLE Old'],
    summary='Get yearly allocations mapping plants to EEG/InnAusV rules.',
    description=(
        '***New***\n\n'
        'Returns the allocations that map a plant to the EEG rule for § 51, previously § 24, and InnAusV for each year of operation.\n\n'
        'Data source: ``main_data_2``, ``mastr_einheitensolar``, ``mastr_anlageneegsolar``, ``auctions``\n\n'
        '- Optional query params: ``start_year``, ``end_year``.\n'
        '- Always returns data for the current year\n'
        '- Defaults: ``start_year`` = ``2016``, ``end_year`` = next year.\n\n'
        '⚠️ Only for internal use.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={**STANDARD_RESPONSES, 200: {
        'description': 'CSV data using German notation (semicolon separator, comma decimal).',
        'content': {'text/csv': {'example': EXAMPLE_BLE_ALLOCATIONS}}
    }}
)
async def get_ble_allocations(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)],
    start_year: int = Query(
            2016,
            title='Start year (inclusive)',
            description=(
                'Allocations will be returned starting from this year.'
            ),
            example=2025,
        ),
    end_year: int | None = Query(
        None,
        title='Optional end year (exclusive)',
        description=(
            'Optional. If not set, allocations up to the latest available year are returned.'
        ),
        example=None,
    )
) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['b.lehmann', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from ingress_negative_hours import NegativeHours

    negative_hours = NegativeHours()

    if end_year is None:
        end_year = pd.Timestamp.now('Europe/Berlin').year + 1

    start_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{start_year}-01-01T00:00')
    start_ts = start_ts.tz_localize('Europe/Berlin')
    end_ts: pd.Timestamp = pd.Timestamp.fromisoformat(f'{end_year}-01-01T00:00')
    end_ts = end_ts.tz_localize('Europe/Berlin')

    df: pd.DataFrame = await negative_hours.get_negative_hours_rules(
        start_ts=start_ts,
        end_ts=end_ts
    )

    df = df.reset_index()

    df['mapped_eeg_rule'] = df['mapped_eeg_rule'].astype('float64')

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False,
        na_rep='',
        encoding='utf-8',
        lineterminator='\n'
    )

    return Response(content=response, status_code=200)

################################################################
# JHE
################################################################

def _validator_jhe_janitza_energy_aggregation(aggregation: str) -> str:
    available_aggregations: list[str] = ['None', 'hour', 'day', 'month']
    
    if not aggregation in available_aggregations:
        raise ValueError(
            f'Invalid "aggregation" format, it must be one of "{(', '.join(available_aggregations))}"'
        )
    
    return aggregation

def _validator_jhe_plant_energy_aggregation(aggregation: str) -> str:
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

jhe_janitza_energy_example: str = """janitza_name;start_ts;end_ts;energy_active_supplied;energy_active_consumed;energy_reactive_fundamental_supplied;energy_reactive_fundamental_consumed
AGD11A;2024-12-31 23:00:00+00:00;2025-01-31 23:00:00+00:00;-132371,404;2695,061;-122014,391;0,0
AGD11A;2025-01-31 23:00:00+00:00;2025-02-28 23:00:00+00:00;-257143,677;2220,626;-147012,575;0,0
AGD11A;2025-02-28 23:00:00+00:00;2025-03-31 22:00:00+00:00;-569316,566;2189,02;-249730,851;0,0
AGD11A;2025-03-31 22:00:00+00:00;2025-04-30 22:00:00+00:00;-666540,66;1920,195;-275691,507;0,0
...
"""

jhe_plant_energy_example: str = """main_data_2_id;see_number;name;park;janitza_name;start_ts;end_ts;energy_active_supplied;energy_active_consumed;energy_reactive_fundamental_supplied;energy_reactive_fundamental_consumed
1;SEE945616407815;Altentreptow;Altentreptow;ATT21A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;-108,059;12,204;-34,77;40,625
2;SEE900610093334;Anklam;Anklam;ANK11A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;-143,321;29,291;-322,873;6,709
3;SEE938159505643;Baalberge;Baalberge;BLB21A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;-1176,452;0,0;-564,272;0,243
4;SEE987511434411;Bienstädt - BA1;Bienstädt;BIS21A;2024-12-31 23:00:00+00:00;2025-01-01 23:00:00+00:00;-1402,624;39,689;-502,54;456,498
...
"""

jhe_main_data_example: str = """see_number;name;park;janitza_name;gross_power;rated_power;country;city;commissioning_date;poi;spv
SEE945616407815;Altentreptow;Altentreptow;ATT21A;653,4;549,32;Deutschland;Altentreptow;2021-05-21;KST Altentreptow;ESI 148
SEE900610093334;PVA Anklam;Anklam;ANK11A;1263,3;1170,0;Deutschland;Anklam;2015-04-29;KST Anklam;ESI 103
SEE938159505643;Baalberge;Baalberge;BLB21A;2384,0;2244,0;Deutschland;Bernburg;2019-03-06;KST Baalberge;EP2SI 6
SEE987511434411;PVA Bienstädt 1;Bienstädt;BIS21A;2105,0;1980,0;Deutschland;Bienstädt;2016-08-30;KST Bienstädt;ESI 26
...
"""

jhe_mini_bess_example: str = (
    'name,associated_plant_name,inverter_power,voltage_level,poi,capacity,cubes,hpc,mini_bess_costs,installation_costs,status,commissioning_date,supplier,model,transformer_station,spv,bank,plant_status\n'
    'Gaarz 1.1 1,Gaarz 1.1,80,400,UW Schmarsow,600.000,1,0,205486.92,14750.00,active,2024-04-19,Naext,Naext Quadragon,TST06, ENERPARC Solar Invest 150 GmbH,DKB,PE10\n'
    'Gaarz 1.1 2,Gaarz 1.1,80,400,UW Schmarsow,600.000,1,0,205486.92,14750.00,active,2024-04-19,Naext,Naext Quadragon,TST08, ENERPARC Solar Invest 150 GmbH,DKB,PE10\n'
    'Eggebek 1.2,Eggebek 1.2,80,800,UW Schobüll,600.000,1,1,266294.36,19500.00,active,2024-06-29,Naext,Naext Quadragon,TST22,ENERPARC Solar Invester 176 GmbH,VR Bank,PE10\n'
    '...'
)

@router.get(
    '/pq/ble/janitza-energy',
    tags=['BLE Old'],
    summary='Retrieve Janitza energy measurements',
    description=(
        'Returns energy measurement data from the Janitza meters.\n\n'
        '- Optional query params: `start_ts`, `end_ts` (ISO 8601 format).\n'
        '- If no timezone is provided, Europe/Berlin is assumed for both start and end.\n'
        '- Defaults: `start_ts` = beginning of year, `end_ts` = now.\n'
        '- Aggregation supported: `hour`, `day`, `month`.\n'
        '- Aggregations (`hour`, `day`, `month`) are calculated using Europe/Berlin time.\n'
        '- If the aggregation extends beyond the end timestamp, the last interval is truncated.\n'
        '  *Example:* `2025-01-01T00:00:00` → `2025-01-15T00:00:00`, agg=`month` '
        'only aggregates data up to the 15th.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': jhe_plant_energy_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
async def get_jhe_janitza_energy(
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
            AfterValidator(_validator_jhe_janitza_energy_aggregation)
        ] = None
    ) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['j.heim', 'b.lehmann', 'p.ngo', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
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


@router.get(
    '/pq/ble/plant-energy',
    tags=['BLE Old'],
    summary='Retrieve plant energy measurements',
    description=(
        'Returns energy measurement data from the Janitza meters split on plant level.\n\n'
        '- Optional query params: `start_ts`, `end_ts` (ISO 8601 format).\n'
        '- If no timezone is provided, Europe/Berlin is assumed for both start and end.\n'
        '- Defaults: `start_ts` = beginning of year, `end_ts` = now.\n'
        '- Aggregation supported: `hour`, `day`, `month`.\n'
        '- Aggregations (`hour`, `day`, `month`) are calculated using Europe/Berlin time.\n'
        '- If the aggregation extends beyond the end timestamp, the last interval is truncated.\n'
        '  *Example:* `2025-01-01T00:00:00` → `2025-01-15T00:00:00`, agg=`month` '
        'only aggregates data up to the 15th.'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': jhe_janitza_energy_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
async def get_jhe_plant_energy(
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
            AfterValidator(_validator_jhe_plant_energy_aggregation)
        ] = None,
        blacklist: bool=Query(True)
    ) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['j.heim', 'b.lehmann', 'p.ngo', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
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

    max_records: int = 2**19

    interval_count = _compute_intervals(
        start_ts=start_ts,
        end_ts=end_ts,
        aggregation=aggregation
    )

    from jhe import JHE

    main_data_2_df: pd.DataFrame = pd.read_csv(StringIO(JHE().get_main_data()))
    plant_count: int = len(main_data_2_df.index)

    max_records_exceed_error_msg: str = f'The requested interval exceeds the maximum allowed number of records of {max_records}. ' \
        f'You requested {interval_count} intervals for {plant_count} plants ' \
        f'which equals {interval_count * plant_count} records. ' \
        f'Try a smaller interval or lower resolution.'

    if (interval_count * plant_count) > max_records:
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


@router.get(
    '/pq/jhe/main-data',
    tags=['JHE Old'],
    summary='Retrieve main data',
    description=(
        'Returns the main data for all plants specific to the fields JHE requested for their Power BI app.\n\n'
        '- No query parameters.\n'
        '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
        '- Data source: coalesced from the Marktstammdatenregister (MaStR) whenever available, '
        'and supplemented with legacy Excel data from "main_2.0.xlsx".'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': jhe_main_data_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
async def get_jhe_main_data(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['j.heim', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from jhe import JHE

    response: str = JHE().get_main_data()

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')


@router.get(
    '/pq/jhe/mini-bess',
    tags=['JHE Old'],
    summary='Retrieve main data for the Mini-BESS',
    description=(
        'Returns the main data for all Mini-BESS specific to the fields JHE requested for their Power BI app.\n\n'
        '- No query parameters.\n'
        '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
        '- Data source: manual data from ``mini_bess.csv``'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': jhe_mini_bess_example}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
def get_jhe_mini_bess(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['j.heim', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
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


@router.get(
    '/pq/jhe/plant-max-power-active-consumed',
    tags=['JHE Old'],
    summary='Retrieve the maximum power active per plant per day',
    description=(
        'Returns the maximum power active per plant per day specific to the fields JHE requested for their Power BI app.\n\n'
        '- No query parameters.\n'
        '- Response is CSV using German notation (semicolon separator, comma decimal).\n'
        '- Data source: ``plant_energy``'
    ),
    response_class=PlainTextResponse(media_type='text/csv'),
    responses={
        200: {
            'content': {'text/csv': {'example': ''}},
            'description': 'CSV data using German notation (semicolon separator, comma decimal).'
        },
        401: {'description': 'Not Authorized. Missing or invalid credentials.'},
        400: {'description': 'Bad Request. Input parameters are invalid.'},
        500: {'description': 'Internal Server Error. Something went wrong on the server.'},
        422: {'description': 'Validation error. Returned if query/path parameters or request body are invalid.'}
    }
)
def get_jhe_max_power_active_consumed(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)] = None,
    accept: Annotated[str | None, Header(include_in_schema=False)] = None,
) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['j.heim', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
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

################################################################
# PNG
################################################################

@router.get('/pq/png/day-ahead', tags=['PNG'], summary='Get Day Ahead')
async def get_png_day_ahead(
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
        )
    ) -> PlainTextResponse:
    user = request.state.user

    allowed_users: list[str] = ['p.ngo', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
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

    from market_price_data import MarketPriceData

    market = MarketPriceData()

    df: pd.DataFrame = await market.get(
        start_ts=start_ts,
        end_ts=end_ts
    )

    df = df.reset_index()

    col_formatter = {
        'day_ahead': lambda x: f'{x:.3f}'.replace('.', ',')
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

    return Response(content=response, status_code=200)

################################################################
# 410 - Gone
################################################################

@router.get('/pq/negative-hours')
@router.get('/pq/day-ahead')
def get_resource_no_longer_available() -> Response:
    return JSONResponse(content={
        'detail': 'The requested resource is no longer available.'
    }, status_code=410)

################################################################
# 308 - Permanent Redirect
################################################################


@router.get(
        '/gis/abr-numbers',
        response_model=dict[str, list[str]],
        summary='Get available ABR numbers',
        description='Returns all unique ABR numbers associated with Enerparc AG.',
        response_description='List of ABR numbers'
    )
async def get_gis_abr_number(
        request: Request,
        user_agent: Annotated[str | None, Header(include_in_schema=False)] = None
    ) -> dict[str, list[str]]:
    user = request.state.user

    allowed_users: list[str] = ['gis', 'admin']

    if user not in allowed_users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f'This endpoint is only authorized to {(', '.join(allowed_users))}',
            headers={'WWW-Authenticate': 'Basic'}
        )
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    df: pd.DataFrame = pd.read_csv(
        '/srv/admin/static/spvs.csv'
    )

    abr: list[str] = (
        df['abr_number']
            .dropna()
            .astype(str)
            .unique()
            .tolist()
    )

    return {
        'abr_numbers': abr
    }
