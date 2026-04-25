"""
ble.py
======

"""

# Standard library
import io
import re

# Third-party
from typing import Annotated
from urllib.parse import quote
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request, status
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.responses import Response

# Local modules
from auth import _auth
from cache import Cache, CacheItem
from routers.util import Router


router: APIRouter = Router(
    tags=['BLE'],
    prefix='/ble',
    dependencies=[Depends(_auth)],
    allowed_users=['b.lehmann', 'c.klix']
)


cache = Cache()


@router.get(path='/main-data')
async def get_main_data(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    from ble import BLE

    df: pd.DataFrame = await BLE().get_ble_main_data()

    decimal_places: dict[str, int] = {
        'nominal_power': 3,
        'rated_power': 3,
        'elevation': 3,
        'slope': 2,
        'azimuth': 2,
        'latitude': 6,
        'longitude': 6
    }

    for col, dp in decimal_places.items():
        df[col] = df[col].apply(lambda x: f'{x:.{dp}f}'.replace('.', ',') if pd.notna(x) else None)

    response: str = df.to_csv(
        sep=';',
        decimal=',',
        index=False
    )

    if re.match(r'^error:', response, re.IGNORECASE):
        return Response(content=response, status_code=400)

    return Response(content=response, status_code=200, media_type='text/csv')


@router.get(path='/energy')
async def get_energy(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)],
    blacklist: bool=Query(False)
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


@router.get(path='/negative-hours-energy')
async def get_negative_hours_energy(
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


@router.get(path='/negative-hours-energy-month')
async def get_negative_hours_energy_month(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> StreamingResponse:
    CACHE_NAME: str = 'ble_negative_hours_energy_month'
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

        df: pd.DataFrame = await BLE().get_ble_negative_hours_energy_month()

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

    filename: str = f'negative_hours_energy_month.csv'

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


@router.get(path='/day-ahead')
async def get_day_ahead(
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
    
    print(f'INFO:     User: "{user}" requesting "GET {request.url.path}"')
    print(f'INFO:     User-Agent: "{user_agent}"')

    from ble import BLE
    from market_price_data import MarketPriceData

    df: pd.DataFrame = await BLE().get_ble_day_ahead(resolution)

    if resolution == 900:
        df: pd.DataFrame = await MarketPriceData().get_day_ahead_with_negative_hours('this_year')

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


@router.get(path='/allocations')
async def get_allocations(
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
