"""
public.py
=========

"""

# Standard library

# Third-party
import importlib
import io
from fastapi.templating import Jinja2Templates
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.responses import Response, HTMLResponse

# Local modules


router: APIRouter = APIRouter(
    tags=['public'],
    prefix='',
    dependencies=None
)

start_time: pd.Timestamp = pd.Timestamp.now()


@router.get('/', response_class=HTMLResponse)
def root():
    with open('web/index.html') as f:
        return f.read()
    
    return FileResponse(Path('static/index.html'))


@router.get('/status')
def get_status():
    uptime: pd.Timedelta = pd.Timestamp.now() - start_time

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
        'uptime': f'{uptime}'
    })


@router.get('/mastr')
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


@router.get('/routes/{router_name}')
async def get_routes(
    request: Request,
    router_name: str
) -> JSONResponse:
    try:
        mod = importlib.import_module(f'routers.{router_name}')
    except ModuleNotFoundError:
        return JSONResponse(status_code=404, content={'error': 'router not found'})

    _router = getattr(mod, 'router', None)

    if not _router or not hasattr(_router, 'routes'):
        return JSONResponse(status_code=400, content={'error': 'invalid router module'})

    data: list[dict[str, str]] = [
        {
            'path': r.path,
            'methods': list(r.methods),
            'name': r.name,
            'summary': r.summary,
            'description': r.description,
            'url': f'https://reporting.dev.enerparc.com/task-ninja-bridge{r.path}'
        }
        for r in _router.routes
        if hasattr(r, 'methods')
    ]

    return JSONResponse(content={'routes': data})
