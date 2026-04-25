"""
main.py
=======

"""

# Standard library
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

# Third-party
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# Local modules
from ingress import router as ingress_router
from ingress import router_ble, router_admin, router_jhe
from tasks import job_day_ahead, job_energy

import routers.public
import routers.ble
import routers.jhe
import routers.fabric


TAGS_METADATA: list[dict[str, str]] = [
    {
        'name': 'public',
        'description': 'Endpoints that can be access without authentication.'
    },
    {
        'name': 'BLE',
        'description': 'Custom endpoints for Bastian Lehmann\'s workflows and reporting needs.'
    },
    {
        'name': 'JHE',
        'description': 'Custom endpoints for Joel Heim\'s workflows and reporting needs.'
    },
    {
        'name': 'PNG',
        'description': 'Custom endpoints for Tuan Phong Ngo\'s workflows and reporting needs.'
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    asyncio.create_task(job_day_ahead())
    asyncio.create_task(job_energy())
    yield


app: FastAPI = FastAPI(
    title='Reporting Cloud - Task Ninja Bridge',
    summary='A summary of the API',
    description='From RDP to Cloud, one bridge at a time.',
    version='1.1.0',
    openapi_tags=TAGS_METADATA,
    redirect_slashes=True,
    lifespan=lifespan,
    terms_of_service='https://reporting.dev.enerparc.com/task-ninja-bridge/tos',
    contact={
        'name': 'Christopher Klix',
        'url': 'https://github.com/ChristopherKlixEnerparcAG',
        'email': 'c.klix@enerparc.com'
    },
    license_info=None
    # root_path=route # Doesn't work for mounting applications. Needs to be passed as a CLI argument directly to uvicorn
 )

app.include_router(routers.public.router)
app.include_router(routers.ble.router)
app.include_router(routers.fabric.router)
app.include_router(routers.jhe.router)
app.include_router(ingress_router, tags=['admin'])
app.include_router(router_ble)
app.include_router(router_admin)
app.include_router(router_jhe)

app.mount(f'/static/', StaticFiles(directory='./web'))
