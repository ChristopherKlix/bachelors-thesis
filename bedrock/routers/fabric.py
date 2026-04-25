"""
fabric.py
=========

"""

# Standard library
import io
import re

# Third-party
from typing import Annotated
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.responses import Response

# Local modules
from auth import _auth
from cache import Cache, CacheItem
from routers.util import Router
from fabric import MainData, PlantEnergy


router: APIRouter = Router(
    tags=['Fabric'],
    prefix='/fabric',
    dependencies=[Depends(_auth)],
    allowed_users=['fabric']
)


cache = Cache()


def is_microsoft_user_agent(user_agent: str) -> bool:
    return re.search(r'\bMicrosoft\.Data\.Mashup\b', user_agent) is not None


def accepts_csv(accept: str | None) -> bool:
    if not accept:
        return True  # default assumption if header missing
    
    return 'text/csv' in accept or '*/*' in accept


@router.get(path='/plants')
async def get_plants(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_plants()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/parks')
async def get_parks(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_parks()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/janitzas')
async def get_janitzas(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_janitzas()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/spvs')
async def get_spvs(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_spvs()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/pois')
async def get_pois(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_pois()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/grid-operators')
async def get_grid_operators(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)]
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await MainData().get_grid_operators()

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')


@router.get(path='/plant-energy')
async def get_plant_energy(
    request: Request,
    user_agent: Annotated[str | None, Header(include_in_schema=False)],
    accept: Annotated[str | None, Header(include_in_schema=False)],
    agg: str
) -> PlainTextResponse:
    if not is_microsoft_user_agent(user_agent):
        raise HTTPException(
            status_code=403,
            detail='only Microsoft.Data.Mashup user agents are allowed'
        )
    
    if not accepts_csv(accept):
        raise HTTPException(
            status_code=406,
            detail='only text/csv is supported'
        )

    df: pd.DataFrame = await PlantEnergy().get(agg=agg)

    response: str = df.to_csv(index=False)

    return PlainTextResponse(content=response, status_code=200, media_type='text/csv')
