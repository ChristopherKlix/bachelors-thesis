"""
util.py
=======

"""


# Standard library
from enum import Enum
import importlib.util
from pathlib import Path
from typing import Annotated, Any, Callable, Dict, List, Optional, Sequence, Type, Union
from functools import wraps
from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from typing_extensions import Doc, deprecated

# Third-party
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi import APIRouter, Depends, Response, params
from fastapi import Request, HTTPException, status
from starlette.types import ASGIApp, Lifespan
from starlette.routing import BaseRoute

from auth import _auth


class Router(APIRouter):
    def __init__(
            self,
            *,
            prefix: Annotated[str, Doc("An optional path prefix for the router.")] = "",
            tags: Annotated[
                Optional[List[Union[str, Enum]]],
                Doc(
                    """
                    A list of tags to be applied to all the *path operations* in this
                    router.

                    It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                    Read more about it in the
                    [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                    """
                ),
            ] = None,
            dependencies: Annotated[
                Optional[Sequence[params.Depends]],
                Doc(
                    """
                    A list of dependencies (using `Depends()`) to be applied to all the
                    *path operations* in this router.

                    Read more about it in the
                    [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                    """
                ),
            ] = None,
            default_response_class: Annotated[
                Type[Response],
                Doc(
                    """
                    The default response class to be used.

                    Read more in the
                    [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
                    """
                ),
            ] = Default(JSONResponse),
            responses: Annotated[
                Optional[Dict[Union[int, str], Dict[str, Any]]],
                Doc(
                    """
                    Additional responses to be shown in OpenAPI.

                    It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                    Read more about it in the
                    [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).

                    And in the
                    [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                    """
                ),
            ] = None,
            callbacks: Annotated[
                Optional[List[BaseRoute]],
                Doc(
                    """
                    OpenAPI callbacks that should apply to all *path operations* in this
                    router.

                    It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                    Read more about it in the
                    [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
                    """
                ),
            ] = None,
            routes: Annotated[
                Optional[List[BaseRoute]],
                Doc(
                    """
                    **Note**: you probably shouldn't use this parameter, it is inherited
                    from Starlette and supported for compatibility.

                    ---

                    A list of routes to serve incoming HTTP and WebSocket requests.
                    """
                ),
                deprecated(
                    """
                    You normally wouldn't use this parameter with FastAPI, it is inherited
                    from Starlette and supported for compatibility.

                    In FastAPI, you normally would use the *path operation methods*,
                    like `router.get()`, `router.post()`, etc.
                    """
                ),
            ] = None,
            redirect_slashes: Annotated[
                bool,
                Doc(
                    """
                    Whether to detect and redirect slashes in URLs when the client doesn't
                    use the same format.
                    """
                ),
            ] = True,
            default: Annotated[
                Optional[ASGIApp],
                Doc(
                    """
                    Default function handler for this router. Used to handle
                    404 Not Found errors.
                    """
                ),
            ] = None,
            dependency_overrides_provider: Annotated[
                Optional[Any],
                Doc(
                    """
                    Only used internally by FastAPI to handle dependency overrides.

                    You shouldn't need to use it. It normally points to the `FastAPI` app
                    object.
                    """
                ),
            ] = None,
            route_class: Annotated[
                Type[APIRoute],
                Doc(
                    """
                    Custom route (*path operation*) class to be used by this router.

                    Read more about it in the
                    [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
                    """
                ),
            ] = APIRoute,
            on_startup: Annotated[
                Optional[Sequence[Callable[[], Any]]],
                Doc(
                    """
                    A list of startup event handler functions.

                    You should instead use the `lifespan` handlers.

                    Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                    """
                ),
            ] = None,
            on_shutdown: Annotated[
                Optional[Sequence[Callable[[], Any]]],
                Doc(
                    """
                    A list of shutdown event handler functions.

                    You should instead use the `lifespan` handlers.

                    Read more in the
                    [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                    """
                ),
            ] = None,
            # the generic to Lifespan[AppType] is the type of the top level application
            # which the router cannot know statically, so we use typing.Any
            lifespan: Annotated[
                Optional[Lifespan[Any]],
                Doc(
                    """
                    A `Lifespan` context manager handler. This replaces `startup` and
                    `shutdown` functions with a single context manager.

                    Read more in the
                    [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                    """
                ),
            ] = None,
            deprecated: Annotated[
                Optional[bool],
                Doc(
                    """
                    Mark all *path operations* in this router as deprecated.

                    It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                    Read more about it in the
                    [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                    """
                ),
            ] = None,
            include_in_schema: Annotated[
                bool,
                Doc(
                    """
                    To include (or not) all the *path operations* in this router in the
                    generated OpenAPI.

                    This affects the generated OpenAPI (e.g. visible at `/docs`).

                    Read more about it in the
                    [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-parameters-from-openapi).
                    """
                ),
            ] = True,
            generate_unique_id_function: Annotated[
                Callable[[APIRoute], str],
                Doc(
                    """
                    Customize the function used to generate unique IDs for the *path
                    operations* shown in the generated OpenAPI.

                    This is particularly useful when automatically generating clients or
                    SDKs for your API.

                    Read more about it in the
                    [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
                    """
                ),
            ] = Default(generate_unique_id),
            allowed_users: list[str] | None=None
        ):
        super().__init__(
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            lifespan=lifespan,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function
        )

        patch_router_get(self, allowed_users=allowed_users)


class SimpleRouter(APIRouter):
    def __init__(self, *args, allowed_users: list[str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        patch_router_get(self, allowed_users=allowed_users)


def patch_router_get(router: APIRouter, allowed_users: list[str] | None=None):
    prefix: str = router.prefix
    meta_dir = Path(__file__).parent.parent / 'meta' / prefix.split('/')[1]

    orig_get = router.get

    def _load_meta(path: str) -> dict:
        file = meta_dir / f'{path.strip('/').replace('/', '_')}.py'

        if not file.exists():
            return {}
        
        spec = importlib.util.spec_from_file_location('meta_module', file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        return getattr(mod, 'META', {})

    def get(path: str, **kwargs):
        meta = _load_meta(path)

        for k, v in meta.items():
            kwargs.setdefault(k, v)

        kwargs.setdefault('response_class', PlainTextResponse)
        kwargs.setdefault('deprecated', False)

        route_allowed_users: list[str] | None = kwargs.pop('allowed_users', None)
        route_allowed_users = route_allowed_users if route_allowed_users is not None else allowed_users

        def decorator(func):
            if route_allowed_users is None:
                return orig_get(path, **kwargs)(func)

            @wraps(func)
            async def wrapper(*args, **kwargs):
                # grab Request from args or kwargs
                request: Request | None = kwargs.get('request') or next(
                    (a for a in args if isinstance(a, Request)), None
                )

                if not request:
                    raise RuntimeError("Request is required to check allowed_users")

                user: str | None = getattr(request.state, 'user', None)
                
                if user not in route_allowed_users and user != 'admin':
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail=f'This endpoint is only authorized to {', '.join(allowed_users)}',
                        headers={'WWW-Authenticate': 'Basic'}
                    )
                
                return await func(*args, **kwargs)
            
            return orig_get(path, **kwargs)(wrapper)

        return decorator

    router.get = get
    return router


def patch_router_get_without_allowed_users(router: APIRouter):
    prefix: str = router.prefix
    meta_dir = Path(__file__).parent.parent / 'meta' / prefix.split('/')[1]

    orig_get = router.get

    def _load_meta(path: str) -> dict:
        file = meta_dir / f'{path.strip('/').replace('/', '_')}.py'

        if not file.exists():
            return {}
        
        spec = importlib.util.spec_from_file_location('meta_module', file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        return getattr(mod, 'META', {})

    def get(path: str, **kwargs):
        meta = _load_meta(path)

        for k, v in meta.items():
            kwargs.setdefault(k, v)

        kwargs.setdefault('response_class', PlainTextResponse)
        kwargs.setdefault('deprecated', False)

        return orig_get(path, **kwargs)

    router.get = get
    return router


def allowed_users(users: list[str]):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # get Request from args or kwargs
            request: Request | None = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if not request:
                request = kwargs.get('request')
            if not request:
                raise RuntimeError("Request parameter is required")

            user = getattr(request.state, 'user', None)

            if user == 'admin':
                return await func(*args, **kwargs)

            if user not in users:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"This endpoint is only authorized to {', '.join(users)}",
                    headers={'WWW-Authenticate': 'Basic'}
                )
            return await func(*args, **kwargs)
        return wrapper
    return decorator
