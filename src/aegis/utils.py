import asyncio
import inspect
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from inspect import (
    iscoroutinefunction,
)
from typing import Any


def _unwrapped_call(call: Callable[..., Any] | None) -> Any:
    if call is None:
        return call  # pragma: no cover
    unwrapped = inspect.unwrap(_impartial(call))
    return unwrapped


def _impartial(func: Callable[..., Any]) -> Callable[..., Any]:
    while isinstance(func, partial):
        func = func.func
    return func


def is_coroutine_callable(call: Callable[..., Any] | None) -> bool:
    if call is None:
        return False  # pragma: no cover
    if inspect.isroutine(_impartial(call)) and iscoroutinefunction(_impartial(call)):
        return True
    if inspect.isroutine(_unwrapped_call(call)) and iscoroutinefunction(
        _unwrapped_call(call)
    ):
        return True
    if inspect.isclass(_unwrapped_call(call)):
        return False
    dunder_call = getattr(_impartial(call), "__call__", None)  # noqa: B004
    if dunder_call is None:
        return False  # pragma: no cover
    if iscoroutinefunction(_impartial(dunder_call)) or iscoroutinefunction(
        _unwrapped_call(dunder_call)
    ):
        return True
    dunder_unwrapped_call = getattr(_unwrapped_call(call), "__call__", None)  # noqa: B004
    if dunder_unwrapped_call is None:
        return False  # pragma: no cover
    if iscoroutinefunction(_impartial(dunder_unwrapped_call)) or iscoroutinefunction(
        _unwrapped_call(dunder_unwrapped_call)
    ):
        return True
    return False


async def invoke_callable(
    call: Callable[..., Any],
    executor: ThreadPoolExecutor | None = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    if is_coroutine_callable(call):
        return await call(*args, **kwargs)
    else:
        return await asyncio.get_running_loop().run_in_executor(
            executor, partial(call, *args, **kwargs)
        )
