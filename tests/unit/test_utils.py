"""Unit tests for utils module."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest

from aegis.utils import invoke_callable, is_coroutine_callable


class TestIsCoroutineCallable:
    """Tests for is_coroutine_callable function."""

    def test_async_function(self) -> None:
        """Test detection of async functions."""

        async def async_func() -> int:
            return 1

        assert is_coroutine_callable(async_func) is True

    def test_sync_function(self) -> None:
        """Test detection of sync functions."""

        def sync_func() -> int:
            return 1

        assert is_coroutine_callable(sync_func) is False

    def test_async_lambda_partial(self) -> None:
        """Test detection of partial wrapped async function."""

        async def async_func(x: int) -> int:
            return x

        wrapped = partial(async_func, 1)
        assert is_coroutine_callable(wrapped) is True

    def test_sync_partial(self) -> None:
        """Test detection of partial wrapped sync function."""

        def sync_func(x: int) -> int:
            return x

        wrapped = partial(sync_func, 1)
        assert is_coroutine_callable(wrapped) is False

    def test_class_with_async_call(self) -> None:
        """Test class with async __call__ method."""

        class AsyncCallable:
            async def __call__(self) -> int:
                return 1

        obj = AsyncCallable()
        assert is_coroutine_callable(obj) is True

    def test_class_with_sync_call(self) -> None:
        """Test class with sync __call__ method."""

        class SyncCallable:
            def __call__(self) -> int:
                return 1

        obj = SyncCallable()
        assert is_coroutine_callable(obj) is False

    def test_none_returns_false(self) -> None:
        """Test None input returns False."""
        assert is_coroutine_callable(None) is False

    def test_class_type_returns_false(self) -> None:
        """Test class type (not instance) returns False."""

        class MyClass:
            pass

        assert is_coroutine_callable(MyClass) is False


class TestInvokeCallable:
    """Tests for invoke_callable function."""

    @pytest.mark.asyncio
    async def test_invoke_async_function(self) -> None:
        """Test invoking async function."""

        async def async_func(x: int) -> int:
            return x * 2

        result = await invoke_callable(async_func, None, 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_invoke_sync_function(self) -> None:
        """Test invoking sync function in executor."""

        def sync_func(x: int) -> int:
            return x * 3

        result = await invoke_callable(sync_func, None, 5)
        assert result == 15

    @pytest.mark.asyncio
    async def test_invoke_with_kwargs(self) -> None:
        """Test invoking with keyword arguments."""

        async def async_func(x: int, multiplier: int = 1) -> int:
            return x * multiplier

        result = await invoke_callable(async_func, None, 5, multiplier=4)
        assert result == 20

    @pytest.mark.asyncio
    async def test_invoke_sync_with_executor(self) -> None:
        """Test invoking sync function with custom executor."""

        def sync_func(x: int) -> int:
            return x + 10

        with ThreadPoolExecutor(max_workers=1) as executor:
            result = await invoke_callable(sync_func, executor, 5)
            assert result == 15

    @pytest.mark.asyncio
    async def test_invoke_async_callable_class(self) -> None:
        """Test invoking async callable class instance."""

        class AsyncMultiplier:
            def __init__(self, factor: int) -> None:
                self.factor = factor

            async def __call__(self, x: int) -> int:
                return x * self.factor

        multiplier = AsyncMultiplier(5)
        result = await invoke_callable(multiplier, None, 3)
        assert result == 15

    @pytest.mark.asyncio
    async def test_invoke_sync_callable_class(self) -> None:
        """Test invoking sync callable class instance."""

        class SyncAdder:
            def __init__(self, offset: int) -> None:
                self.offset = offset

            def __call__(self, x: int) -> int:
                return x + self.offset

        adder = SyncAdder(100)
        result = await invoke_callable(adder, None, 5)
        assert result == 105
