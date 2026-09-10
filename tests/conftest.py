"""
Global pytest configuration and fixtures for C2 Wireless PoC test suite.
Provides native execution for async test functions without requiring third-party plugins.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
import pytest


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    """
    Hook to execute coroutine test functions seamlessly using asyncio.run().
    Enables native `async def test_*()` without external plugins.
    """
    testfunction = pyfuncitem.obj
    if inspect.iscoroutinefunction(testfunction):
        argnames = pyfuncitem._fixtureinfo.argnames
        funcargs = pyfuncitem.funcargs
        testargs = {arg: funcargs[arg] for arg in argnames}
        asyncio.run(testfunction(**testargs))
        return True
    return None
