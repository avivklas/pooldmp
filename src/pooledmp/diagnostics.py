"""Introspection helpers for tests and benchmarks.

They centralise access to private ``multiprocessing`` attributes that are not
covered by the standard library type stubs.
"""

from __future__ import annotations

import multiprocessing.connection as mpc
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from multiprocessing.managers import BaseManager
from typing import Any, cast


@contextmanager
def count_connects() -> Iterator[Counter[str]]:
    """Count client-side dials made via ``multiprocessing.connection.Client``.

    Every ``Client()`` goes through ``SocketClient`` for AF_UNIX and AF_INET,
    so this counts connections regardless of who opens them (stock or pooled).
    The counter's ``"n"`` key holds the total.
    """
    counter: Counter[str] = Counter()
    module = cast(Any, mpc)
    real: Callable[[Any], Any] = module.SocketClient

    def counting(address: Any) -> Any:
        counter["n"] += 1
        return real(address)

    module.SocketClient = counting
    try:
        yield counter
    finally:
        module.SocketClient = real


def manager_address(manager: BaseManager) -> Any:
    """Address of a started or connected manager."""
    address = manager.address
    if address is None:
        raise ValueError("manager is not started")
    return address


def manager_authkey(manager: BaseManager) -> bytes:
    return bytes(cast(Any, manager)._authkey)


def number_of_objects(manager: BaseManager) -> int:
    """Number of live shared objects in the manager server."""
    return int(cast(Any, manager)._number_of_objects())
