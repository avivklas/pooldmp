"""Manager classes that use pooled control-plane connections."""

from __future__ import annotations

from multiprocessing import connection
from multiprocessing import managers as _mp_managers
from multiprocessing.context import BaseContext
from multiprocessing.managers import BaseManager, SyncManager
from typing import Any, cast

from .client import pooled_client
from .pool import close_pool
from .server import PooledServer


class _SerializerName(str):
    """Serializer key that re-registers itself when unpickled.

    Proxies pickle their serializer name.  Under ``spawn``/``forkserver`` the
    receiving process may not have imported this package yet; unpickling this
    object imports it, which registers the serializer before the proxy is
    rebuilt.
    """

    __slots__ = ()

    def __reduce__(self) -> tuple[Any, tuple[()]]:
        return (_pooled_serializer_name, ())


def _pooled_serializer_name() -> str:
    return POOLED_SERIALIZER


POOLED_SERIALIZER = _SerializerName("pooled-pickle")
cast(Any, _mp_managers).listener_client[POOLED_SERIALIZER] = (connection.Listener, pooled_client)


class _PooledMixin(BaseManager):
    """Selects the pooled serializer and server, and cleans up the pool."""

    _Server = PooledServer

    def __init__(
        self,
        address: Any = None,
        authkey: bytes | None = None,
        serializer: str = POOLED_SERIALIZER,
        ctx: BaseContext | None = None,
        *,
        shutdown_timeout: float = 1.0,
    ) -> None:
        super().__init__(address, authkey, serializer, ctx, shutdown_timeout=shutdown_timeout)

    @staticmethod
    def _finalize_manager(
        process: Any,
        address: Any,
        authkey: Any,
        state: Any,
        client_cls: Any,
        shutdown_timeout: float,
    ) -> None:
        try:
            cast(Any, BaseManager)._finalize_manager(
                process, address, authkey, state, client_cls, shutdown_timeout
            )
        finally:
            close_pool(address)


class PooledManager(_PooledMixin):
    """``BaseManager`` whose proxies and manager reuse pooled connections."""


class PooledSyncManager(_PooledMixin, SyncManager):
    """``SyncManager`` (``dict``, ``list``, ``Queue``, ...) with pooling."""


def Manager(ctx: BaseContext | None = None) -> PooledSyncManager:
    """Pooled counterpart of ``multiprocessing.Manager()``; returns it started."""
    manager = PooledSyncManager(ctx=ctx)
    manager.start()
    return manager


__all__ = ["POOLED_SERIALIZER", "Manager", "PooledManager", "PooledSyncManager"]
