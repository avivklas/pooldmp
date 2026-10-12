"""A small, thread-safe, fork-aware pool of long-lived connections."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from multiprocessing import connection
from multiprocessing.connection import Connection
from typing import Any

Address = Any
Pinger = Callable[[Connection], None]


@dataclass(frozen=True)
class PoolConfig:
    """Tunables for a :class:`ConnectionPool`.

    Attributes:
        max_idle: Idle connections kept per pool; surplus ones are closed.
        ping_after: Idle seconds after which a connection is verified with the
            pool's ``ping`` callback before reuse.  ``None`` disables pings
            (the cheap ``poll(0)`` EOF check always runs).
    """

    max_idle: int = 4
    ping_after: float | None = 30.0

    def __post_init__(self) -> None:
        if self.max_idle < 0:
            raise ValueError("max_idle must be >= 0")


@dataclass
class PoolStats:
    """Counters used by tests and benchmarks."""

    opened: int = 0
    reused: int = 0
    released: int = 0
    discarded: int = 0
    stale_detected: int = 0
    pings: int = 0

    def snapshot(self) -> dict[str, int]:
        return dict(vars(self))


@dataclass
class _Idle:
    conn: Connection
    last_used: float = field(default_factory=time.monotonic)


class ConnectionPool:
    """Pool of authenticated connections to one server address.

    ``acquire`` hands out an exclusive connection (reusing an idle healthy one
    if possible, otherwise dialling a new one); ``release`` returns it.  The
    pool never blocks: concurrency is bounded only by callers, while at most
    ``config.max_idle`` connections are retained between uses.

    Locking uses an ``RLock`` and tiny critical sections because ``release``
    can be re-entered from ``__del__`` during garbage collection on a thread
    that already holds the lock.
    """

    def __init__(
        self,
        address: Address,
        authkey: bytes | None = None,
        config: PoolConfig | None = None,
        ping: Pinger | None = None,
    ) -> None:
        self.address = address
        self._authkey = authkey
        self.config = config or PoolConfig()
        self._ping = ping
        self.stats = PoolStats()
        self._idle: deque[_Idle] = deque()
        self._data_idle: deque[_Idle] = deque()
        self._lock = threading.RLock()
        self._closed = False
        self._pid = os.getpid()

    @property
    def idle_count(self) -> int:
        return len(self._idle)

    @property
    def data_idle_count(self) -> int:
        return len(self._data_idle)

    @property
    def closed(self) -> bool:
        return self._closed

    def connect_new(self) -> Connection:
        """Dial a brand new connection, bypassing the idle list."""
        conn = connection.Client(self.address, authkey=self._authkey)
        self.stats.opened += 1
        return conn

    def acquire(self) -> tuple[Connection, bool]:
        """Return ``(connection, reused)``; ``reused`` means it was idle."""
        self._reset_if_forked()
        while True:
            entry = self._pop_idle()
            if entry is None:
                return self.connect_new(), False
            if self._is_healthy(entry):
                self.stats.reused += 1
                return entry.conn, True
            self.stats.stale_detected += 1
            self.discard(entry.conn)

    def release(self, conn: Connection) -> None:
        """Return a connection that is at a message boundary to the pool."""
        self._store(conn, self._idle)

    def acquire_data(self) -> Connection | None:
        """Take an idle connection that the server already serves as a
        per-thread data channel, or ``None`` if there is none healthy."""
        self._reset_if_forked()
        while True:
            with self._lock:
                entry = self._data_idle.pop() if self._data_idle else None
            if entry is None:
                return None
            if self._is_healthy(entry, ping=False):
                self.stats.reused += 1
                return entry.conn
            self.stats.stale_detected += 1
            self.discard(entry.conn)

    def release_data(self, conn: Connection) -> None:
        """Park a data channel (server side still in ``serve_client``)."""
        self._store(conn, self._data_idle)

    def _store(self, conn: Connection, idle: deque[_Idle]) -> None:
        self._reset_if_forked()
        if conn.closed:
            return
        with self._lock:
            keep = not self._closed and len(idle) < self.config.max_idle
            if keep:
                idle.append(_Idle(conn))
                self.stats.released += 1
        if not keep:
            self.discard(conn)

    def discard(self, conn: Connection) -> None:
        """Close a connection that must not be reused."""
        self.stats.discarded += 1
        with contextlib.suppress(OSError):
            conn.close()

    @contextmanager
    def lease(self) -> Iterator[Connection]:
        """Borrow a connection; it is discarded if the body raises."""
        conn, _ = self.acquire()
        try:
            yield conn
        except BaseException:
            self.discard(conn)
            raise
        else:
            self.release(conn)

    def close(self) -> None:
        """Close all idle connections and refuse to retain new ones."""
        with self._lock:
            self._closed = True
            idle = [*self._idle, *self._data_idle]
            self._idle, self._data_idle = deque(), deque()
        for entry in idle:
            self.discard(entry.conn)

    def _pop_idle(self) -> _Idle | None:
        with self._lock:
            return self._idle.pop() if self._idle else None  # LIFO: keep hot

    def _is_healthy(self, entry: _Idle, ping: bool = True) -> bool:
        conn = entry.conn
        try:
            if conn.closed or conn.poll(0):
                # Any readable byte on an idle connection means EOF or garbage.
                return False
            idle_for = time.monotonic() - entry.last_used
            ping_after = self.config.ping_after
            if (
                ping
                and self._ping is not None
                and ping_after is not None
                and idle_for >= ping_after
            ):
                self.stats.pings += 1
                self._ping(conn)
        except (OSError, EOFError, ConnectionError):
            return False
        return True

    def _reset_if_forked(self) -> None:
        # Connections inherited across fork() are shared with the parent; the
        # child must forget them (closing only drops the child's descriptor).
        if os.getpid() == self._pid:
            return
        self._lock = threading.RLock()
        inherited = [*self._idle, *self._data_idle]
        self._idle, self._data_idle = deque(), deque()
        self._pid = os.getpid()
        for entry in inherited:
            self.discard(entry.conn)


_pools: dict[tuple[Any, bytes | None], ConnectionPool] = {}
_pools_lock = threading.Lock()
default_config = PoolConfig()


def _reinit_registry_lock() -> None:
    global _pools_lock
    _pools_lock = threading.Lock()


os.register_at_fork(after_in_child=_reinit_registry_lock)


def get_pool(address: Address, authkey: bytes | None, ping: Pinger | None = None) -> ConnectionPool:
    """Return the process-wide pool for ``(address, authkey)``, creating it."""
    key = (address, bytes(authkey) if authkey is not None else None)
    with _pools_lock:
        pool = _pools.get(key)
        if pool is None:
            pool = ConnectionPool(address, authkey, default_config, ping)
            _pools[key] = pool
        return pool


def close_pool(address: Address) -> None:
    """Close and forget every pool targeting ``address``."""
    with _pools_lock:
        doomed = [k for k in _pools if k[0] == address]
        pools = [_pools.pop(k) for k in doomed]
    for pool in pools:
        pool.close()


def pool_stats() -> dict[Any, dict[str, int]]:
    """Counters of every live pool in this process, keyed by address."""
    with _pools_lock:
        return {k[0]: p.stats.snapshot() for k, p in _pools.items()}
