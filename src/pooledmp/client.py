"""Client-side connection handle that returns itself to the pool on close."""

from __future__ import annotations

import contextlib
import os
from multiprocessing.connection import Connection
from types import TracebackType
from typing import Any

from .pool import Address, ConnectionPool, get_pool
from .protocol import IDEMPOTENT_METHODS, control_method, ping

_PASSTHROUGH = frozenset({"poll", "fileno", "closed", "readable", "writable"})
_ACCEPT_REPLY = ("#RETURN", None)


class PooledConnection:
    """Drop-in for a ``multiprocessing.connection.Connection`` used by managers.

    A handle is in one of two modes:

    * *control* mode: strict request/response with the server itself
      (``create``, ``incref``, ``decref``...).  ``close()`` -- or garbage
      collection, which stock manager code relies on -- returns the socket to
      the pool's idle list unless a request is still in flight.
    * *data* mode: entered by ``accept_connection``.  The server now serves the
      socket from a dedicated ``serve_client`` thread for proxy method calls.
      Instead of dialling again for the next proxy, ``close()`` parks the socket
      in the pool's data list and the next ``accept_connection`` adopts it
      (replying locally, as the server is already in that state).

    Reconnect semantics: if a *reused* connection fails while sending, nothing
    reached the server, so the request is replayed on a fresh connection (after
    re-running the ``accept_connection`` handshake in data mode).  If it fails
    while waiting for the reply, only idempotent control requests are
    replayed; anything else surfaces the error exactly like stock.
    """

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
        self._conn: Connection | None = None
        self._reused = False
        self._pending: Any = None
        self._in_flight = False
        self._broken = False
        self._discard_on_close = False
        self._accept_request: Any = None
        self._synthetic_reply: Any = None
        self._pid = os.getpid()
        self._conn, self._reused = pool.acquire()

    @property
    def _data_mode(self) -> bool:
        return self._accept_request is not None

    def send(self, obj: Any) -> None:
        conn = self._require()
        name = control_method(obj)
        if name == "accept_connection" and not self._data_mode:
            self._accept_request = obj
            if self._adopt_data_channel():
                return
        elif name == "shutdown":
            self._discard_on_close = True
        try:
            conn.send(obj)
        except OSError:
            if not self._reused:
                self._broken = True
                raise
            conn = self._reconnect(replay_accept=obj is not self._accept_request)
            conn.send(obj)
        self._pending = obj
        self._in_flight = True

    def recv(self) -> Any:
        if self._synthetic_reply is not None:
            reply, self._synthetic_reply = self._synthetic_reply, None
            return reply
        conn = self._require()
        try:
            message = conn.recv()
        except (EOFError, OSError):
            replayable = (
                self._reused
                and not self._data_mode
                and control_method(self._pending) in IDEMPOTENT_METHODS
            )
            if not replayable:
                self._broken = True
                self._in_flight = False
                raise
            conn = self._reconnect(replay_accept=False)
            conn.send(self._pending)
            message = conn.recv()
        self._in_flight = False
        self._reused = False
        return message

    def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        inherited = self._pid != os.getpid()
        if self._discard_on_close or self._in_flight or self._broken or inherited:
            self._pool.discard(conn)
        elif self._data_mode:
            self._pool.release_data(conn)
        else:
            self._pool.release(conn)

    @property
    def closed(self) -> bool:
        return self._conn is None or self._conn.closed

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        conn = self._require()
        if name not in _PASSTHROUGH:
            # Raw byte access bypasses request tracking, so the socket can no
            # longer be assumed to sit at a message boundary.
            self._discard_on_close = True
        return getattr(conn, name)

    def __enter__(self) -> PooledConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.close()

    def _require(self) -> Connection:
        if self._conn is None:
            raise OSError("handle is closed")
        return self._conn

    def _adopt_data_channel(self) -> bool:
        """Swap in a parked data channel; the handshake is answered locally."""
        data_conn = self._pool.acquire_data()
        if data_conn is None:
            return False
        self._pool.release(self._require())
        self._conn = data_conn
        self._reused = True
        self._synthetic_reply = _ACCEPT_REPLY
        return True

    def _reconnect(self, replay_accept: bool) -> Connection:
        self._pool.discard(self._require())
        self._pool.stats.stale_detected += 1
        conn = self._conn = self._pool.connect_new()
        self._reused = False
        if replay_accept and self._data_mode:
            conn.send(self._accept_request)
            conn.recv()
        return conn


def pooled_client(
    address: Address, family: str | None = None, authkey: bytes | None = None
) -> PooledConnection:
    """Pooled replacement for ``multiprocessing.connection.Client``.

    ``family`` is accepted for signature compatibility; it is inferred from the
    address exactly as stock does.
    """
    return PooledConnection(get_pool(address, authkey, ping=ping))
