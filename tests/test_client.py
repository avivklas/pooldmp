from __future__ import annotations

from collections.abc import Iterator
from multiprocessing.connection import Connection
from typing import cast

import pytest

from pooledmp import ConnectionPool, PooledConnection
from pooledmp.pool import _Idle

from .helpers import EchoServer


@pytest.fixture
def server() -> Iterator[EchoServer]:
    srv = EchoServer()
    yield srv
    srv.close()


def request(name: str) -> tuple[None, str, tuple[()], dict[str, object]]:
    return (None, name, (), {})


def test_close_returns_connection_to_pool(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle.send(request("incref"))
    assert handle.recv() == request("incref")
    handle.close()
    assert pool.idle_count == 1
    handle.close()  # idempotent
    assert pool.idle_count == 1


def test_garbage_collection_returns_connection(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle.send(request("incref"))
    handle.recv()
    del handle
    assert pool.idle_count == 1


def test_in_flight_request_is_not_pooled(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle.send(request("incref"))
    handle.close()
    assert pool.idle_count == 0


@pytest.mark.parametrize("name", ["accept_connection", "shutdown"])
def test_dedicating_requests_are_not_pooled(server: EchoServer, name: str) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle.send(request(name))
    handle.recv()
    handle.close()
    assert pool.idle_count == 0


def test_raw_byte_access_marks_connection_dedicated(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle.send_bytes(b"raw")
    handle.close()
    assert pool.idle_count == 0


class HalfDeadConnection:
    """Looks idle and healthy to ``poll`` but fails on first send."""

    closed = False

    def poll(self, timeout: float = 0) -> bool:
        return False

    def send(self, obj: object) -> None:
        raise BrokenPipeError

    def close(self) -> None:
        self.closed = True


def test_send_on_dead_reused_connection_reconnects(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    pool._idle.append(_Idle(cast(Connection, HalfDeadConnection())))
    handle = PooledConnection(pool)
    handle.send(request("incref"))
    assert handle.recv() == request("incref")
    assert pool.stats.stale_detected == 1
    assert pool.stats.opened == 1


def test_send_failure_on_fresh_connection_is_raised(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    handle = PooledConnection(pool)
    handle._conn = cast(Connection, HalfDeadConnection())
    with pytest.raises(BrokenPipeError):
        handle.send(request("incref"))
    handle.close()
    assert pool.idle_count == 0


def test_idempotent_request_is_replayed_after_lost_reply(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    warm = PooledConnection(pool)
    warm.send(request("dummy"))
    warm.recv()
    warm.close()
    handle = PooledConnection(pool)
    server.mute = True
    handle.send(request("dummy"))
    server.kill_connections()
    server.mute = False
    assert handle.recv() == request("dummy")


def test_non_idempotent_request_is_not_replayed(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    warm = PooledConnection(pool)
    warm.send(request("incref"))
    warm.recv()
    warm.close()
    handle = PooledConnection(pool)
    server.mute = True
    handle.send(request("incref"))
    server.kill_connections()
    with pytest.raises((EOFError, OSError)):
        handle.recv()
    handle.close()
    assert pool.idle_count == 0


def test_use_after_close_raises(server: EchoServer) -> None:
    handle = PooledConnection(ConnectionPool(server.address, b"k"))
    handle.close()
    assert handle.closed
    with pytest.raises(OSError):
        handle.send(request("incref"))


def test_context_manager_closes(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"k")
    with PooledConnection(pool) as handle:
        handle.send(request("incref"))
        handle.recv()
    assert pool.idle_count == 1
