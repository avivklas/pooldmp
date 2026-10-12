from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from multiprocessing.connection import Connection

import pytest

from pooledmp import ConnectionPool, PoolConfig

from .helpers import EchoServer


@pytest.fixture
def server() -> Iterator[EchoServer]:
    srv = EchoServer()
    yield srv
    srv.close()


def make_pool(server: EchoServer, max_idle: int = 4) -> ConnectionPool:
    return ConnectionPool(server.address, b"k", PoolConfig(max_idle=max_idle))


def roundtrip(conn: Connection) -> object:
    conn.send("x")
    return conn.recv()


def test_connection_is_reused(server: EchoServer) -> None:
    pool = make_pool(server)
    for _ in range(20):
        with pool.lease() as conn:
            assert roundtrip(conn) == "x"
    assert server.accepted == 1
    assert pool.stats.opened == 1
    assert pool.stats.reused == 19


def test_max_idle_caps_retained_connections(server: EchoServer) -> None:
    pool = make_pool(server, max_idle=2)
    held = [pool.acquire()[0] for _ in range(5)]
    for conn in held:
        pool.release(conn)
    assert pool.idle_count == 2
    assert sum(c.closed for c in held) == 3


def test_stale_connection_is_detected_and_replaced(server: EchoServer) -> None:
    pool = make_pool(server)
    with pool.lease() as conn:
        roundtrip(conn)
    server.kill_connections()
    with pool.lease() as conn:
        assert roundtrip(conn) == "x"
    assert pool.stats.stale_detected == 1
    assert pool.stats.opened == 2


def test_ping_runs_after_idle_threshold(server: EchoServer) -> None:
    pings: list[object] = []
    pool = ConnectionPool(
        server.address, b"k", PoolConfig(ping_after=0.0), ping=lambda c: pings.append(c)
    )
    with pool.lease():
        pass
    with pool.lease():
        pass
    assert len(pings) == 1
    assert pool.stats.pings == 1


def test_failing_ping_discards_connection(server: EchoServer) -> None:
    def bad_ping(_: object) -> None:
        raise ConnectionError

    pool = ConnectionPool(server.address, b"k", PoolConfig(ping_after=0.0), ping=bad_ping)
    with pool.lease():
        pass
    with pool.lease():
        pass
    assert pool.stats.opened == 2


def test_lease_discards_on_error(server: EchoServer) -> None:
    pool = make_pool(server)
    with pytest.raises(RuntimeError), pool.lease():
        raise RuntimeError
    assert pool.idle_count == 0


def test_concurrent_use_is_thread_safe(server: EchoServer) -> None:
    pool = make_pool(server, max_idle=4)
    errors: list[BaseException] = []

    def worker(n: int) -> None:
        try:
            for i in range(200):
                with pool.lease() as conn:
                    conn.send((n, i))
                    assert conn.recv() == (n, i)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert pool.stats.opened <= 8
    assert pool.idle_count <= 4
    assert server.accepted == pool.stats.opened


def test_close_closes_idle_and_stops_retaining(server: EchoServer) -> None:
    pool = make_pool(server)
    conn, _ = pool.acquire()
    pool.release(conn)
    pool.close()
    assert conn.closed
    other, _ = pool.acquire()
    pool.release(other)
    assert other.closed
    assert pool.idle_count == 0


def test_wrong_authkey_is_rejected(server: EchoServer) -> None:
    pool = ConnectionPool(server.address, b"wrong")
    with pytest.raises(Exception, match="auth|digest|Authentication"):
        pool.acquire()


def test_fork_child_does_not_reuse_parent_connections(server: EchoServer) -> None:
    pool = make_pool(server)
    with pool.lease() as conn:
        roundtrip(conn)
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child
        try:
            conn, reused = pool.acquire()
            os.write(w, b"1" if reused else b"0")
        finally:
            os._exit(0)
    os.waitpid(pid, 0)
    os.close(w)
    assert os.read(r, 1) == b"0"
    os.close(r)
    with pool.lease() as conn:  # parent's connection must still be healthy
        assert roundtrip(conn) == "x"
    assert pool.stats.opened == 1


def test_negative_max_idle_rejected() -> None:
    with pytest.raises(ValueError):
        PoolConfig(max_idle=-1)
