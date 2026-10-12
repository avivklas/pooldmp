from __future__ import annotations

import multiprocessing as mp
import multiprocessing.managers as mpm
import pickle
import threading
from collections.abc import Iterator
from multiprocessing.connection import Client
from typing import Any, cast

import pytest

import pooledmp
from pooledmp import POOLED_SERIALIZER, PooledSyncManager, get_pool, pool_stats
from pooledmp.diagnostics import (
    count_connects,
    manager_address,
    manager_authkey,
    number_of_objects,
)

from .helpers import append_to, make_and_drop_lists


@pytest.fixture
def manager() -> Iterator[PooledSyncManager]:
    m = pooledmp.Manager()
    yield m
    m.shutdown()


def stats_for(manager: PooledSyncManager) -> dict[str, int]:
    return pool_stats()[manager_address(manager)]


def test_proxy_semantics_match_stock(manager: PooledSyncManager) -> None:
    d = manager.dict()
    lst = manager.list()
    d["a"] = 1
    lst.extend([1, 2, 3])
    assert d.copy() == {"a": 1}
    assert list(lst) == [1, 2, 3]
    with pytest.raises(KeyError):
        d["missing"]
    ns = manager.Namespace()
    ns.x = 5
    assert ns.x == 5
    q = manager.Queue()
    q.put("hi")
    assert q.get() == "hi"


def test_proxy_lifecycle_reuses_one_control_connection(manager: PooledSyncManager) -> None:
    with count_connects() as connects:
        for _ in range(100):
            manager.list()
    # stock opens 4 connections per list (create/incref/decref/decref).
    assert connects["n"] <= 1
    assert number_of_objects(manager) == 0


def test_stock_manager_opens_one_connection_per_control_request() -> None:
    with mp.Manager() as m, count_connects() as connects:
        for _ in range(25):
            m.list()
    assert connects["n"] >= 100


def test_refcounting_still_frees_objects(manager: PooledSyncManager) -> None:
    lst = manager.list()
    assert number_of_objects(manager) == 1
    del lst
    assert number_of_objects(manager) == 0


def test_stock_client_can_talk_to_pooled_server() -> None:
    m = PooledSyncManager(serializer="pickle")
    m.start()
    try:
        lst = m.list()
        lst.append(1)
        assert list(lst) == [1]
        with Client(manager_address(m), authkey=manager_authkey(m)) as conn:
            conn.send((None, "number_of_objects", (), {}))
            assert conn.recv() == ("#RETURN", 1)
    finally:
        m.shutdown()


def test_multiple_requests_on_one_raw_connection(manager: PooledSyncManager) -> None:
    key = manager_authkey(manager)
    with Client(manager_address(manager), authkey=key) as conn:
        for _ in range(5):
            conn.send((None, "number_of_objects", (), {}))
            assert conn.recv() == ("#RETURN", 0)
        conn.send((None, "nope", (), {}))
        kind, text = conn.recv()
        assert kind == "#TRACEBACK" and "unrecognized" in text
        conn.send((None, "number_of_objects", (), {}))
        assert conn.recv() == ("#RETURN", 0)


def test_threads_share_small_pool(manager: PooledSyncManager) -> None:
    errors: list[BaseException] = []

    def work() -> None:
        try:
            for i in range(50):
                lst = manager.list([i])
                assert lst[0] == i
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    pool = get_pool(manager_address(manager), manager_authkey(manager))
    assert pool.idle_count <= pool.config.max_idle
    # Stock would dial ~5 connections per iteration (2000); pooling leaves at
    # most the per-iteration data connection a proxy opens for its first call.
    assert stats_for(manager)["opened"] <= 8 * 50
    assert number_of_objects(manager) == 0


def test_nested_proxies_and_iterators(manager: PooledSyncManager) -> None:
    d = manager.dict({"a": 1, "b": 2})
    assert sorted(iter(d)) == ["a", "b"]
    assert sorted(d.keys()) == ["a", "b"]


def test_manager_shutdown_closes_pool() -> None:
    m = pooledmp.Manager()
    m.list()
    address = m.address
    assert address in pool_stats()
    m.shutdown()
    assert address not in pool_stats()


def test_pool_recovers_when_server_connections_die(manager: PooledSyncManager) -> None:
    manager.list()
    pool = get_pool(manager_address(manager), manager_authkey(manager))
    assert pool.idle_count == 1
    for entry in list(pool._idle):
        entry.conn.close()
    lst = manager.list([7])
    assert lst[0] == 7


def test_tcp_address_manager_is_pooled() -> None:
    m = PooledSyncManager(address=("127.0.0.1", 0))
    m.start()
    try:
        assert isinstance(m.address, tuple)
        with count_connects() as connects:
            for _ in range(50):
                m.list()
        assert connects["n"] <= 1
    finally:
        m.shutdown()


def test_wrong_authkey_cannot_attach(manager: PooledSyncManager) -> None:
    other = PooledSyncManager(address=manager.address, authkey=b"wrong")
    with pytest.raises(Exception, match="auth|digest|Authentication"):
        other.connect()


def test_serializer_name_unpickles_to_registered_key() -> None:
    restored = pickle.loads(pickle.dumps(POOLED_SERIALIZER))
    assert restored == POOLED_SERIALIZER
    assert restored in cast(Any, mpm).listener_client


@pytest.mark.parametrize("method", ["fork", "spawn", "forkserver"])
def test_proxies_work_in_pool_workers(manager: PooledSyncManager, method: str) -> None:
    ctx = mp.get_context(method)
    lst = manager.list()
    with ctx.Pool(3) as pool:
        pool.starmap(append_to, [(lst, i) for i in range(30)])
    assert sorted(lst) == list(range(30))


def test_multiple_processes_attach_by_address(manager: PooledSyncManager) -> None:
    ctx = mp.get_context("spawn")
    key = manager_authkey(manager)
    with ctx.Pool(2) as pool:
        done = pool.starmap(make_and_drop_lists, [(manager.address, key, 40)] * 4)
    assert done == [40] * 4
    assert number_of_objects(manager) == 0


def test_pooled_manager_custom_registration() -> None:
    class Counter:
        def __init__(self) -> None:
            self.n = 0

        def bump(self) -> int:
            self.n += 1
            return self.n

    class MyManager(pooledmp.PooledManager):
        pass

    MyManager.register("Counter", Counter)
    m: Any = MyManager()
    m.start()
    try:
        c = m.Counter()
        assert [c.bump() for _ in range(3)] == [1, 2, 3]
    finally:
        m.shutdown()


def test_data_channel_is_recycled_across_proxy_lifetimes(manager: PooledSyncManager) -> None:
    with count_connects() as connects:
        for i in range(60):
            lst = manager.list()
            lst.append(i)
            assert lst[0] == i
            del lst
    # one control + one data connection serve all 60 proxy lifetimes
    assert connects["n"] <= 2
    pool = get_pool(manager_address(manager), manager_authkey(manager))
    assert pool.data_idle_count == 1


def test_stock_opens_a_data_connection_per_proxy_lifetime() -> None:
    with mp.Manager() as m, count_connects() as connects:
        for i in range(20):
            lst = m.list()
            lst.append(i)
            del lst
    assert connects["n"] >= 20 * 5


def test_dead_parked_data_channel_is_replaced(manager: PooledSyncManager) -> None:
    lst = manager.list()
    lst.append(1)
    del lst
    pool = get_pool(manager_address(manager), manager_authkey(manager))
    for entry in list(pool._data_idle):
        entry.conn.close()
    lst = manager.list()
    lst.append(2)
    assert list(lst) == [2]
    assert pool.stats.stale_detected >= 1


class Tricky:
    """Exposes method names that collide with manager control names."""

    def shutdown(self) -> str:
        return "mine"

    def dummy(self) -> str:
        return "dummy"

    def accept_connection(self) -> str:
        return "accept"


def test_user_methods_named_like_control_methods(manager: PooledSyncManager) -> None:
    class TrickyManager(pooledmp.PooledManager):
        pass

    TrickyManager.register("Tricky", Tricky)
    m: Any = TrickyManager()
    m.start()
    try:
        for _ in range(3):
            t = m.Tricky()
            assert (t.shutdown(), t.dummy(), t.accept_connection()) == ("mine", "dummy", "accept")
            del t
        assert number_of_objects(m) == 0
    finally:
        m.shutdown()


def test_data_channel_reconnects_when_adopted_channel_died(manager: PooledSyncManager) -> None:
    lst = manager.list()
    lst.append(1)
    handle = cast(Any, lst)._tls.connection
    pool = get_pool(manager_address(manager), manager_authkey(manager))
    # Simulate an idle-looking data channel whose socket silently fails on send.
    real = handle._conn

    class FailingOnce:
        closed = False

        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def send(self, obj: Any) -> None:
            raise BrokenPipeError

        def close(self) -> None:
            real.close()

    handle._conn = FailingOnce()
    handle._reused = True
    lst.append(2)
    assert list(lst) == [1, 2]
    assert pool.stats.stale_detected >= 1


def test_forked_child_does_not_share_parent_channels(manager: PooledSyncManager) -> None:
    lst = manager.list()
    lst.append("parent")
    proc = mp.get_context("fork").Process(target=append_to, args=(lst, 1))
    proc.start()
    proc.join(10)
    assert proc.exitcode == 0
    lst.append("parent-again")
    assert list(lst) == ["parent", 1, "parent-again"]
