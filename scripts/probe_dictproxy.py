"""Dials per DictProxy operation, stock vs pooled (steady state, AF_UNIX)."""

import multiprocessing as mp
import multiprocessing.connection as mpc
from typing import Any

import pooledmp

dials = 0
real_client = mpc.SocketClient


def counting_client(address: Any) -> Any:
    global dials
    dials += 1
    return real_client(address)


mpc.SocketClient = counting_client  # type: ignore[assignment]


def step(label: str, fn: Any) -> Any:
    before = dials
    result = fn()
    print(f"  {label:52} {dials - before}")
    return result


def task_total(proxy: Any) -> int:
    proxy["k"] = 1
    proxy["k"]
    return dials  # includes the incref done while unpickling the argument


def run(title: str, manager: Any) -> None:
    print(title)
    d = step("d = m.dict()", manager.dict)
    step("d['a'] = 1   (first call from this thread)", lambda: d.__setitem__("a", 1))
    step(
        "d['a'] = 2 / d['a'] / len(d) / d.copy()  (x100)",
        lambda: [(d.__setitem__("a", i), d["a"], len(d), d.copy()) for i in range(100)],
    )
    it = step("it = iter(d)         (returns a new Iterator proxy)", lambda: iter(d))
    step("next(it)", lambda: next(it))
    before = dials
    del it
    print(f"  {'del it                    (decref)':52} {dials - before}")
    before = dials
    del d
    print(f"  {'del d                     (decref + close data conn)':52} {dials - before}")
    d = manager.dict()
    with mp.get_context("fork").Pool(1, initializer=lambda: None) as pool:
        pool.apply(task_total, (d,))  # warm
        totals = [pool.apply(task_total, (d,)) for _ in range(6)]
    deltas = [b - a for a, b in zip(totals, totals[1:], strict=False)]
    print(f"  Pool.apply(f, (d,)) in a long-lived worker: dials per task = {deltas}")


if __name__ == "__main__":
    run("STOCK", mp.Manager())
    run("POOLED", pooledmp.Manager())
