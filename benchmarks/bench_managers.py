"""Benchmark stock ``multiprocessing`` managers against ``pooledmp``.

Run: ``python -m benchmarks.bench_managers [--quick] [--json out.json]``
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import multiprocessing.connection as mpc
import statistics
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from multiprocessing.managers import SyncManager
from typing import Any

import pooledmp
from pooledmp.diagnostics import count_connects, manager_address, manager_authkey

Kind = str  # "stock" | "pooled"
Family = str  # "unix" | "tcp"


@dataclass
class Result:
    scenario: str
    family: Family
    kind: Kind
    ops: int
    seconds: float
    connects: int
    time_wait: int | None

    @property
    def us_per_op(self) -> float:
        return self.seconds / self.ops * 1e6

    @property
    def ops_per_s(self) -> float:
        return self.ops / self.seconds


def make_manager(kind: Kind, family: Family) -> SyncManager:
    address = ("127.0.0.1", 0) if family == "tcp" else None
    cls = pooledmp.PooledSyncManager if kind == "pooled" else SyncManager
    manager = cls(address=address)
    manager.start()
    return manager


def attach(kind: Kind, address: Any, authkey: bytes) -> SyncManager:
    cls = pooledmp.PooledSyncManager if kind == "pooled" else SyncManager
    manager = cls(address=address, authkey=authkey)
    manager.connect()
    return manager


def time_wait_sockets(port: int) -> int:
    """Count loopback TCP sockets in TIME_WAIT involving ``port``."""
    total = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path) as fh:
                lines = fh.read().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            local_port = int(fields[1].split(":")[1], 16)
            remote_port = int(fields[2].split(":")[1], 16)
            if fields[3] == "06" and port in (local_port, remote_port):
                total += 1
    return total


def churn_lists(manager: SyncManager, n: int) -> None:
    for _ in range(n):
        manager.list()


def churn_lists_with_call(manager: SyncManager, n: int) -> None:
    for _ in range(n):
        manager.list().append(1)


def churn_iterators(manager: SyncManager, n: int) -> None:
    d = manager.dict({"a": 1})
    for _ in range(n):
        iter(d)


def method_calls(manager: SyncManager, n: int) -> None:
    lst = manager.list()
    for i in range(n):
        lst.append(i)


def threaded(
    workload: Callable[[SyncManager, int], None], threads: int
) -> Callable[[SyncManager, int], None]:
    def run(manager: SyncManager, n: int) -> None:
        pool = [
            threading.Thread(target=workload, args=(manager, n // threads)) for _ in range(threads)
        ]
        for t in pool:
            t.start()
        for t in pool:
            t.join()

    return run


def _worker(kind: Kind, address: Any, authkey: bytes, n: int) -> int:
    with count_connects() as connects:
        manager = attach(kind, address, authkey)
        churn_lists(manager, n)
    return int(connects["n"])


@dataclass
class Timed:
    """Self-measured outcome for workloads that span several processes."""

    seconds: float
    connects: int


def multiprocess(procs: int) -> Callable[[SyncManager, int], Timed]:
    def run(manager: SyncManager, n: int) -> Timed:
        kind = "pooled" if isinstance(manager, pooledmp.PooledSyncManager) else "stock"
        ctx = mp.get_context("spawn")
        key = manager_authkey(manager)
        with ctx.Pool(procs) as pool:
            address = manager_address(manager)
            warmup = sum(pool.starmap(_worker, [(kind, address, key, 1)] * procs))
            started = time.perf_counter()
            counts = pool.starmap(_worker, [(kind, address, key, n // procs)] * procs)
            elapsed = time.perf_counter() - started
        return Timed(elapsed, warmup + sum(counts))

    return run


def run_scenario(
    name: str, workload: Callable[[SyncManager, int], Any], kind: Kind, family: Family, n: int
) -> Result:
    manager = make_manager(kind, family)
    try:
        workload(manager, max(10, n // 20))  # warm up
        with count_connects() as connects:
            started = time.perf_counter()
            returned = workload(manager, n)
            elapsed = time.perf_counter() - started
        total_connects = connects["n"]
        if isinstance(returned, Timed):
            elapsed, total_connects = returned.seconds, returned.connects
        wait = time_wait_sockets(manager_address(manager)[1]) if family == "tcp" else None
        return Result(name, family, kind, n, elapsed, total_connects, wait)
    finally:
        manager.shutdown()


def micro_handshake(n: int) -> dict[str, float]:
    """Cost of one authenticated dial versus one pooled ``dummy`` round trip."""
    manager = make_manager("pooled", "unix")
    try:
        key = manager_authkey(manager)
        started = time.perf_counter()
        for _ in range(n):
            with mpc.Client(manager_address(manager), authkey=key) as conn:
                conn.send((None, "dummy", (), {}))
                conn.recv()
        dial = (time.perf_counter() - started) / n * 1e6
        with mpc.Client(manager_address(manager), authkey=key) as conn:
            started = time.perf_counter()
            for _ in range(n):
                conn.send((None, "dummy", (), {}))
                conn.recv()
            reuse = (time.perf_counter() - started) / n * 1e6
        return {"dial_auth_request_us": dial, "reused_request_us": reuse}
    finally:
        manager.shutdown()


def collect(quick: bool) -> tuple[list[Result], dict[str, float]]:
    n = 300 if quick else 3000
    repeats = 1 if quick else 3
    # (workload, ops, shrink ops on TCP?).  The data-plane scenario does no
    # per-op dialling, so it keeps its full size on TCP for a stable timing.
    scenarios: dict[str, tuple[Callable[[SyncManager, int], Any], int, bool]] = {
        "proxy create/destroy": (churn_lists, n, True),
        "proxy create+call+destroy": (churn_lists_with_call, n, True),
        "iterator proxy (#PROXY reply)": (churn_iterators, n, True),
        "proxy method call (data plane)": (method_calls, n * 10, False),
        "8 threads create/destroy": (threaded(churn_lists, 8), n, True),
        "multiprocess x4 create/destroy": (multiprocess(4), n, True),
    }
    results: list[Result] = []
    for family in ("unix", "tcp"):
        for name, (workload, base_ops, shrink) in scenarios.items():
            # Stock TCP is orders of magnitude slower per op; keep runs bounded.
            ops = max(40, base_ops // 20) if family == "tcp" and shrink else base_ops
            for kind in ("stock", "pooled"):
                runs = [run_scenario(name, workload, kind, family, ops) for _ in range(repeats)]
                best = sorted(runs, key=lambda r: r.seconds)[len(runs) // 2]
                best.connects = int(statistics.median(r.connects for r in runs))
                waits = [r.time_wait for r in runs if r.time_wait is not None]
                best.time_wait = int(statistics.median(waits)) if waits else None
                results.append(best)
                print(
                    f"{family:4} {kind:6} {name:34} {best.us_per_op:9.1f} us/op "
                    f"connects={best.connects}",
                    flush=True,
                )
    return results, micro_handshake(500 if quick else 3000)


def render_markdown(results: list[Result], micro: dict[str, float]) -> str:
    out: list[str] = []
    for family in ("unix", "tcp"):
        out.append(
            f"\n### {'AF_UNIX (default)' if family == 'unix' else 'AF_INET (TCP loopback)'}\n"
        )
        header = (
            "| scenario | stock us/op | pooled us/op | speedup | stock connects | pooled connects |"
        )
        if family == "tcp":
            header += " stock TIME_WAIT | pooled TIME_WAIT |"
        out.append(header)
        out.append("|" + "---|" * (header.count("|") - 1))
        names = dict.fromkeys(r.scenario for r in results)
        for name in names:
            s = next(
                r for r in results if (r.scenario, r.family, r.kind) == (name, family, "stock")
            )
            p = next(
                r for r in results if (r.scenario, r.family, r.kind) == (name, family, "pooled")
            )
            row = (
                f"| {name} | {s.us_per_op:.0f} | {p.us_per_op:.0f} | "
                f"{s.us_per_op / p.us_per_op:.1f}x | {s.connects} | {p.connects} |"
            )
            if family == "tcp":
                row += f" {s.time_wait} | {p.time_wait} |"
            out.append(row)
    out.append(
        f"\nMicro: authenticated dial + 1 request = {micro['dial_auth_request_us']:.0f} us; "
        f"1 request on a reused connection = {micro['reused_request_us']:.0f} us."
    )
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--json", help="write raw results to this path")
    args = parser.parse_args()
    results, micro = collect(args.quick)
    print(render_markdown(results, micro))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"results": [asdict(r) for r in results], "micro": micro}, fh, indent=2)


if __name__ == "__main__":
    main()
