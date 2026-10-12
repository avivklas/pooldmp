"""Empirically count where stock multiprocessing dials new connections."""

import collections
import multiprocessing as mp
import multiprocessing.connection as mpc
import multiprocessing.resource_sharer as rs
import os
from typing import Any

counts: collections.Counter[str] = collections.Counter()
real_socket_client = mpc.SocketClient


def counting_client(address: Any) -> Any:
    counts[f"{mpc.address_type(address)} dial"] += 1
    return real_socket_client(address)


def report(label: str) -> None:
    print(f"{label:46} {dict(counts) or '{} (no dials)'}")
    counts.clear()


def main() -> None:
    mpc.SocketClient = counting_client  # type: ignore[assignment]
    print("default_family on this platform:", mpc.default_family)
    with mp.Manager() as manager:
        print("manager address:", manager.address, "->", mpc.address_type(manager.address))
        counts.clear()
        shared = manager.dict()
        for i in range(100):
            shared[i] = i
        report("manager.dict() + 100 setitem calls")
        for _ in range(50):
            manager.list()
        report("50x manager.list() create+destroy")
    with mp.Pool(2) as pool:
        counts.clear()
        pool.map(abs, range(1000))
        report("Pool.map over 1000 items")
    queue: Any = mp.Queue()
    queue.put(1)
    queue.get()
    report("Queue put/get")
    read_fd, write_fd = os.pipe()
    ident = rs.DupFd(read_fd)
    for _ in range(1):
        ident.detach()
    report("resource_sharer.DupFd(fd).detach() (1 receive)")
    os.close(write_fd)


if __name__ == "__main__":
    main()
