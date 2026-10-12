"""Why is a stock manager over TCP ~44 ms per dial? Test the Nagle hypothesis."""

import multiprocessing.connection as mpc
import socket
import time
from multiprocessing.managers import SyncManager

real_client = mpc.SocketClient
real_accept = mpc.SocketListener.accept


def nodelay_client(address):
    conn = real_client(address)
    s = socket.socket(fileno=__import__("os").dup(conn.fileno()))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.detach()
    return conn


def time_churn(label: str, n: int = 30) -> None:
    m = SyncManager(address=("127.0.0.1", 0))
    m.start()
    m.list()
    t = time.perf_counter()
    for _ in range(n):
        m.list()
    print(f"{label:28} {(time.perf_counter() - t) / n * 1e6:10.0f} us per create/destroy")
    m.shutdown()


if __name__ == "__main__":
    time_churn("stock, Nagle on")
    mpc.SocketClient = nodelay_client
    time_churn("client TCP_NODELAY only")
