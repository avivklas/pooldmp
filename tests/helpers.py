"""Importable helpers (usable from spawned worker processes)."""

from __future__ import annotations

import multiprocessing.connection as mpc
import os
import pickle
import socket
import threading
from multiprocessing import AuthenticationError
from typing import Any


class EchoServer:
    """Threaded authenticated server answering each message with itself."""

    def __init__(self, authkey: bytes = b"k") -> None:
        self.listener = mpc.Listener(authkey=authkey)
        self.address = self.listener.address
        self.accepted = 0
        self.mute = False
        self._conns: list[mpc.Connection] = []
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while True:
            try:
                conn = self.listener.accept()
            except (OSError, AuthenticationError):
                return
            self.accepted += 1
            self._conns.append(conn)
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: mpc.Connection) -> None:
        try:
            while True:
                message = conn.recv()
                if not self.mute:
                    conn.send(message)
        except (EOFError, OSError, TypeError, pickle.UnpicklingError):
            pass

    def kill_connections(self) -> None:
        """Server-side close of every accepted connection (simulates restart)."""
        for conn in self._conns:
            with socket.socket(fileno=os.dup(conn.fileno())) as sock:
                sock.shutdown(socket.SHUT_RDWR)
        self._conns.clear()

    def close(self) -> None:
        self.listener.close()
        self.kill_connections()


def append_to(proxy: Any, value: int) -> int:
    proxy.append(value)
    return len(proxy)


def make_and_drop_lists(address: Any, authkey: bytes, count: int) -> int:
    """Worker: attach to a manager by address and churn proxies."""
    from pooledmp import PooledSyncManager

    manager = PooledSyncManager(address=address, authkey=authkey)
    manager.connect()
    for _ in range(count):
        manager.list()
    return count
