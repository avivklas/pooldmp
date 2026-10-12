"""Manager server that keeps control connections open between requests."""

from __future__ import annotations

from multiprocessing import connection
from multiprocessing.connection import Connection
from multiprocessing.managers import Server
from traceback import format_exc
from typing import Any

from .protocol import DEDICATING_METHODS


class PooledServer(Server):
    """``multiprocessing.managers.Server`` with persistent control channels.

    Stock ``Server.handle_request`` authenticates, serves exactly one request
    and closes the socket, spawning a thread per request.  This variant
    authenticates once and then loops, so a pooled client can reuse the same
    socket (and server thread) for many control requests.  It stays wire
    compatible: a stock client that closes after one request simply ends the
    loop with EOF.
    """

    authkey: Any

    def handle_request(self, conn: Connection) -> None:
        try:
            self._serve_connection(conn)
        except SystemExit:
            # serve_client() exits via SystemExit on EOF, as in stock.
            pass
        finally:
            conn.close()

    def _serve_connection(self, conn: Connection) -> None:
        try:
            connection.deliver_challenge(conn, self.authkey)
            connection.answer_challenge(conn, self.authkey)
        except Exception:
            self._try_send(conn, ("#TRACEBACK", format_exc()))
            return
        while not self._is_stopping():
            try:
                request = conn.recv()
            except (EOFError, OSError):
                return
            except Exception:
                self._try_send(conn, ("#TRACEBACK", format_exc()))
                return
            if not self._serve_one(conn, request):
                return

    def _serve_one(self, conn: Connection, request: Any) -> bool:
        """Serve one request; return ``False`` once the socket is spent."""
        funcname: str | None = None
        try:
            _, funcname, args, kwds = request
            if funcname not in self.public:
                raise AssertionError(f"{funcname!r} unrecognized")
            result = getattr(self, funcname)(conn, *args, **kwds)
            msg: tuple[str, Any] = ("#RETURN", result)
        except Exception:
            msg = ("#TRACEBACK", format_exc())
        if funcname in DEDICATING_METHODS and msg[0] == "#RETURN":
            # accept_connection ran serve_client to completion; shutdown has
            # already replied itself.  Either way the channel is finished.
            return False
        return self._try_send(conn, msg)

    def _try_send(self, conn: Connection, msg: tuple[str, Any]) -> bool:
        try:
            conn.send(msg)
        except Exception:
            try:
                conn.send(("#TRACEBACK", format_exc()))
            except Exception:
                return False
        return True

    def _is_stopping(self) -> bool:
        stop_event = getattr(self, "stop_event", None)
        return bool(stop_event is not None and stop_event.is_set())
