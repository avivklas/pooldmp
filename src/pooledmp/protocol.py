"""Knowledge about the ``multiprocessing.managers`` wire protocol.

A manager request is a 4-tuple ``(ident, methodname, args, kwds)`` and the
reply is ``(kind, result)``.  The pool itself is protocol agnostic; everything
protocol specific lives here so that it can be audited in one place.
"""

from __future__ import annotations

from multiprocessing.connection import Connection
from typing import Any, Final

#: Requests after which the server stops treating the socket as a
#: request/response channel.  ``accept_connection`` turns it into a dedicated
#: per-thread ``serve_client`` loop; ``shutdown`` ends the server.  Such a
#: connection must never be returned to the pool.
DEDICATING_METHODS: Final = frozenset({"accept_connection", "shutdown"})

#: Requests that can safely be replayed on a fresh connection if the reply was
#: lost because a reused connection turned out to be dead.
IDEMPOTENT_METHODS: Final = frozenset({"dummy", "debug_info", "number_of_objects", "get_methods"})


def control_method(request: Any) -> str | None:
    """Return the server method of a *control* request, else ``None``.

    Control requests carry ``ident=None`` and address the server itself.  Data
    requests carry the id of a shared object and a user-chosen method name
    (which may collide with control names such as ``shutdown``), so they must
    never be interpreted here.
    """
    if isinstance(request, tuple) and len(request) == 4 and request[0] is None:
        name = request[1]
        if isinstance(name, str):
            return name
    return None


def ping(conn: Connection) -> None:
    """Round-trip the server's no-op ``dummy`` method; raise if unhealthy."""
    conn.send((None, "dummy", (), {}))
    kind, _ = conn.recv()
    if kind != "#RETURN":
        raise ConnectionError(f"unexpected ping reply {kind!r}")
