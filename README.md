# pooledmp: pooled control connections for `multiprocessing` managers

Prototype that replaces the *ephemeral* connections in
`multiprocessing.managers` with a small pool of long-lived, health-checked
connections. Standalone package, no CPython fork or global monkey patch.

## 1. Does the premise hold? Partly.

Verified against CPython 3.12.3 (`/usr/lib/python3.12/multiprocessing`) and by
counting real dials (`scripts/probe_stock.py`, which wraps
`connection.SocketClient`, the single choke point of `Client()`).

| Component | Transport | Dials per use |
|---|---|---|
| `Pool`, `Queue`, `SimpleQueue`, `Pipe`, `Lock` | anonymous pipes / `socketpair` / semaphores, inherited at fork or spawn | **none** (`Pool.map` of 1000 items: 0 dials) |
| `connection.Listener()` / `Client()` default | `default_family` is **`AF_UNIX`** on POSIX, `AF_PIPE` on Windows. `AF_INET` only if you pass a `(host, port)` address (`connection.py:48-57`) | n/a |
| `Manager()` (`managers.py`) | `AF_UNIX` by default, TCP only for a user-supplied tuple address (remote managers) | **one new connection, new auth handshake and new server thread for every control request**: `create`, `incref`, `decref` (twice per proxy lifetime), `#PROXY` replies, `debug_info`, `number_of_objects`, `shutdown`. `_callmethod` data calls already use one persistent connection per thread, but it is closed when the last proxy in the thread dies, so the next proxy dials again |
| `resource_sharer` (sending fds / `Connection` objects via `DupFd` when not spawning) | `Listener()` default, so `AF_UNIX` | one dial per received handle (by design: one-shot key). Not pooled here |

Measured: `manager.dict()` plus 100 item sets cost 4 dials; 50 x
`manager.list()` create/destroy cost **200 dials** (4 per proxy lifetime).

So:

* "Uses ephemeral TCP connections": **false by default**. Local IPC is pipes
  or `AF_UNIX`. Pool, Queue, Pipe already use long-lived channels.
* "Ephemeral connections instead of reuse": **true for the Manager control
  plane** (the part people hit when proxies are created, passed to workers or
  garbage collected) and for `resource_sharer`.
* **TCP is where it hurts, by orders of magnitude.** On a TCP manager
  (`address=("127.0.0.1", 0)`) each stock dial costs about 44 ms on Linux
  loopback. The cause is Nagle plus delayed ACK on the multi-message auth
  handshake. Setting `TCP_NODELAY` on the client socket alone brings a stock
  create/destroy from 176 ms to 1.1 ms (`scripts/probe_tcp_nagle.py`). Each
  dial also leaves a `TIME_WAIT` socket (see table below).

### What this means for a `DictProxy`
Stock dials per operation (AF_UNIX, `scripts/probe_dictproxy.py`):
| Operation | Stock dials |
|---|---|
| `d = m.dict()` | 3 (`create`, `incref`, internal `decref`) |
| first method call on `d` from a thread | 1 (`accept_connection`) |
| `d[k]`, `d[k] = v`, `len(d)`, `d.copy()`, `k in d`, `d.keys()` | 0 |
| `iter(d)` / `for k in d` (returns an Iterator proxy) | 2, plus 1 when freed |
| `del d` | 1 (`decref`) |
| pass `d` to a pool task (unpickle, first call, free) | 3 per task |
Each dial also costs an auth handshake and a server thread (about 220 us on
AF_UNIX, about 44 ms on TCP). A long-lived proxy used heavily is fine on
stock; the cost is in creating, passing per task, iterating and destroying
proxies. Cheap fixes without this package: pass the proxy once via a `Pool`
initializer, and prefer `d.items()`/`d.copy()` over `for k in d`. With the
pool these steady-state dials drop to 0 (the first control and data
connections are one-time).

## 2. Design

```
 proxy / manager code (stock, unmodified)
        |  self._Client(address, authkey=...)       <- serializer "pooled-pickle"
        v
 PooledConnection  --acquire/release-->  ConnectionPool (per address+authkey, per process)
        |                                   idle list (control)   data list (parked serve_client sockets)
        v
 PooledServer (manager process): authenticate once, then loop serving control requests
```

* **Hook point.** `PooledSyncManager`/`PooledManager` select a registered
  serializer name, `"pooled-pickle"`, whose `Client` is `pooled_client`.
  Stock proxy code reads `listener_client[serializer][1]`, so every
  `create/incref/decref/#PROXY/accept_connection` call goes through the pool
  without copying any private stdlib logic. The serializer key is a `str`
  subclass whose `__reduce__` re-imports this package on unpickle, so proxies
  passed to `spawn`/`forkserver` workers register the serializer themselves.
* **`PooledServer`** (set via the existing `BaseManager._Server` hook):
  authenticates once, then serves requests in a loop until EOF. This is wire
  compatible: a stock client that closes after one request just ends the loop.
* **`ConnectionPool`**: LIFO idle list (`max_idle=4` by default), tiny `RLock`
  critical sections (release can run from `__del__` during GC), fork-aware
  (a child drops inherited sockets instead of sharing them with the parent),
  no blocking and no hard cap on concurrently leased connections.
* **Health checks.** `poll(0)` on every reuse (readable idle socket means
  EOF or garbage, so discard); optional ping (`dummy` request) after
  `ping_after=30s` idle; failing connections are discarded.
* **Reconnect.** A reused connection that fails on *send* is replayed on a
  fresh one, redoing the `accept_connection` handshake in data mode. A lost
  *reply* is replayed only for idempotent control requests (`dummy`,
  `debug_info`, `number_of_objects`, `get_methods`). Everything else raises, as
  stock does, so `incref`/`decref` are never applied twice.
* **Two pooled kinds of channel.** Control sockets (request/response) and
  *data* sockets (the per-thread socket the server serves from a
  `serve_client` thread after `accept_connection`). A data socket is parked
  on close and re-adopted by the next `accept_connection`, which is answered
  locally because the server is already in that state. That removes the
  per-proxy-lifetime dial.
* **Clean shutdown.** `manager.shutdown()` closes and forgets the pool for its
  address; `close_pool`/`ConnectionPool.close` are available directly.
  Control requests are recognised only when `ident is None`, so user methods
  named `shutdown`, `dummy` or `accept_connection` on shared objects are safe
  (tested).

```python
import pooledmp

with pooledmp.Manager() as m:  # drop-in for multiprocessing.Manager()
    d = m.dict()
    ...
```

Custom managers: subclass `pooledmp.PooledManager` (or `PooledSyncManager`) and
call `register(...)` as usual. For remote managers pass `address=(host, port)`.

## 3. Benchmarks

`python -m benchmarks.bench_managers` (PYTHONPATH=src:.), Linux x86-64, 8 vCPU,
CPython 3.12.3, median of 3 runs, time per operation after a warm-up. "connects"
counts dials in the measured window (multiprocess rows include each worker's
warm-up dial, spawned workers). Raw data: `benchmarks/results.json`, log:
`benchmarks/results.log`.

### AF_UNIX (default)

| scenario | stock us/op | pooled us/op | speedup | stock dials | pooled dials |
|---|---|---|---|---|---|
| proxy create/destroy | 869 | 139 | 6.2x | 12000 | 0 |
| proxy create+call+destroy | 1092 | 191 | 5.7x | 15000 | 0 |
| iterator proxy (`#PROXY` reply) | 695 | 143 | 4.9x | 9005 | 0 |
| proxy method call (data plane) | 22 | 22 | 1.0x | 5 | 0 |
| 8 threads create/destroy | 686 | 163 | 4.2x | 12000 | 4 |
| 4 processes create/destroy | 708 | 90 | 7.8x | 12024 | 4 |

### AF_INET (TCP loopback), reduced op counts because stock is slow

| scenario | stock us/op | pooled us/op | speedup | stock dials | pooled dials | stock TIME_WAIT | pooled TIME_WAIT |
|---|---|---|---|---|---|---|---|
| proxy create/destroy | 176132 | 158 | 1118x | 600 | 0 | 640 | 0 |
| proxy create+call+destroy | 220185 | 210 | 1048x | 750 | 0 | 769 | 0 |
| iterator proxy (`#PROXY` reply) | 133571 | 158 | 846x | 455 | 0 | 490 | 0 |
| proxy method call (data plane) | 32 | 25 | 1.3x | 5 | 0 | 10 | 0 |
| 8 threads create/destroy | 21866 | 385 | 57x | 576 | 4 | 610 | 8 |
| 4 processes create/destroy | 44585 | 103 | 433x | 616 | 4 | 672 | 8 |

Micro (AF_UNIX): authenticated dial plus one request = 221 us; one request on
an already-open connection = 23 us.

Read these honestly:

* The win is on the **control plane** (proxy create, pass, destroy), not on
  method calls, which were already on a persistent per-thread connection (1.0x).
* On the default `AF_UNIX` the gain is about 4 to 8x per proxy operation.
  The 1000x TCP figure is mostly the Nagle/delayed-ACK stall; a one-line
  `TCP_NODELAY` fix to stock would bring stock to about 1.1 ms (still about 7x
  slower than pooled at 150 us).
* "Pooled dials: 0" is steady state; the pool dials once in the warm-up.
  Thread concurrency raises the count only as far as peak simultaneous use.
* Stock `Pool`, `Queue` and `Pipe` were not benchmarked because they dial
  nothing; there is nothing to pool.

## 4. Tests, lint, types

```
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest            # 47 tests, ~1 s
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/mypy              # strict
```

Covers: pool reuse, `max_idle`, stale detection, ping, thread safety (8
threads), fork isolation, wrong authkey, close; handle semantics (GC release,
in-flight and dedicated handling, reconnect-on-send, idempotent replay vs
non-replay); server loop with stock and raw clients; proxy semantics;
refcounts and object freeing; dial counts stock vs pooled; data-channel
recycling and recovery from dead parked channels; AF_INET; `fork`, `spawn` and
`forkserver` workers receiving proxies; spawned processes attaching by
address; custom registered managers; name collisions with control methods.

## 5. Limitations

* Python 3.12 tested only. Relies on private-but-stable hooks:
  `managers.listener_client`, `BaseManager._Server`, `_finalize_manager`,
  and the manager wire protocol. Needs a re-check per CPython release.
* Only the Manager control and data planes are pooled. `resource_sharer` still
  dials once per received handle; sharing a handle is inherently one-shot and
  would need a protocol change.
* Server cost: each pooled socket holds one server thread (at most `max_idle`
  control plus `max_idle` parked data sockets per client process, plus one
  data socket per thread with live proxies). Stock uses short-lived threads
  instead.
* No hard cap or backpressure on simultaneous connections (by design, to avoid
  deadlocks); concurrency beyond `max_idle` dials and closes on release.
* The server must be a `PooledServer` for pooling to take effect. A stock
  server works with `PooledSyncManager(serializer="pickle")` (no pooling).
  A pooled client against a stock server would break, because the stock server
  closes after each request.
* The pooled serializer must be imported in every process that uses these
  proxies; unpickling a proxy does that automatically.
* Windows (`AF_PIPE`) untested. TCP_NODELAY is not set by the pool. The first
  dial per pooled connection still pays the handshake.

## 6. Recommended next steps

1. Upstream the cheap fixes to CPython: set `TCP_NODELAY` in
   `SocketClient`/`SocketListener.accept` (huge win for TCP managers), and cache
   or reuse control connections in `BaseProxy._incref/_decref`.
2. Do not rewrite `Pool`/`Queue`; they already use persistent channels.
3. If keeping this package: add an `atexit`/weakref close for pools, a
   `max_connections` option with timeouts, and optional pooling for
   `resource_sharer`; run the suite against 3.10 to 3.13 in CI.
4. Measure your real workload first: if it creates few proxies, stock is
   already fine. The gain is proportional to proxy churn and process count.
