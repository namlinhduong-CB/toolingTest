"""
target.py - works out which Analytics cluster we talk to, and provides a small
thread-safe HTTP client for the Analytics query/admin and management REST APIs.

There are two ways to get a Target:

  * EA_repro (default): read EA_repro's `.ea_repro_state.json` (next to
    ea_repro.py, or in EA_repro/dist when built with PyInstaller). Hosts, ports,
    credentials, node container names and the S3/MinIO details all come from it.
  * Any cluster: --host/--username/--password (plus --query-port / --mgmt-port /
    --tls). Analytics nodes are discovered from /pools/default.

No third-party dependencies.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import ssl
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
EA_REPRO_DIR = HERE.parent / "EA_repro"


# --------------------------------------------------------------------------- #
# Terminal helpers (same look as ea_repro.py)
# --------------------------------------------------------------------------- #

def _c(code: str, s: str) -> str:
    if not sys.stdout.isatty():
        return s
    return f"\033[{code}m{s}\033[0m"


def info(msg: str) -> None:
    print(f"{_c('36', '›')} {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"{_c('32', '✓')} {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"{_c('33', '!')} {msg}", flush=True)


def die(msg: str, code: int = 1):
    print(f"{_c('31', '✗')} {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def step(msg: str) -> None:
    print(f"\n{_c('1;35', '■')} {_c('1', msg)}", flush=True)


class CbasError(RuntimeError):
    pass


# --------------------------------------------------------------------------- #
# HTTP: per-thread keep-alive connections
# --------------------------------------------------------------------------- #

class Http:
    """Keep-alive HTTP(S) client. One connection per (thread, base URL)."""

    def __init__(self, username: str, password: str, *, insecure: bool = True):
        tok = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.auth = f"Basic {tok}"
        self._local = threading.local()
        self._ctx = ssl.create_default_context()
        if insecure:
            self._ctx.check_hostname = False
            self._ctx.verify_mode = ssl.CERT_NONE

    def _conn(self, base: str, timeout: float) -> http.client.HTTPConnection:
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = self._local.conns = {}
        c = conns.get(base)
        if c is None:
            u = urllib.parse.urlsplit(base)
            if u.scheme == "https":
                c = http.client.HTTPSConnection(u.hostname, u.port or 443,
                                                timeout=timeout, context=self._ctx)
            else:
                c = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=timeout)
            conns[base] = c
        c.timeout = timeout
        if c.sock is not None:
            c.sock.settimeout(timeout)
        return c

    def _drop(self, base: str) -> None:
        conns = getattr(self._local, "conns", {})
        c = conns.pop(base, None)
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    def request(self, method: str, url: str, *, form: dict | None = None,
                body: bytes | None = None, timeout: float = 30.0,
                headers: dict | None = None) -> tuple[int, bytes]:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}"
        path = u.path + (f"?{u.query}" if u.query else "")
        hdrs = {"Authorization": self.auth, "Connection": "keep-alive"}
        if form is not None:
            body = urllib.parse.urlencode(form).encode()
            hdrs["Content-Type"] = "application/x-www-form-urlencoded"
        if headers:
            hdrs.update(headers)
        last: Exception | None = None
        for attempt in range(2):  # one silent retry on a stale keep-alive socket
            c = self._conn(base, timeout)
            try:
                c.request(method, path, body=body, headers=hdrs)
                r = c.getresponse()
                data = r.read()
                if r.getheader("Connection", "").lower() == "close":
                    self._drop(base)
                return r.status, data
            except (http.client.RemoteDisconnected, BrokenPipeError,
                    ConnectionResetError, http.client.CannotSendRequest,
                    http.client.ResponseNotReady) as e:
                self._drop(base)
                last = e
                if attempt == 0:
                    continue
                raise
            except Exception as e:  # timeouts, connection refused etc.
                self._drop(base)
                raise
        raise last  # pragma: no cover

    def get_json(self, url: str, timeout: float = 10.0):
        st, data = self.request("GET", url, timeout=timeout)
        if st >= 400:
            raise CbasError(f"GET {url} → HTTP {st}: {data[:200]!r}")
        return json.loads(data or b"null")


# --------------------------------------------------------------------------- #
# Target
# --------------------------------------------------------------------------- #

@dataclass
class Target:
    query_urls: list[str]          # analytics service base URLs (one per node)
    mgmt_url: str                  # cluster manager base URL
    username: str
    password: str
    kind: str = "ea"               # "ea" (Enterprise Analytics / Columnar) | "server"
    ea: dict | None = None         # EA_repro state, when the target came from it
    ea_state_path: str | None = None
    http: Http = field(init=False, repr=False)
    _rr: int = field(default=0, init=False, repr=False)
    _rr_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self):
        self.http = Http(self.username, self.password)

    # ---- identity ---------------------------------------------------------
    @property
    def from_ea_repro(self) -> bool:
        return self.ea is not None

    @property
    def project(self) -> str | None:
        return self.ea.get("project") if self.ea else None

    def container(self, service: str) -> str:
        if not self.ea:
            raise CbasError("docker actions need an EA_repro target")
        return f"{self.ea['project']}-{service}"

    def node_services(self) -> list[str]:
        if not self.ea:
            return []
        return [f"ea-node{i}" for i in range(1, int(self.ea.get("nodes", 1)) + 1)]

    @property
    def docker_network(self) -> str:
        return f"{self.ea['project']}_default"

    def describe(self) -> str:
        src = f"EA_repro project '{self.project}'" if self.ea else "explicit host"
        return (f"{self.kind} via {src}: query={', '.join(self.query_urls)} "
                f"mgmt={self.mgmt_url} user={self.username}")

    # ---- default scope / naming ------------------------------------------
    def default_scope(self) -> str:
        return "Default.Default" if self.kind == "ea" else "Default"

    def next_query_url(self) -> str:
        with self._rr_lock:
            u = self.query_urls[self._rr % len(self.query_urls)]
            self._rr += 1
        return u

    # ---- SQL++ -------------------------------------------------------------
    def query(self, statement: str, *, timeout: float = 600.0,
              client_context_id: str | None = None, url: str | None = None,
              params: dict | None = None) -> dict:
        base = url or self.query_urls[0]
        form = {"statement": statement,
                "timeout": f"{int(max(1, timeout))}s"}
        if client_context_id:
            form["client_context_id"] = client_context_id
        if params:
            form.update(params)
        st, data = self.http.request("POST", f"{base}/analytics/service",
                                     form=form, timeout=timeout + 15)
        try:
            resp = json.loads(data or b"{}")
        except json.JSONDecodeError:
            resp = {"status": "http-error",
                    "errors": [{"code": st, "msg": f"HTTP {st}: {data[:300]!r}"}]}
        resp.setdefault("_http_status", st)
        return resp

    def q(self, statement: str, *, timeout: float = 600.0) -> list:
        """Run a statement and return results, raising CbasError on failure."""
        resp = self.query(statement, timeout=timeout)
        if resp.get("status") != "success":
            raise CbasError(first_error(resp) + f"\n  statement: {statement[:400]}")
        return resp.get("results", [])

    def q_any(self, statements: list[str], *, timeout: float = 600.0,
              ok_if: tuple[str, ...] = ()) -> tuple[str, list]:
        """Tries each statement in order, since the syntax differs between EA and
        Server Analytics, and returns the first that succeeds. Errors whose
        message contains any of `ok_if` count as success (e.g. 'already exists')."""
        errs = []
        for s in statements:
            resp = self.query(s, timeout=timeout)
            if resp.get("status") == "success":
                return s, resp.get("results", [])
            msg = first_error(resp)
            if any(k.lower() in msg.lower() for k in ok_if):
                return s, []
            errs.append(f"{s[:160]}\n      → {msg}")
        raise CbasError("all variants failed:\n    " + "\n    ".join(errs))

    def count(self, fq: str, timeout: float = 120.0) -> int:
        r = self.q(f"SELECT VALUE COUNT(*) FROM {fq};", timeout=timeout)
        return int(r[0]) if r else 0

    def ping(self) -> dict:
        t0 = time.time()
        resp = self.query("SELECT VALUE 1;", timeout=10)
        return {"ok": resp.get("status") == "success",
                "ms": round((time.time() - t0) * 1000, 1),
                "error": None if resp.get("status") == "success" else first_error(resp)}


def first_error(resp: dict) -> str:
    errs = resp.get("errors") or []
    if errs:
        e = errs[0]
        return f"[{e.get('code', '?')}] {e.get('msg', e)}"
    return f"status={resp.get('status')} http={resp.get('_http_status')}"


def error_code(resp: dict) -> str:
    errs = resp.get("errors") or []
    if errs:
        return str(errs[0].get("code", "?"))
    return str(resp.get("status", "unknown"))


# --------------------------------------------------------------------------- #
# Identifier helpers
# --------------------------------------------------------------------------- #

def bq(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq_name(scope: str, coll: str) -> str:
    """'Default.Default' and 'orders' becomes `Default`.`Default`.`orders`"""
    parts = [p for p in scope.split(".") if p] + [coll]
    return ".".join(bq(p) for p in parts)


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #

def find_ea_state(explicit: str | None) -> Path | None:
    if explicit:
        p = Path(explicit).expanduser()
        if p.is_dir():
            p = p / ".ea_repro_state.json"
        return p if p.exists() else None
    cands = [EA_REPRO_DIR / ".ea_repro_state.json",
             EA_REPRO_DIR / "dist" / ".ea_repro_state.json"]
    cands = [c for c in cands if c.exists()]
    if not cands:
        return None
    return max(cands, key=lambda c: c.stat().st_mtime)


def add_target_args(p) -> None:
    g = p.add_argument_group("target cluster (default: the running EA_repro env)")
    g.add_argument("--ea-state", default=None,
                   help="path to an EA_repro .ea_repro_state.json (or its directory)")
    g.add_argument("--host", default=None,
                   help="any cluster: hostname/IP of a node (disables EA_repro lookup)")
    g.add_argument("-u", "--username", default=None)
    g.add_argument("-p", "--password", default=None)
    g.add_argument("--query-port", type=int, default=None,
                   help="analytics port (default 8095, or 18095 with --tls)")
    g.add_argument("--mgmt-port", type=int, default=None,
                   help="cluster manager port (default 8091, or 18091 with --tls)")
    g.add_argument("--tls", action="store_true", help="use https (certs not verified)")
    g.add_argument("--kind", choices=["ea", "server"], default=None,
                   help="'ea' = Enterprise Analytics/Columnar, 'server' = Server "
                        "with the Analytics service (auto-detected if omitted)")
    g.add_argument("--no-discover", action="store_true",
                   help="only use --host for queries; don't discover other analytics nodes")


def resolve_target(args) -> Target:
    if getattr(args, "host", None):
        return _target_from_host(args)
    path = find_ea_state(getattr(args, "ea_state", None))
    if path is None:
        die("No target. Either start an EA_repro env (`../EA_repro/ea_repro.py up`) "
            "or pass --host/-u/-p for any cluster.")
    st = json.loads(path.read_text())
    n = int(st.get("nodes", 1))
    q = [f"http://localhost:{st['ea_query_host_base'] + i * 10}" for i in range(n)]
    t = Target(query_urls=q,
               mgmt_url=f"http://localhost:{st['ea_ui_host_base']}",
               username=args.username or st["ea_username"],
               password=args.password or st["ea_password"],
               kind=args.kind or "ea", ea=st, ea_state_path=str(path))
    return t


def _target_from_host(args) -> Target:
    scheme = "https" if args.tls else "http"
    qport = args.query_port or (18095 if args.tls else 8095)
    mport = args.mgmt_port or (18091 if args.tls else 8091)
    if not args.username or args.password is None:
        die("--host needs -u/--username and -p/--password")
    host = args.host
    mgmt = f"{scheme}://{host}:{mport}"
    t = Target(query_urls=[f"{scheme}://{host}:{qport}"], mgmt_url=mgmt,
               username=args.username, password=args.password,
               kind=args.kind or "server")
    # discover the analytics nodes and the kind
    try:
        pools = t.http.get_json(f"{mgmt}/pools/default", timeout=8)
        nodes = pools.get("nodes", [])
        if not args.no_discover:
            urls = []
            for nd in nodes:
                if "cbas" in nd.get("services", []):
                    h = nd.get("hostname", "").rsplit(":", 1)[0].strip("[]")
                    if h in ("127.0.0.1", "localhost"):
                        h = host
                    urls.append(f"{scheme}://{h}:{qport}")
            # Nodes advertise their internal names (Docker IPs, k8s pod names etc.), so
            # only keep the ones we can reach from here, otherwise fall back to --host.
            reach = []
            for u in sorted(set(urls)):
                pu = urllib.parse.urlsplit(u)
                try:
                    import socket
                    with socket.create_connection((pu.hostname, pu.port), timeout=2):
                        reach.append(u)
                except OSError:
                    warn(f"analytics node {pu.hostname}:{pu.port} (from /pools/default) "
                         f"isn't reachable from here — skipping it")
            if reach:
                t.query_urls = reach
        if args.kind is None:
            # EA / Columnar nodes only run kv (metadata only) and cbas, and never
            # expose n1ql/index. Server with Analytics nearly always has n1ql.
            svcs = set(s for nd in nodes for s in nd.get("services", []))
            ver = str(nodes[0].get("version", "")) if nodes else ""
            if "n1ql" not in svcs and "cbas" in svcs:
                t.kind = "ea"
            info(f"detected kind={t.kind} (services={','.join(sorted(svcs))}, version={ver})")
    except Exception as e:  # noqa: BLE001
        warn(f"could not read {mgmt}/pools/default ({e}); using --host only")
    return t
