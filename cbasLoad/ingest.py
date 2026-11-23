"""
ingest.py - the auto-created Couchbase source, the remote link into
Analytics, and the KV ingestion workload with end-to-end lag measurement.

Source modes
  container  (default for EA_repro) a Couchbase Server Data Service container
             `<project>-cbsource` on the EA_repro compose network. Alternate
             addresses make it reachable from the host SDK.
  existing   any Couchbase cluster you point at (--source-host ...)
  local      for Server-with-Analytics targets, the cluster's own bucket via
             the Local link (no remote link needed)

Writer engines
  sdk          Python SDK (couchbase>=4): upsert/insert/delete/expire mix, the
               same document templates as `gen`, rate limit and load profile.
  pillowfight  cbc-pillowfight inside the source container (highest
               throughput, but constant rate and upsert/get only).

Lag
  A heartbeat doc (`cbasload::hb`) is written every --hb-interval, then a probe
  reads it back through Analytics to give the end-to-end ingestion lag in ms.
  Every --count-every it also compares the KV item count with the Analytics
  COUNT(*), which gives the gap.
"""

from __future__ import annotations

import json
import random
import re
import socket
import subprocess
import tempfile
import threading
import time
from datetime import timedelta
from pathlib import Path

import docgen
import state
from recorder import Profile, Recorder, parse_duration
from target import (CbasError, Http, Target, bq, die, first_error, fq_name, info,
                    ok, warn)

HB_KEY = "cbasload::hb"
SOURCE_SVC = "cbsource"
CB_BIN = "/opt/couchbase/bin"


def add_source_args(p) -> None:
    g = p.add_argument_group("ingestion source")
    g.add_argument("--source", choices=["container", "existing", "local"], default=None,
                   help="container = auto-created Couchbase Server container (default for "
                        "EA_repro), existing = --source-host cluster, local = the target's "
                        "own Data Service (Server with Analytics)")
    g.add_argument("--source-image", default="couchbase/server:latest",
                   help="image for the auto-created source container")
    g.add_argument("--source-ram", type=int, default=1024,
                   help="source cluster data quota MiB (container mode)")
    g.add_argument("--source-bucket", default="source")
    g.add_argument("--source-bucket-ram", type=int, default=512)
    g.add_argument("--source-scope", default="cbasload")
    g.add_argument("--source-collection", default=None,
                   help="KV collection to write (default: same name as the analytics collection)")
    g.add_argument("--source-compression", choices=["off", "passive", "active"],
                   default="passive", help="bucket compressionMode (active gives the SUPP-30 shape)")
    g.add_argument("--source-host", default=None, help="existing mode: a node of that cluster")
    g.add_argument("--source-user", default="Administrator")
    g.add_argument("--source-password", default="password123")
    g.add_argument("--source-link-host", default=None,
                   help="hostname[:port] Analytics should use to reach the source "
                        "(default: --source-host, or the container's network alias)")
    g.add_argument("--link-name", default="cbsourcelink")


# --------------------------------------------------------------------------- #
# Source description
# --------------------------------------------------------------------------- #

def _free_port(pref: int) -> int:
    p = pref
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                p += 1


def source_mode(t: Target, args) -> str:
    if args.source:
        return args.source
    if args.source_host:
        return "existing"
    return "container" if t.from_ea_repro else "local"


class Source:
    """Where KV writes go and how Analytics sees them."""

    def __init__(self, mode: str, *, mgmt_url: str, sdk_connstr: str, link_host: str | None,
                 user: str, password: str, bucket: str, scope: str, collection: str,
                 container: str | None = None, extra: dict | None = None):
        self.mode, self.mgmt_url, self.sdk_connstr = mode, mgmt_url, sdk_connstr
        self.link_host, self.user, self.password = link_host, user, password
        self.bucket, self.scope, self.collection = bucket, scope, collection
        self.container = container
        self.extra = extra or {}
        self.http = Http(user, password)

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("mode", "mgmt_url", "sdk_connstr", "link_host",
                                               "user", "password", "bucket", "scope",
                                               "collection", "container", "extra")}

    @classmethod
    def from_dict(cls, d: dict) -> "Source":
        return cls(**d)

    @property
    def keyspace(self) -> str:
        return f"{bq(self.bucket)}.{bq(self.scope)}.{bq(self.collection)}"

    # ---- REST on the source cluster --------------------------------------
    def rest(self, method: str, path: str, form: dict | None = None,
             ok_codes=(200, 202), timeout: float = 30) -> tuple[int, str]:
        st, body = self.http.request(method, f"{self.mgmt_url}{path}", form=form,
                                     timeout=timeout)
        txt = body.decode(errors="replace")
        if st not in ok_codes:
            raise CbasError(f"{method} {path} → HTTP {st}: {txt[:300]}")
        return st, txt

    def item_count(self) -> int | None:
        """Item count of our KV collection (stats API, 7.0+). Falls back to the
        bucket's basicStats.itemCount, which is coarser and lags by a few seconds."""
        try:
            _, txt = self.rest(
                "GET", f"/pools/default/stats/range/kv_collection_item_count?bucket="
                       f"{self.bucket}&scope={self.scope}&collection={self.collection}"
                       f"&nodesAggregation=sum&start=-3", timeout=5)
            vals = json.loads(txt)["data"][0]["values"]
            return int(vals[-1][1])
        except Exception:  # noqa: BLE001
            pass
        try:
            _, txt = self.rest("GET", f"/pools/default/buckets/{self.bucket}", timeout=5)
            return int(json.loads(txt)["basicStats"]["itemCount"])
        except Exception:  # noqa: BLE001
            return None

    def ensure_keyspace(self, *, ram: int, compression: str) -> None:
        st, _ = self.http.request("GET", f"{self.mgmt_url}/pools/default/buckets/{self.bucket}",
                                  timeout=10)
        if st == 404:
            self.rest("POST", "/pools/default/buckets", form={
                "name": self.bucket, "bucketType": "couchbase", "ramQuota": ram,
                "compressionMode": compression, "flushEnabled": 1, "replicaNumber": 0,
            })
            ok(f"created bucket '{self.bucket}' ({ram} MiB, compression={compression})")
            self._wait_bucket()
        if self.scope != "_default":
            self.rest("POST", f"/pools/default/buckets/{self.bucket}/scopes",
                      form={"name": self.scope}, ok_codes=(200, 400))
        if self.collection != "_default":
            self.rest("POST", f"/pools/default/buckets/{self.bucket}/scopes/{self.scope}"
                              f"/collections", form={"name": self.collection},
                      ok_codes=(200, 400))
        time.sleep(1.0)

    def _wait_bucket(self, timeout: float = 120) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                _, txt = self.rest("GET", f"/pools/default/buckets/{self.bucket}", timeout=5)
                d = json.loads(txt)
                if d.get("nodes") and all(n.get("status") == "healthy" for n in d["nodes"]):
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1.5)
        raise CbasError(f"bucket {self.bucket} not ready after {timeout}s")

    def delete_bucket(self) -> None:
        self.rest("DELETE", f"/pools/default/buckets/{self.bucket}", ok_codes=(200, 404))


# --------------------------------------------------------------------------- #
# Container source lifecycle
# --------------------------------------------------------------------------- #

def _cli(container: str, argv: list[str], user: str, pw: str, *, init: bool = False) -> str:
    cmd = ["exec", container, f"{CB_BIN}/couchbase-cli", *argv, "-c", "localhost:8091"]
    if not init:
        cmd += ["-u", user, "-p", pw]
    r = state.docker(cmd, check=False)
    return (r.stdout or "") + (r.stderr or "")


def source_up(t: Target, args, coll_name: str) -> Source:
    """Creates (or reuses) the source and its bucket/scope/collection."""
    mode = source_mode(t, args)
    kv_coll = args.source_collection or coll_name
    saved = state.get_source(t)

    if mode == "container":
        if not t.from_ea_repro:
            die("--source container needs an EA_repro target (it joins its docker network). "
                "Use --source existing --source-host … for other clusters.")
        cname = t.container(SOURCE_SVC)
        if saved and saved.get("container") == cname and _exists(cname):
            src = Source.from_dict(saved)
            src.collection = kv_coll
            if not state.container_running(cname):
                info(f"starting existing source container {cname}")
                state.docker(["start", cname])
                state.docker(["unpause", cname], check=False)
                _wait_http(src.mgmt_url + "/pools", 180)
        else:
            src = _create_container_source(t, args, cname, kv_coll)
    elif mode == "existing":
        if not args.source_host:
            die("--source existing needs --source-host")
        h = args.source_host
        src = Source("existing", mgmt_url=f"http://{h}:8091", sdk_connstr=f"couchbase://{h}",
                     link_host=args.source_link_host or h, user=args.source_user,
                     password=args.source_password, bucket=args.source_bucket,
                     scope=args.source_scope, collection=kv_coll)
    else:  # local
        if t.kind == "ea":
            die("--source local needs a Server-with-Analytics target; Enterprise Analytics "
                "has no Data service. Use --source container (EA_repro) or existing.")
        host = re.sub(r"^https?://", "", t.mgmt_url).rsplit(":", 1)[0]
        src = Source("local", mgmt_url=t.mgmt_url, sdk_connstr=f"couchbase://{host}",
                     link_host=None, user=t.username, password=t.password,
                     bucket=args.source_bucket, scope=args.source_scope, collection=kv_coll)

    src.ensure_keyspace(ram=args.source_bucket_ram, compression=args.source_compression)
    state.put_source(t, src.to_dict())
    ok(f"source ready: {src.mode} {src.mgmt_url} → {src.bucket}.{src.scope}.{src.collection}")
    return src


def _exists(name: str) -> bool:
    return state.docker(["inspect", name], check=False).returncode == 0


def _wait_http(url: str, timeout: float) -> None:
    import urllib.request
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            urllib.request.urlopen(url, timeout=3)
            return
        except Exception as e:  # noqa: BLE001
            if "401" in str(e):
                return
        time.sleep(2)
    raise CbasError(f"{url} not reachable after {timeout}s")


def _create_container_source(t: Target, args, cname: str, kv_coll: str) -> Source:
    user, pw = args.source_user, args.source_password
    mport, kport = _free_port(8191), _free_port(11310)
    alias = f"{SOURCE_SVC}.{t.project}.local"
    info(f"creating source container {cname} ({args.source_image}) on {t.docker_network}, "
         f"host ports mgmt={mport} kv={kport}")
    if _exists(cname):
        state.docker(["rm", "-f", cname])
    state.docker(["run", "-d", "--name", cname, "--hostname", alias,
                  "--network", t.docker_network, "--network-alias", alias,
                  "--network-alias", SOURCE_SVC,
                  "-p", f"{mport}:8091", "-p", f"{kport}:11210",
                  "--ulimit", "nofile=40960:40960",
                  "--label", f"cbasload.project={t.project}", args.source_image], quiet=False)
    mgmt = f"http://localhost:{mport}"
    _wait_http(mgmt + "/pools", 180)
    out = _cli(cname, ["node-init", "--node-init-hostname", alias], user, pw, init=True)
    out = _cli(cname, ["cluster-init", "--cluster-username", user, "--cluster-password", pw,
                       "--services", "data", "--cluster-ramsize", str(args.source_ram),
                       "--cluster-name", f"{t.project}-source"], user, pw, init=True)
    if "SUCCESS" not in out:
        raise CbasError(f"source cluster-init failed: {out.strip()[:400]}")
    out = _cli(cname, ["setting-alternate-address", "--set", "--node", alias,
                       "--hostname", "127.0.0.1", "--ports", f"mgmt={mport},kv={kport}"],
               user, pw)
    if "SUCCESS" not in out:
        warn(f"alternate address not set (host SDK may not connect): {out.strip()[:300]}")
    ok("source cluster initialised")
    return Source("container", mgmt_url=mgmt,
                  sdk_connstr=f"couchbase://127.0.0.1:{kport}?network=external",
                  link_host=alias, user=user, password=pw, bucket=args.source_bucket,
                  scope=args.source_scope, collection=kv_coll, container=cname,
                  extra={"mgmt_port": mport, "kv_port": kport, "image": args.source_image})


def source_down(t: Target, *, keep: bool = False) -> None:
    saved = state.get_source(t)
    if not saved:
        warn("no source recorded for this target")
        return
    if saved.get("container"):
        state.docker(["rm", "-f", saved["container"]], check=False)
        ok(f"removed {saved['container']}")
    if not keep:
        state.put_source(t, None)


# --------------------------------------------------------------------------- #
# Link and collection on the Analytics side
# --------------------------------------------------------------------------- #

def ensure_link_and_collection(t: Target, src: Source, link: str, scope: str,
                               coll: str) -> str:
    fq = fq_name(scope, coll)
    db_scope = scope.split(".")
    if src.mode == "local":
        # Not every version accepts IF NOT EXISTS before ON, so rely on the
        # "already exists" error instead.
        t.q_any([
            f"CREATE ANALYTICS COLLECTION {fq} ON {src.keyspace};",
            f"CREATE DATASET {fq} ON {src.keyspace};",
        ], ok_if=("already exists",))
        _connect(t, ["Local", f"{bq(db_scope[-1])}.Local"])
        return fq

    create_remote_link(t, src, link, scope)
    t.q_any([
        f"CREATE COLLECTION {fq} ON {src.keyspace} AT {bq(link)};",                 # EA / Columnar
        f"CREATE ANALYTICS COLLECTION {fq} ON {src.keyspace} AT {fq_name(scope, link)};",
        f"CREATE ANALYTICS COLLECTION {fq} ON {src.keyspace} AT {bq(link)};",       # Server
    ], ok_if=("already exists",))
    ok(f"collection {fq} ← {src.bucket}.{src.scope}.{src.collection} AT {link}")
    _connect(t, [bq(link), fq_name(scope, link)])
    return fq


def _connect(t: Target, names: list[str]) -> None:
    stmts = [f"CONNECT LINK {n};" for n in names]
    s, _ = t.q_any(stmts, ok_if=("already connected",))
    ok(f"link connected ({s.strip(';')})")


def create_remote_link(t: Target, src: Source, link: str, scope: str) -> None:
    host = src.link_host
    host_port = host if ":" in host else f"{host}:8091"
    exists = ("already exists", "exists")
    errs = []
    # Firstly, try SQL++ CREATE LINK (EA / Columnar style).
    for details in (
        {"connectionString": f"couchbase://{host}", "username": src.user,
         "password": src.password, "encryption": "none"},
        {"hostname": host_port, "username": src.user, "password": src.password,
         "encryption": "none"},
    ):
        stmt = f"CREATE LINK {bq(link)} TYPE COUCHBASE WITH {json.dumps(details)};"
        resp = t.query(stmt, timeout=60)
        msg = first_error(resp) if resp.get("status") != "success" else ""
        if resp.get("status") == "success" or any(k in msg.lower() for k in exists):
            ok(f"remote link '{link}' → {host} (SQL++)")
            return
        errs.append(f"SQL {list(details)}: {msg}")
    # Secondly, the REST /analytics/link endpoint (Server 7.x and EA).
    for path in (f"/analytics/link/{'/'.join(scope.split('.'))}/{link}",
                 f"/analytics/link/{scope.split('.')[-1]}/{link}"):
        st, body = t.http.request("POST", f"{t.query_urls[0]}{path}", form={
            "type": "couchbase", "hostname": host_port, "username": src.user,
            "password": src.password, "encryption": "none"}, timeout=60)
        txt = body.decode(errors="replace")
        if st == 200 or any(k in txt.lower() for k in exists):
            ok(f"remote link '{link}' → {host} (REST {path})")
            return
        errs.append(f"REST {path}: HTTP {st} {txt[:200]}")
    # Finally, the EA CLI inside the node container (mirrors EA_repro's S3 link).
    if t.from_ea_repro:
        details = {"type": "couchbase", "hostname": host_port, "username": src.user,
                   "password": src.password, "encryption": "none",
                   "dataverse": scope.split(".")[-1]}
        r = state.docker(["exec", t.container("ea-node1"),
                          "/opt/enterprise-analytics/bin/couchbase-cli",
                          "enterprise-analytics-link-setup", "--create", "--name", link,
                          "--link-details", json.dumps(details),
                          "-c", "localhost:8091", "-u", t.username, "-p", t.password],
                         check=False)
        out = (r.stdout or "") + (r.stderr or "")
        if "SUCCESS" in out or "already exists" in out.lower():
            ok(f"remote link '{link}' → {host} (couchbase-cli)")
            return
        errs.append(f"CLI: {out.strip()[:300]}")
    raise CbasError("could not create remote link:\n    " + "\n    ".join(errs))


def drop_link(t: Target, link: str, scope: str) -> None:
    for s in (f"DISCONNECT LINK {bq(link)};", f"DISCONNECT LINK {fq_name(scope, link)};"):
        if t.query(s, timeout=60).get("status") == "success":
            break


# --------------------------------------------------------------------------- #
# SDK helpers
# --------------------------------------------------------------------------- #

def sdk_collection(src: Source):
    try:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions
    except ImportError:
        die("the sdk engine needs the Couchbase Python SDK:  pip install couchbase  "
            "(or use --engine pillowfight)")
    opts = ClusterOptions(PasswordAuthenticator(src.user, src.password),
                          timeout_options=ClusterTimeoutOptions(
                              kv_timeout=timedelta(seconds=10),
                              connect_timeout=timedelta(seconds=20)))
    cluster = Cluster(src.sdk_connstr, opts)
    cluster.wait_until_ready(timedelta(seconds=60))
    coll = cluster.bucket(src.bucket).scope(src.scope).collection(src.collection)
    return cluster, coll


def seed_source(src: Source, gen: docgen.DocGen, n: int, *, start: int = 0,
                threads: int = 8, batch: int = 500) -> float:
    """Bulk upserts docs start..start+n-1 and returns the seconds taken."""
    cluster, coll = sdk_collection(src)
    t0 = time.time()
    nxt = [start]
    lock = threading.Lock()
    done = [0]
    errors: list[str] = []
    end = start + n

    def worker():
        while True:
            with lock:
                a = nxt[0]
                if a >= end:
                    return
                b = min(end, a + batch)
                nxt[0] = b
            docs = {gen.key(i): gen.doc(i) for i in range(a, b)}
            for attempt in range(5):
                try:
                    res = coll.upsert_multi(docs)
                    if not res.all_ok:
                        bad = {k: docs[k] for k in res.exceptions}
                        if attempt == 4:
                            errors.append(f"{len(bad)} docs failed: "
                                          f"{next(iter(res.exceptions.values()))}")
                        docs = bad
                        time.sleep(0.5 * (attempt + 1))
                        continue
                    break
                except Exception as e:  # noqa: BLE001
                    if attempt == 4:
                        errors.append(str(e)[:200])
                    time.sleep(0.5 * (attempt + 1))
            with lock:
                done[0] += b - a
                if (done[0] // batch) % max(1, (n // batch) // 10 or 1) == 0:
                    info(f"  seeded {done[0]:,}/{n:,} docs "
                         f"({done[0] / max(1e-9, time.time() - t0):,.0f} docs/s)")

    ths = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    cluster.close()
    if errors:
        warn(f"{len(errors)} seed batch errors, e.g. {errors[0]}")
    return time.time() - t0


# --------------------------------------------------------------------------- #
# Ingest workload
# --------------------------------------------------------------------------- #

def parse_mix(s: str) -> dict[str, float]:
    out = {}
    for part in s.split(","):
        k, v = part.split("=")
        k = k.strip()
        if k not in ("upsert", "insert", "delete", "expire", "get"):
            raise ValueError(f"unknown op '{k}' in --ops")
        out[k] = float(v)
    return out


class RateLimiter:
    def __init__(self, rate_fn):
        self.rate_fn = rate_fn
        self.lock = threading.Lock()
        self.next = time.time()

    def acquire(self, stop: threading.Event) -> bool:
        rate = self.rate_fn()
        if rate <= 0:
            return not stop.is_set()
        with self.lock:
            now = time.time()
            if self.next < now - 1.0:
                self.next = now - 1.0  # allow at most 1s of burst catch-up
            slot = self.next
            self.next = slot + 1.0 / rate
        d = slot - time.time()
        if d > 0:
            return not stop.wait(d)
        return not stop.is_set()


class IngestLoad:
    def __init__(self, t: Target, rec: Recorder, src: Source, fq: str, gen: docgen.DocGen,
                 profile: Profile, *, keyspace: int, rate: float, threads: int,
                 ops: dict[str, float], ttl: float, key_skew: float, engine: str,
                 hb_interval: float, probe_interval: float, count_every: float,
                 seed: int = 3):
        self.t, self.rec, self.src, self.fq, self.gen = t, rec, src, fq, gen
        self.profile = profile
        self.keyspace = max(1, keyspace)
        self.rate, self.threads = rate, max(1, threads)
        self.ops = ops
        self.ttl = ttl
        self.key_zipf = docgen.Zipf(self.keyspace, key_skew) if key_skew > 0 else None
        self.engine = engine
        self.hb_interval, self.probe_interval = hb_interval, probe_interval
        self.count_every = count_every
        self.seed = seed
        self.stop_ev = threading.Event()
        self.ths: list[threading.Thread] = []
        self.next_new = self.keyspace
        self.lock = threading.Lock()
        self.version = int(time.time())
        self.hb_seq = 0
        self.proc: subprocess.Popen | None = None
        self.paused = threading.Event()   # set while chaos disables writes

    def describe(self) -> str:
        r = f"{self.rate:g} ops/s" if self.rate > 0 else "unthrottled"
        mix = ",".join(f"{k}={v:g}" for k, v in self.ops.items())
        return (f"{self.engine} engine, {self.threads} threads, {r}, ops {mix}, "
                f"keyspace {self.keyspace:,}, profile {self.profile.describe()}")

    def start(self) -> None:
        self.t_start = time.time()
        if self.engine == "sdk":
            self.cluster, self.coll = sdk_collection(self.src)
            self.limiter = RateLimiter(
                lambda: self.rate * self.profile.level(time.time() - self.t_start))
            for i in range(self.threads):
                self._spawn(self._sdk_worker, i, f"kv{i}")
        else:
            self._start_pillowfight()
            self.cluster, self.coll = sdk_collection(self.src)
        self._spawn(self._heartbeat, 0, "hb")
        self._spawn(self._probe, 0, "probe")

    def _spawn(self, fn, i, name):
        th = threading.Thread(target=fn, args=(i,), daemon=True, name=name)
        th.start()
        self.ths.append(th)

    def stop(self) -> None:
        self.stop_ev.set()
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            if self.src.container:
                state.docker(["exec", self.src.container, "pkill", "-f", "cbc-pillowfight"],
                             check=False)
        for th in self.ths:
            th.join(5)
        try:
            self.cluster.close()
        except Exception:  # noqa: BLE001
            pass

    # ---- SDK writer --------------------------------------------------------
    def _pick_existing(self, rng: random.Random) -> int:
        with self.lock:
            hi = self.next_new
        if self.key_zipf:
            return min(hi - 1, self.key_zipf.sample(rng))
        return rng.randrange(hi)

    def _sdk_worker(self, idx: int) -> None:
        from couchbase.exceptions import DocumentNotFoundException, DocumentExistsException
        from couchbase.options import UpsertOptions
        rng = random.Random(self.seed * 31 + idx)
        names = list(self.ops)
        weights = [self.ops[k] for k in names]
        while self.limiter.acquire(self.stop_ev):
            if self.paused.is_set():
                self.stop_ev.wait(0.2)
                continue
            op = rng.choices(names, weights=weights)[0]
            t0 = time.time()
            try:
                if op == "insert":
                    with self.lock:
                        n = self.next_new
                        self.next_new += 1
                    self.coll.insert(self.gen.key(n), self.gen.doc(n, 1, int(t0)))
                elif op == "delete":
                    n = self._pick_existing(rng)
                    try:
                        self.coll.remove(self.gen.key(n))
                    except DocumentNotFoundException:
                        op = "delete_miss"
                elif op == "get":
                    n = self._pick_existing(rng)
                    try:
                        self.coll.get(self.gen.key(n))
                    except DocumentNotFoundException:
                        op = "get_miss"
                else:
                    n = self._pick_existing(rng)
                    self.version += 1
                    doc = self.gen.doc(n, self.version, int(t0))
                    if op == "expire":
                        self.coll.upsert(self.gen.key(n), doc,
                                         UpsertOptions(expiry=timedelta(seconds=self.ttl)))
                    else:
                        self.coll.upsert(self.gen.key(n), doc)
                self.rec.record_ingest(op, True, (time.time() - t0) * 1000)
            except DocumentExistsException:
                self.rec.record_ingest(op, True, (time.time() - t0) * 1000)
            except Exception as e:  # noqa: BLE001
                self.rec.record_ingest(op, False, 0, code=type(e).__name__)
                self.stop_ev.wait(0.05)

    # ---- pillowfight ------------------------------------------------------
    def _start_pillowfight(self) -> None:
        if not self.src.container:
            die("--engine pillowfight runs inside the source container (container mode only)")
        tmp = Path(tempfile.mkdtemp(prefix="cbasload-docs-"))
        files = []
        for i in range(20):
            f = tmp / f"doc{i}.json"
            f.write_text(json.dumps(self.gen.doc(i)))
            files.append(f.name)
        state.docker(["exec", self.src.container, "rm", "-rf", "/tmp/cbasload_docs"], check=False)
        state.docker(["cp", str(tmp), f"{self.src.container}:/tmp/cbasload_docs"])
        set_pct = int(round(100 * sum(v for k, v in self.ops.items() if k != "get")
                            / max(1e-9, sum(self.ops.values()))))
        per_thread = int(self.rate / self.threads) if self.rate > 0 else 0
        # -t: with a TTY, pillowfight flushes its OPS/SEC progress lines straight away
        argv = ["exec", "-t", self.src.container, f"{CB_BIN}/cbc-pillowfight",
                # Use the internal hostname. 127.0.0.1 would match the alternate address
                # and send pillowfight to the host-mapped KV port, which doesn't exist in
                # here. dnssrv=off because the SRV lookup on the Docker DNS stalls
                # bootstrap for ~20s.
                "-U", f"couchbase://{self.src.link_host}/{self.src.bucket}?dnssrv=off",
                "-u", self.src.user, "-P", self.src.password,
                "--collection", f"{self.src.scope}.{self.src.collection}",
                # Note in lcb 3.x -I is the deprecated --num-iterations rather than --num-items
                "-J", "--num-items", str(self.keyspace), "-t", str(self.threads),
                "--set-pct", str(set_pct), "-p", self.gen.spec.key_prefix, "-n"]
        for f in files:
            argv += ["--docs", f"/tmp/cbasload_docs/{f}"]
        if per_thread:
            argv += ["--rate-limit", str(per_thread)]
        if self.ops.get("expire"):
            argv += ["-e", str(int(self.ttl))]
        info("pillowfight: docker " + " ".join(a if "-P" != b else "***"
                                               for b, a in zip([""] + argv, argv)))
        self._pf_argv = ["docker"] + argv
        self._spawn(self._pf_supervisor, 0, "pf")

    def _pf_supervisor(self, _):
        """Runs pillowfight, and if it dies (e.g. chaos killed the source) relaunches
        it once the source answers again. pillowfight prints `OPS/SEC: <avg>` every
        10,000 ops, so each line is exactly 10k ops. We spread these evenly over the
        time since the previous line so the per-second chart isn't spiky."""
        import os
        import select
        while not self.stop_ev.is_set():
            self.proc = subprocess.Popen(self._pf_argv, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT)
            fd = self.proc.stdout.fileno()
            buf, last_t, tail = b"", self.rec.now(), []
            while not self.stop_ev.is_set():
                r, _, _ = select.select([fd], [], [], 0.5)
                if not r:
                    if self.proc.poll() is not None:
                        break
                    continue
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                buf += chunk
                *lines, buf = re.split(rb"[\r\n]", buf)
                for ln in lines:
                    if b"OPS/SEC" in ln:
                        now = self.rec.now()
                        self.rec.record_ingest_spread("pillowfight", 10_000, last_t, now)
                        last_t = now
                    elif ln.strip():
                        tail = (tail + [ln.decode(errors="replace").strip()[:300]])[-5:]
            if self.stop_ev.is_set():
                return
            self.proc.wait()
            self.rec.extra.setdefault("pillowfight_log", []).extend(tail)
            self.rec.event("ingest", f"pillowfight exited rc={self.proc.returncode}; "
                                     f"relaunching when the source is back")
            while not self.stop_ev.wait(2):
                if state.container_running(self.src.container) and \
                        self.src.item_count() is not None:
                    break
            if not self.stop_ev.is_set():
                self.rec.event("ingest", "pillowfight relaunched")

    # ---- heartbeat and lag probe ------------------------------------------
    def _heartbeat(self, _):
        """Writes hbSeq=1,2,3... and remembers when KV acknowledged each write."""
        self.hb_acked: dict[int, float] = {}
        while not self.stop_ev.wait(self.hb_interval):
            seq = self.hb_seq + 1
            try:
                self.coll.upsert(HB_KEY, {"docType": "cbasload_hb", "hbTs": int(time.time() * 1000),
                                          "hbSeq": seq, "hbRun": self.rec.run_id})
                self.hb_seq = seq
                self.hb_acked[seq] = time.time()
                self.hb_last_ok = time.time()
                for old in [s for s in self.hb_acked if s < seq - 3600]:
                    del self.hb_acked[old]
            except Exception as e:  # noqa: BLE001
                self.rec.record_ingest("heartbeat", False, 0, code=type(e).__name__)

    def _lag_ms(self, visible_seq: int, run: str) -> int | None:
        """Ingestion lag is the age of the oldest heartbeat that KV has acked but
        Analytics can't see yet (0 when Analytics has the latest one). It's accurate
        to roughly the probe round-trip and isn't limited by --hb-interval."""
        acked = getattr(self, "hb_acked", {})
        if not acked:
            return None
        if time.time() - getattr(self, "hb_last_ok", 0) > 3 * self.hb_interval + 2:
            return None  # KV isn't accepting heartbeats, so lag can't be measured
        if run != self.rec.run_id:
            visible_seq = 0  # Analytics still holds a previous run's heartbeat
        if visible_seq >= self.hb_seq:
            return 0
        pending = [ts for s, ts in acked.items() if s > visible_seq]
        if not pending:
            return 0
        return int((time.time() - min(pending)) * 1000)

    def _probe(self, _):
        last_count = 0.0
        while not self.stop_ev.wait(self.probe_interval):
            sample: dict = {}
            try:
                r = self.t.query(f'SELECT c.hbSeq, c.hbRun FROM {self.fq} c '
                                 f'WHERE META(c).id = "{HB_KEY}";', timeout=20,
                                 client_context_id=f"cbasload-{self.rec.run_id}-lagprobe")
                if r.get("status") == "success":
                    row = (r.get("results") or [{}])[0]
                    lag = self._lag_ms(int(row.get("hbSeq") or 0), row.get("hbRun", ""))
                    if lag is not None:
                        sample["lag_ms"] = lag
                    else:
                        sample["hb_stale"] = True
                else:
                    sample["probe_error"] = first_error(r)[:200]
            except Exception as e:  # noqa: BLE001
                sample["probe_error"] = str(e)[:200]
            if self.count_every and time.time() - last_count >= self.count_every:
                last_count = time.time()
                kv = self.src.item_count()
                try:
                    cb = self.t.count(self.fq, timeout=60)
                except Exception:  # noqa: BLE001
                    cb = None
                sample.update({"kv_items": kv, "cbas_items": cb})
                if kv is not None and cb is not None:
                    sample["gap"] = kv - cb
            if sample.get("probe_error") and not getattr(self, "_probe_warned", False):
                self._probe_warned = True
                self.rec.event("ingest", f"lag probe failing: {sample['probe_error']}")
            self.rec.add_ingest_sample(sample)
