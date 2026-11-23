"""
querymix.py - weighted SQL++ query mixes with placeholders, and the query
load generator (closed-loop concurrency or open-loop QPS).

Mix file (JSON):
  {
    "queries": [
      {"name": "point_lookup", "weight": 30,
       "sql": "SELECT VALUE c FROM ${coll} c WHERE c.id = ${key};"},
      ...
    ]
  }

Placeholders (rendered per request, as SQL literals):
  ${coll}               the target collection (fully qualified)
  ${coll:customers}     companion collection  <coll>_customers
  ${key}                an existing document id (uniform over the generated keyspace)
  ${customer}           a customerId drawn with the dataset's Zipf skew
  ${int:a,b}            random integer in [a,b]
  ${float:a,b}          random float in [a,b]
  ${choice:A,B,C}       one of the values (quoted as strings)
  ${country} ${status} ${category} ${tier}    a valid value for that field
  ${daterange:N}        "start" AND "end"  (an N-day window; use after BETWEEN)
  ${date}               a single ISO timestamp inside the data range
"""

from __future__ import annotations

import heapq
import json
import math
import queue
import random
import re
import threading
import time
from pathlib import Path

import docgen
from recorder import Profile, Recorder, parse_duration_range
from target import Target, error_code, first_error, warn

PH = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")

# --------------------------------------------------------------------------- #
# Built-in mixes
# --------------------------------------------------------------------------- #

BASE_MIX = [
    {"name": "point_lookup", "weight": 30,
     "sql": "SELECT VALUE c FROM ${coll} c WHERE c.id = ${key};"},
    {"name": "range_scan", "weight": 15,
     "sql": "SELECT c.id, c.amount, c.created_at FROM ${coll} c "
            "WHERE c.created_at BETWEEN ${daterange:7} LIMIT 500;"},
    {"name": "filter_groupby", "weight": 15,
     "sql": "SELECT c.country, COUNT(*) AS n, AVG(c.amount) AS avg_amount FROM ${coll} c "
            "WHERE c.status = ${status} GROUP BY c.country;"},
    {"name": "customer_history", "weight": 15,
     "sql": "SELECT c.id, c.amount, c.created_at FROM ${coll} c "
            "WHERE c.customerId = ${customer} ORDER BY c.created_at DESC LIMIT 20;"},
    {"name": "topn_customers", "weight": 8,
     "sql": "SELECT c.customerId, SUM(c.amount) AS total FROM ${coll} c "
            "GROUP BY c.customerId ORDER BY total DESC LIMIT 10;"},
    {"name": "window_rank", "weight": 7,
     "sql": "SELECT c.id, c.country, c.amount, RANK() OVER (PARTITION BY c.country "
            "ORDER BY c.amount DESC) AS rnk FROM ${coll} c "
            "WHERE c.category = ${category} LIMIT 100;"},
    {"name": "heavy_scan", "weight": 3,
     "sql": "SELECT c.category, c.country, c.status, COUNT(*) AS n, MIN(c.amount) AS mn, "
            "MAX(c.amount) AS mx, COUNT(DISTINCT c.customerId) AS custs FROM ${coll} c "
            "GROUP BY c.category, c.country, c.status ORDER BY n DESC;"},
]

NESTED_EXTRA = [
    {"name": "nested_filter", "weight": 10,
     "sql": "SELECT c.address.city, COUNT(*) AS n, AVG(c.metrics.ltv) AS ltv FROM ${coll} c "
            "WHERE c.profile.tier = ${tier} AND c.address.country = ${country} "
            "GROUP BY c.address.city;"},
]

ARRAYS_EXTRA = [
    {"name": "unnest_items", "weight": 10,
     "sql": "SELECT i.cat, SUM(i.qty * i.price) AS revenue, COUNT(*) AS lines FROM ${coll} c "
            "UNNEST c.items i WHERE c.created_at BETWEEN ${daterange:30} GROUP BY i.cat;"},
    {"name": "any_satisfies", "weight": 5,
     "sql": "SELECT VALUE COUNT(*) FROM ${coll} c WHERE "
            "ANY i IN c.items SATISFIES i.cat = ${category} AND i.qty >= ${int:3,5} END;"},
]

ORDERS_EXTRA = [
    {"name": "join_tier_revenue", "weight": 8,
     "sql": "SELECT cu.tier, COUNT(*) AS orders, SUM(o.total) AS revenue FROM ${coll} o "
            "JOIN ${coll:customers} cu ON o.customerId = cu.custNo "
            "WHERE o.created_at BETWEEN ${daterange:30} GROUP BY cu.tier;"},
    {"name": "join_customer", "weight": 7,
     "sql": "SELECT cu.name, cu.tier, o.id, o.total FROM ${coll} o "
            "JOIN ${coll:customers} cu ON o.customerId = cu.custNo "
            "WHERE cu.custNo = ${customer} LIMIT 50;"},
]


def builtin_mix(template: str) -> list[dict]:
    mix = [dict(q) for q in BASE_MIX]
    if template == "nested":
        mix += NESTED_EXTRA
    if template in ("arrays", "orders"):
        mix += ARRAYS_EXTRA
    if template == "orders":
        mix += ORDERS_EXTRA
    return mix


def load_mix(path: str | None, template: str, only: list[str] | None = None,
             weights: list[str] | None = None) -> list[dict]:
    if path:
        data = json.loads(Path(path).read_text())
        mix = data["queries"] if isinstance(data, dict) else data
    else:
        mix = builtin_mix(template)
    for w in weights or []:  # --weight name=5
        name, val = w.split("=", 1)
        for q in mix:
            if q["name"] == name:
                q["weight"] = float(val)
    if only:
        mix = [q for q in mix if q["name"] in only]
    mix = [q for q in mix if float(q.get("weight", 1)) > 0]
    if not mix:
        raise SystemExit("query mix is empty")
    return mix


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #

def _lit(v) -> str:
    return json.dumps(v)


class Renderer:
    def __init__(self, coll_fq: str, companion_fq: dict[str, str], gen: docgen.DocGen,
                 keyspace: int):
        self.coll = coll_fq
        self.companion = companion_fq
        self.gen = gen
        self.keyspace = max(1, keyspace)

    def render(self, sql: str, r: random.Random) -> str:
        def sub(m: re.Match) -> str:
            name, arg = m.group(1), m.group(2)
            if name == "coll":
                if arg:
                    if arg not in self.companion:
                        raise KeyError(f"no companion collection '{arg}' "
                                       f"(have: {list(self.companion) or 'none'})")
                    return self.companion[arg]
                return self.coll
            if name == "key":
                return _lit(self.gen.key(r.randrange(self.keyspace)))
            if name == "customer":
                return str(self.gen.sample_customer(r))
            if name == "int":
                a, b = (int(float(x)) for x in arg.split(","))
                return str(r.randint(a, b))
            if name == "float":
                a, b = (float(x) for x in arg.split(","))
                return repr(round(r.uniform(a, b), 4))
            if name == "choice":
                return _lit(r.choice([x.strip() for x in arg.split(",")]))
            if name == "country":
                return _lit(r.choice(docgen.COUNTRIES))
            if name == "status":
                return _lit(r.choice(docgen.STATUSES))
            if name == "category":
                return _lit(r.choice(docgen.CATEGORIES))
            if name == "tier":
                return _lit(r.choice(docgen.TIERS))
            if name == "daterange":
                a, b = self.gen.date_range(r, int(arg or 7))
                return f"{_lit(a)} AND {_lit(b)}"
            if name == "date":
                a, _ = self.gen.date_range(r, 0)
                return _lit(a)
            raise KeyError(f"unknown placeholder ${{{name}}}")
        return PH.sub(sub, sql)


# --------------------------------------------------------------------------- #
# Query load generator
# --------------------------------------------------------------------------- #

class QueryLoad:
    """Closed loop (fixed concurrency) or open loop (target QPS)."""

    def __init__(self, target: Target, rec: Recorder, mix: list[dict], renderer: Renderer,
                 profile: Profile, *, concurrency: int = 8, qps: float = 0.0,
                 max_inflight: int = 256, arrival: str = "poisson",
                 query_timeout: float = 120.0, cancel_pct: float = 0.0,
                 cancel_after: str = "0.5s", seed: int = 1, params: dict | None = None):
        self.t, self.rec, self.mix, self.r = target, rec, mix, renderer
        self.profile = profile
        self.concurrency = max(1, concurrency)
        self.qps = qps
        self.max_inflight = max(1, max_inflight)
        self.arrival = arrival
        self.qtimeout = query_timeout
        self.cancel_pct = cancel_pct / 100.0 if cancel_pct > 1 else cancel_pct
        self.cancel_lo, self.cancel_hi = parse_duration_range(cancel_after)
        self.seed = seed
        self.params = params or {}
        self.weights = [float(q.get("weight", 1)) for q in mix]
        self.stop_ev = threading.Event()
        self.threads: list[threading.Thread] = []
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._inflight_lock = threading.Lock()
        # cancellation scheduler
        self._cheap: list = []
        self._ccv = threading.Condition()

    # ---- public -----------------------------------------------------------
    def start(self) -> None:
        self.t_start = time.time()
        if self.cancel_pct > 0:
            th = threading.Thread(target=self._cancel_loop, daemon=True, name="canceller")
            th.start()
            self.threads.append(th)
        if self.qps > 0:
            q: queue.Queue = queue.Queue(maxsize=self.max_inflight * 4)
            self._q = q
            for i in range(self.max_inflight):
                th = threading.Thread(target=self._open_worker, args=(i,), daemon=True,
                                      name=f"qw{i}")
                th.start()
                self.threads.append(th)
            th = threading.Thread(target=self._dispatcher, daemon=True, name="dispatch")
            th.start()
            self.threads.append(th)
        else:
            for i in range(self.concurrency):
                th = threading.Thread(target=self._closed_worker, args=(i,), daemon=True,
                                      name=f"qw{i}")
                th.start()
                self.threads.append(th)

    def stop(self, wait: float = 5.0) -> None:
        self.stop_ev.set()
        with self._ccv:
            self._ccv.notify_all()
        deadline = time.time() + wait
        for th in self.threads:
            th.join(max(0.0, deadline - time.time()))

    def describe(self) -> str:
        mode = (f"open-loop {self.qps:g} q/s (max inflight {self.max_inflight}, {self.arrival})"
                if self.qps > 0 else f"closed-loop {self.concurrency} workers")
        c = f", cancel {self.cancel_pct:.0%} after {self.cancel_lo:g}-{self.cancel_hi:g}s" \
            if self.cancel_pct > 0 else ""
        return f"{mode}, profile {self.profile.describe()}, {len(self.mix)} queries{c}"

    # ---- loops ------------------------------------------------------------
    def _elapsed(self) -> float:
        return time.time() - self.t_start

    def _closed_worker(self, idx: int) -> None:
        rng = random.Random(self.seed * 7919 + idx)
        while not self.stop_ev.is_set():
            lvl = self.profile.level(self._elapsed())
            if idx == 0:
                self.rec.note_level(lvl)
            if idx >= math.ceil(lvl * self.concurrency):
                self.stop_ev.wait(0.05)
                continue
            self._one(rng, time.time())

    def _dispatcher(self) -> None:
        rng = random.Random(self.seed * 104729)
        nxt = time.time()
        while not self.stop_ev.is_set():
            lvl = self.profile.level(self._elapsed())
            self.rec.note_level(lvl)
            rate = max(1e-6, self.qps * lvl)
            gap = rng.expovariate(rate) if self.arrival == "poisson" else 1.0 / rate
            nxt += gap
            delay = nxt - time.time()
            if delay > 0:
                if self.stop_ev.wait(delay):
                    break
            elif delay < -5:  # too far behind, so don't build up an unbounded burst
                nxt = time.time()
            try:
                self._q.put_nowait(nxt)
            except queue.Full:
                with self._inflight_lock:
                    self.rec.backlog += 1
                self.rec.record_query("_dropped_backlog", self._elapsed(), 0.0,
                                      service_ms=0, ok=False, code="client_backlog",
                                      msg="open-loop queue full: server can't keep up "
                                          "with --qps (raise --max-inflight or lower --qps)")

    def _open_worker(self, idx: int) -> None:
        rng = random.Random(self.seed * 15485863 + idx)
        while not self.stop_ev.is_set():
            try:
                intended = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            self._one(rng, intended)

    # ---- one request ------------------------------------------------------
    def _one(self, rng: random.Random, intended: float) -> None:
        q = rng.choices(self.mix, weights=self.weights, k=1)[0]
        name = q["name"]
        try:
            stmt = self.r.render(q["sql"], rng)
        except Exception as e:  # noqa: BLE001
            self.rec.record_query(name, self._elapsed(), 0, service_ms=0, ok=False,
                                  code="render", msg=str(e))
            self.stop_ev.wait(0.5)
            return
        with self._seq_lock:
            self._seq += 1
            seq = self._seq
        ctx = f"cbasload-{self.rec.run_id}-{name}-{seq}"
        url = self.t.next_query_url()
        timeout = parse_timeout(q.get("timeout"), self.qtimeout)
        if self.cancel_pct > 0 and rng.random() < self.cancel_pct:
            self._schedule_cancel(time.time() + rng.uniform(self.cancel_lo, self.cancel_hi),
                                  ctx, url, name)
        with self._inflight_lock:
            self.rec.inflight += 1
        t_send = time.time()
        t_rel = intended - self.rec.t0
        try:
            resp = self.t.query(stmt, timeout=timeout, client_context_id=ctx, url=url,
                                params=self.params)
            done = time.time()
            okk = resp.get("status") == "success"
            server_ms = _elapsed_ms(resp.get("metrics", {}).get("elapsedTime"))
            self.rec.record_query(
                name, t_rel, (done - intended) * 1000, service_ms=(done - t_send) * 1000,
                ok=okk, code="" if okk else _code(resp),
                msg="" if okk else first_error(resp) + f" | {stmt[:200]}",
                server_ms=server_ms, rows=int(resp.get("metrics", {}).get("resultCount", 0)))
        except Exception as e:  # noqa: BLE001
            done = time.time()
            code = "client_timeout" if "timed out" in str(e).lower() else \
                f"client_{type(e).__name__}"
            self.rec.record_query(name, t_rel, (done - intended) * 1000,
                                  service_ms=(done - t_send) * 1000, ok=False, code=code,
                                  msg=str(e)[:300])
            # Connection-level failure (node down or restarting). Back off briefly as a
            # real client would, rather than spinning out thousands of errors per second.
            self.stop_ev.wait(0.25)
        finally:
            with self._inflight_lock:
                self.rec.inflight -= 1

    # ---- cancellation -----------------------------------------------------
    def _schedule_cancel(self, at: float, ctx: str, url: str, name: str) -> None:
        with self._ccv:
            heapq.heappush(self._cheap, (at, ctx, url, name))
            self._ccv.notify()

    def _cancel_loop(self) -> None:
        while not self.stop_ev.is_set():
            with self._ccv:
                while not self._cheap and not self.stop_ev.is_set():
                    self._ccv.wait(0.5)
                if self.stop_ev.is_set():
                    return
                at, ctx, url, name = self._cheap[0]
                delay = at - time.time()
                if delay > 0:
                    self._ccv.wait(min(delay, 0.5))
                    continue
                heapq.heappop(self._cheap)
            threading.Thread(target=self._cancel_one, args=(ctx, url, name),
                             daemon=True).start()

    def _cancel_one(self, ctx: str, url: str, name: str) -> None:
        try:
            st, _ = self.t.http.request(
                "DELETE", f"{url}/analytics/admin/active_requests",
                form={"client_context_id": ctx}, timeout=10)
            # 200 means cancelled, 404 means already finished (or unknown)
            self.rec.record_cancel(name, st == 200)
        except Exception:  # noqa: BLE001
            self.rec.record_cancel(name, False)


def _code(resp: dict) -> str:
    """23010 'Job ... has been cancelled' is the expected outcome of --cancel-pct."""
    c = error_code(resp)
    return "cancelled" if c == "23010" else c


def parse_timeout(v, default: float) -> float:
    if v is None:
        return default
    from recorder import parse_duration
    return parse_duration(v)


def _elapsed_ms(s) -> float:
    """Analytics metrics durations look like '12.345678ms' / '1.2s' / '850.1µs'."""
    if not s:
        return 0.0
    m = re.fullmatch(r"([\d.]+)\s*(ns|µs|us|ms|s|m|h)", str(s))
    if not m:
        return 0.0
    v = float(m.group(1))
    return v * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1, "s": 1e3,
                "m": 6e4, "h": 3.6e6}[m.group(2)]


def post_check(target: Target, mix: list[dict], renderer: Renderer, rec: Recorder,
               repeats: int = 3) -> list[dict]:
    """Runs every query in the mix sequentially, uncancelled, after the load.
    This catches issues of the 'collection broken after cancellations' kind
    (SUPP-121/129)."""
    rng = random.Random(99)
    out = []
    for q in mix:
        for i in range(repeats):
            stmt = renderer.render(q["sql"], rng)
            t0 = time.time()
            resp = target.query(stmt, timeout=300,
                                client_context_id=f"cbasload-{rec.run_id}-postcheck-{q['name']}-{i}")
            okk = resp.get("status") == "success"
            out.append({"name": q["name"], "ok": okk, "ms": round((time.time() - t0) * 1000, 1),
                        "error": None if okk else first_error(resp)})
            if not okk:
                warn(f"post-check {q['name']} failed: {first_error(resp)}")
    return out
