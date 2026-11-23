"""
recorder.py - thread-safe metrics for a run, plus load profiles and the
server-side stats sampler.

Everything a run produces ends up in one dict (Recorder.to_json) which
report.py renders to HTML.
"""

from __future__ import annotations

import math
import random
import re
import threading
import time
from collections import Counter, defaultdict

from target import Target, info


# --------------------------------------------------------------------------- #
# Duration / profile helpers
# --------------------------------------------------------------------------- #

def parse_duration(s: str | float | int | None) -> float:
    """Converts '90' / '90s' / '5m' / '1h' / '250ms' to seconds."""
    if s is None or s == "":
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    m = re.fullmatch(r"\s*([\d.]+)\s*(ms|s|m|h)?\s*", str(s))
    if not m:
        raise ValueError(f"bad duration: {s!r}")
    v = float(m.group(1))
    return v * {"ms": 0.001, "s": 1, "m": 60, "h": 3600, None: 1}[m.group(2)]


def parse_duration_range(s: str) -> tuple[float, float]:
    if "-" in s:
        a, b = s.split("-", 1)
        if not re.search(r"[a-z]", a) and re.search(r"[a-z]+$", b):
            a += re.search(r"[a-z]+$", b).group(0)
        return parse_duration(a), parse_duration(b)
    v = parse_duration(s)
    return v, v


class Profile:
    """Load level multiplier 0..1 as a function of elapsed seconds.

    steady            1.0 throughout
    ramp              linear from 0 to 1 over --ramp (default: whole run), then holds
    step              --steps equal increments across the run
    spike             --base level, which jumps to 1.0 for --spike-len every --spike-every
    """

    def __init__(self, kind: str, duration: float, *, ramp: float = 0, steps: int = 4,
                 base: float = 0.3, spike_every: float = 60, spike_len: float = 10):
        self.kind, self.duration = kind, max(1e-9, duration)
        self.ramp = ramp or duration
        self.steps = max(1, steps)
        self.base, self.spike_every, self.spike_len = base, spike_every, spike_len

    def level(self, t: float) -> float:
        k = self.kind
        if k == "ramp":
            return max(0.02, min(1.0, t / max(1e-9, self.ramp)))
        if k == "step":
            return min(1.0, (math.floor(t / (self.duration / self.steps)) + 1) / self.steps)
        if k == "spike":
            return 1.0 if (t % self.spike_every) >= (self.spike_every - self.spike_len) \
                else self.base
        return 1.0

    def describe(self) -> str:
        if self.kind == "ramp":
            return f"ramp over {self.ramp:.0f}s"
        if self.kind == "step":
            return f"{self.steps} steps"
        if self.kind == "spike":
            return f"base {self.base:.0%}, spike {self.spike_len:.0f}s every {self.spike_every:.0f}s"
        return "steady"


def add_run_args(p) -> None:
    g = p.add_argument_group("run control")
    g.add_argument("--duration", default="60s", help="run length, e.g. 90s, 10m (default 60s)")
    g.add_argument("--profile", choices=["steady", "ramp", "step", "spike"], default="steady")
    g.add_argument("--ramp", default="", help="ramp time for --profile ramp (default: whole run)")
    g.add_argument("--steps", type=int, default=4, help="steps for --profile step")
    g.add_argument("--spike-base", type=float, default=0.3,
                   help="baseline level 0..1 for --profile spike")
    g.add_argument("--spike-every", default="60s")
    g.add_argument("--spike-len", default="10s")
    g.add_argument("--interval", default="5s", help="live console stats interval")
    g.add_argument("--sample-interval", default="2s",
                   help="server stats sampling interval (0 disables)")
    g.add_argument("--out", default=None,
                   help="output dir for run.json and report.html (default runs/<run-id>)")
    g.add_argument("--chaos", action="append", default=[], metavar="AT:ACTION[:ARG]",
                   help="disruption at a time offset, e.g. 30s:restart-node:ea-node1, "
                        "60s:disconnect-link, 20s:pause-source (repeatable; "
                        "`cbasload.py chaos-actions` lists them)")
    g.add_argument("--label", default="", help="free-text label stored in the report")


def profile_from_args(args) -> Profile:
    d = parse_duration(args.duration)
    return Profile(args.profile, d, ramp=parse_duration(args.ramp) if args.ramp else 0,
                   steps=args.steps, base=args.spike_base,
                   spike_every=parse_duration(args.spike_every),
                   spike_len=parse_duration(args.spike_len))


# --------------------------------------------------------------------------- #
# Recorder
# --------------------------------------------------------------------------- #

RESERVOIR = 100_000


def pct(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


class _QStat:
    __slots__ = ("count", "ok", "errors", "lat", "seen", "svc_sum", "server_sum",
                 "rows", "msgs", "cancel_req", "cancelled")

    def __init__(self):
        self.count = self.ok = 0
        self.errors: Counter = Counter()
        self.lat: list[float] = []
        self.seen = 0
        self.svc_sum = self.server_sum = 0.0
        self.rows = 0
        self.msgs: dict[str, str] = {}
        self.cancel_req = self.cancelled = 0


class Recorder:
    def __init__(self, run_id: str, meta: dict):
        self.run_id = run_id
        self.meta = meta
        self.t0 = time.time()
        self.lock = threading.Lock()
        self.q: dict[str, _QStat] = defaultdict(_QStat)
        # per-second query buckets, keyed by second: {"ok":n,"err":n,"lat":[...]}
        self.sec: dict[int, dict] = defaultdict(lambda: {"ok": 0, "err": 0, "lat": []})
        self.events: list[dict] = []
        self.ingest: list[dict] = []          # per-interval ingestion samples
        self.ingest_sec: dict[int, Counter] = defaultdict(Counter)
        self.ingest_tot: Counter = Counter()
        self.ingest_lat: list[float] = []
        self.ingest_seen = 0
        self.server: list[dict] = []          # server stats samples
        self.level: list[tuple[float, float]] = []
        self.inflight = 0
        self.backlog = 0
        self._rng = random.Random(7)
        self.extra: dict = {}

    def now(self) -> float:
        return time.time() - self.t0

    # ---- queries ----------------------------------------------------------
    def record_query(self, name: str, t_start: float, latency_ms: float, *,
                     service_ms: float, ok: bool, code: str = "", msg: str = "",
                     server_ms: float = 0.0, rows: int = 0) -> None:
        with self.lock:
            s = self.q[name]
            s.count += 1
            s.svc_sum += service_ms
            s.server_sum += server_ms
            b = self.sec[int(t_start + latency_ms / 1000.0)]
            if ok:
                s.ok += 1
                s.rows += rows
                b["ok"] += 1
                b["lat"].append(latency_ms)
                s.seen += 1
                if len(s.lat) < RESERVOIR:
                    s.lat.append(latency_ms)
                else:
                    j = self._rng.randrange(s.seen)
                    if j < RESERVOIR:
                        s.lat[j] = latency_ms
            else:
                s.errors[code] += 1
                if code != "cancelled":  # intentional (--cancel-pct), so don't count it as an error
                    b["err"] += 1
                if code not in s.msgs:
                    s.msgs[code] = msg[:400]

    def record_cancel(self, name: str, cancelled: bool) -> None:
        with self.lock:
            s = self.q[name]
            s.cancel_req += 1
            s.cancelled += int(cancelled)

    # ---- ingestion --------------------------------------------------------
    def record_ingest(self, op: str, ok: bool, latency_ms: float, code: str = "") -> None:
        with self.lock:
            sec = int(self.now())
            c = self.ingest_sec[sec]
            if ok:
                c[op] += 1
                self.ingest_tot[op] += 1
                self.ingest_seen += 1
                if len(self.ingest_lat) < RESERVOIR:
                    self.ingest_lat.append(latency_ms)
                else:
                    j = self._rng.randrange(self.ingest_seen)
                    if j < RESERVOIR:
                        self.ingest_lat[j] = latency_ms
            else:
                c["err"] += 1
                self.ingest_tot[f"err:{code or op}"] += 1

    def record_ingest_bulk(self, op: str, n: int) -> None:
        """For external engines (pillowfight) that only report rates."""
        with self.lock:
            self.ingest_sec[int(self.now())][op] += n
            self.ingest_tot[op] += n

    def record_ingest_spread(self, op: str, n: int, t_from: float, t_to: float) -> None:
        """Attribute n ops evenly to the seconds between t_from and t_to."""
        with self.lock:
            a, b = int(t_from), int(t_to)
            secs = list(range(a, b + 1)) or [b]
            share, rem = divmod(n, len(secs))
            for i, s in enumerate(secs):
                self.ingest_sec[s][op] += share + (1 if i < rem else 0)
            self.ingest_tot[op] += n

    def add_ingest_sample(self, sample: dict) -> None:
        with self.lock:
            sample["t"] = round(self.now(), 2)
            self.ingest.append(sample)

    # ---- misc -------------------------------------------------------------
    def event(self, kind: str, detail: str, **kw) -> None:
        with self.lock:
            e = {"t": round(self.now(), 2), "kind": kind, "detail": detail, **kw}
            self.events.append(e)
        info(f"[{e['t']:7.1f}s] {kind}: {detail}")

    def add_server_sample(self, sample: dict) -> None:
        with self.lock:
            sample["t"] = round(self.now(), 2)
            self.server.append(sample)

    def note_level(self, lvl: float) -> None:
        with self.lock:
            if not self.level or abs(self.level[-1][1] - lvl) > 1e-6:
                self.level.append((round(self.now(), 2), round(lvl, 4)))

    # ---- live line --------------------------------------------------------
    def live_line(self, window: int) -> str:
        now = int(self.now())
        with self.lock:
            okc = errc = 0
            lats: list[float] = []
            ing = Counter()
            for s in range(now - window, now):
                b = self.sec.get(s)
                if b:
                    okc += b["ok"]
                    errc += b["err"]
                    lats.extend(b["lat"])
                c = self.ingest_sec.get(s)
                if c:
                    ing.update(c)
            lag = next((x for x in reversed(self.ingest)
                        if x.get("lag_ms") is not None or x.get("hb_stale")), None)
            gap = next((x for x in reversed(self.ingest) if x.get("gap") is not None), None)
            inflight, backlog = self.inflight, self.backlog
        lats.sort()
        parts = [f"t={self.now():6.0f}s"]
        if okc or errc or self.q:
            p50, p99 = pct(lats, .5), pct(lats, .99)
            parts.append(f"q/s={okc / window:7.1f} err/s={errc / window:5.1f} "
                         f"p50={_ms(p50)} p99={_ms(p99)} inflight={inflight}"
                         + (f" backlog={backlog}" if backlog else ""))
        if ing or self.ingest:
            ops = sum(v for k, v in ing.items() if k != "err")
            parts.append(f"kv ops/s={ops / window:8.1f} kv err/s={ing['err'] / window:4.1f}")
            if lag or gap:
                lag_s = "n/a" if lag and lag.get("hb_stale") else \
                    _ms(lag.get('lag_ms') if lag else None).strip()
                parts.append(f"lag={lag_s} "
                             f"gap={gap.get('gap') if gap else '-'}")
        return "  ".join(parts)

    # ---- export -----------------------------------------------------------
    def summary(self) -> dict:
        with self.lock:
            per = {}
            all_lat: list[float] = []
            tot = Counter()
            for name, s in sorted(self.q.items()):
                lat = sorted(s.lat)
                all_lat.extend(s.lat)
                per[name] = {
                    "count": s.count, "ok": s.ok, "errors": dict(s.errors),
                    "error_samples": s.msgs,
                    "p50": pct(lat, .5), "p90": pct(lat, .9), "p95": pct(lat, .95),
                    "p99": pct(lat, .99), "max": lat[-1] if lat else None,
                    "mean": (sum(lat) / len(lat)) if lat else None,
                    "avg_service_ms": s.svc_sum / s.count if s.count else None,
                    "avg_server_ms": s.server_sum / s.count if s.count else None,
                    "avg_rows": s.rows / s.ok if s.ok else None,
                    "cancel_requested": s.cancel_req, "cancelled": s.cancelled,
                }
                tot["count"] += s.count
                tot["ok"] += s.ok
                tot["cancelled"] += s.errors.get("cancelled", 0)
            all_lat.sort()
            elapsed = max(1e-9, self.now())
            ilat = sorted(self.ingest_lat)
            ing_ops = sum(v for k, v in self.ingest_tot.items() if not k.startswith("err:"))
            lags = [x["lag_ms"] for x in self.ingest if x.get("lag_ms") is not None]
            return {
                "elapsed_s": round(elapsed, 2),
                "queries": {
                    "total": tot["count"], "ok": tot["ok"], "cancelled": tot["cancelled"],
                    "errors": tot["count"] - tot["ok"] - tot["cancelled"],
                    "qps": tot["count"] / elapsed,
                    "p50": pct(all_lat, .5), "p95": pct(all_lat, .95),
                    "p99": pct(all_lat, .99), "max": all_lat[-1] if all_lat else None,
                },
                "per_query": per,
                "ingest": {
                    "ops": ing_ops, "ops_per_s": ing_ops / elapsed,
                    "by_op": dict(self.ingest_tot),
                    "p50": pct(ilat, .5), "p99": pct(ilat, .99),
                    "lag_ms_max": max(lags) if lags else None,
                    "lag_ms_p50": pct(sorted(lags), .5) if lags else None,
                    "lag_ms_final": lags[-1] if lags else None,
                } if (self.ingest_tot or self.ingest) else None,
            }

    def to_json(self) -> dict:
        summ = self.summary()
        with self.lock:
            secs = sorted(set(self.sec) | set(self.ingest_sec))
            series = []
            for s in secs:
                b = self.sec.get(s, {"ok": 0, "err": 0, "lat": []})
                lat = sorted(b["lat"])
                ing = self.ingest_sec.get(s, Counter())
                series.append({
                    "t": s, "ok": b["ok"], "err": b["err"],
                    "p50": pct(lat, .5), "p95": pct(lat, .95), "p99": pct(lat, .99),
                    "max": lat[-1] if lat else None,
                    "kv": {k: v for k, v in ing.items()},
                })
            return {
                "run_id": self.run_id, "started": self.t0, "meta": self.meta,
                "summary": summ, "series": series, "events": list(self.events),
                "ingest": list(self.ingest), "server": list(self.server),
                "level": list(self.level), "extra": self.extra,
            }


def _ms(v) -> str:
    if v is None:
        return "    -"
    if v >= 10_000:
        return f"{v / 1000:5.1f}s"
    return f"{v:5.0f}ms"


# --------------------------------------------------------------------------- #
# Server stats sampler
# --------------------------------------------------------------------------- #

def _flatten(prefix: str, obj, out: dict) -> None:  # noqa: unused helper kept for JSON stats
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        out[prefix] = obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _flatten(f"{prefix}.{k}" if prefix else str(k), v, out)


_PROM = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|\+Inf|-Inf)")


def parse_prometheus(text: str, prefix: str = "cbas_") -> dict[str, float]:
    """Sum every series of each metric name (labels dropped)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line[0] == "#" or not line.startswith(prefix):
            continue
        m = _PROM.match(line)
        if not m:
            continue
        try:
            v = float(m.group(3))
        except ValueError:
            continue
        if math.isnan(v) or math.isinf(v):
            continue
        out[m.group(1)] = out.get(m.group(1), 0.0) + v
    return out


class Sampler(threading.Thread):
    """Polls each analytics node's Prometheus endpoint (cbas_* metrics, with
    counters turned into per-second rates), active request counts and node CPU/RAM."""

    def __init__(self, target: Target, rec: Recorder, interval: float):
        super().__init__(daemon=True, name="sampler")
        self.t, self.rec, self.interval = target, rec, interval
        self.stop_ev = threading.Event()
        self._warned: set[str] = set()
        self._prev: dict[str, tuple[float, float]] = {}

    def run(self) -> None:
        while not self.stop_ev.wait(self.interval):
            sample: dict = {}
            for i, u in enumerate(self.t.query_urls, start=1):
                n = f"n{i}"
                try:
                    st, body = self.t.http.request("GET", f"{u}/_prometheusMetrics", timeout=5)
                    if st != 200:
                        raise RuntimeError(f"HTTP {st}")
                    now = time.time()
                    for k, v in parse_prometheus(body.decode(errors="replace")).items():
                        if k.endswith("_total"):
                            key = f"{n}.{k[:-6]}_per_s"
                            prev = self._prev.get(key)
                            self._prev[key] = (now, v)
                            if prev and now > prev[0] and v >= prev[1]:
                                sample[key] = round((v - prev[1]) / (now - prev[0]), 3)
                        elif k.endswith("_bytes"):
                            sample[f"{n}.{k[:-6]}_mib"] = round(v / 1048576, 1)
                        else:
                            sample[f"{n}.{k}"] = v
                except Exception as e:  # noqa: BLE001
                    self._warn_once(f"prom:{u}", f"prometheus metrics unavailable on {u}: {e}")
                try:
                    ar = self.t.http.get_json(f"{u}/analytics/admin/active_requests", timeout=5)
                    if isinstance(ar, list):
                        sample[f"{n}.active_requests"] = len(ar)
                        sample[f"{n}.active_requests_running"] = sum(
                            1 for r in ar if str(r.get("state", "")).lower() == "running")
                except Exception as e:  # noqa: BLE001
                    self._warn_once(f"ar:{u}", f"active_requests unavailable on {u}: {e}")
            try:
                pools = self.t.http.get_json(f"{self.t.mgmt_url}/pools/default", timeout=5)
                for j, nd in enumerate(pools.get("nodes", []), start=1):
                    ss = nd.get("systemStats", {})
                    h = f"node{j}"
                    if "cpu_utilization_rate" in ss:
                        sample[f"{h}.cpu_pct"] = round(ss["cpu_utilization_rate"], 2)
                    if ss.get("mem_total"):
                        sample[f"{h}.mem_used_pct"] = round(
                            100 * (1 - ss.get("mem_free", 0) / ss["mem_total"]), 2)
                    sample[f"{h}.healthy"] = 1 if nd.get("status") == "healthy" else 0
            except Exception as e:  # noqa: BLE001
                self._warn_once("pools", f"/pools/default unavailable: {e}")
            if sample:
                self.rec.add_server_sample(sample)

    def _warn_once(self, key: str, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.rec.event("sampler", msg)

    def stop(self) -> None:
        self.stop_ev.set()


class LivePrinter(threading.Thread):
    def __init__(self, rec: Recorder, interval: float):
        super().__init__(daemon=True, name="live")
        self.rec, self.interval = rec, max(1.0, interval)
        self.stop_ev = threading.Event()

    def run(self) -> None:
        while not self.stop_ev.wait(self.interval):
            print("  " + self.rec.live_line(int(self.interval)), flush=True)

    def stop(self) -> None:
        self.stop_ev.set()
