"""
docgen.py - deterministic synthetic JSON documents for Analytics load.

Doc n is a pure function of (seed, n, version). As such, regenerating doc 42
always gives the same doc, so query placeholders (${key}, ${customer}) can
target real data and ingestion updates can bump `version` reproducibly.

Templates
  flat     scalar fields only (an orders-like record)
  nested   flat plus address{...geo{}}, profile{prefs{}}, metrics{}
  arrays   flat plus items[1..N]{sku,qty,price,cat}, events[] (UNNEST-heavy)
  orders   arrays-shaped orders plus a companion `<coll>_customers` collection
           (joinable on orders.customerId = customers.custNo)
  customers   the companion template (used automatically by `orders`)

Knobs
  size         target serialised size per doc: "1KB" or "512B-16KB" (uniform)
  pad          what the padding looks like: random | compressible | none
  skew         Zipf exponent for customerId (0 = uniform, 1.1 gives a typical hot set)
  cardinality  distinct customers
  drift        fraction of docs with schema drift (missing/extra/retyped fields)
  bigints      add integers > 2^53 (SUPP-69 / MB-65767 shape)
"""

from __future__ import annotations

import bisect
import json
import random
import string
from dataclasses import dataclass, asdict

TEMPLATES = ("flat", "nested", "arrays", "orders", "customers")

COUNTRIES = ["US", "UK", "DE", "FR", "JP", "IN", "BR", "AU", "CA", "SG", "NL", "ES"]
STATUSES = ["new", "paid", "shipped", "delivered", "returned", "cancelled"]
CATEGORIES = ["books", "games", "garden", "grocery", "health", "home",
              "kitchen", "music", "outdoors", "sports", "tools", "toys"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CITIES = ["Springfield", "Riverside", "Fairview", "Franklin", "Greenville",
          "Bristol", "Clinton", "Salem", "Madison", "Georgetown"]

EPOCH0 = 1_704_067_200  # 2024-01-01T00:00:00Z, fixed so the data is reproducible
DAY = 86_400


def parse_size(s: str | int | None) -> int:
    if s is None or s == "":
        return 0
    if isinstance(s, int):
        return s
    s = s.strip().upper().replace("IB", "B")
    mult = 1
    for suf, m in (("KB", 1024), ("MB", 1024 ** 2), ("K", 1024), ("M", 1024 ** 2), ("B", 1)):
        if s.endswith(suf):
            s, mult = s[: -len(suf)], m
            break
    return int(float(s) * mult)


def parse_size_range(s: str | None) -> tuple[int, int]:
    if not s:
        return (0, 0)
    if "-" in s:
        a, b = s.split("-", 1)
        lo, hi = parse_size(a), parse_size(b)
        return (min(lo, hi), max(lo, hi))
    v = parse_size(s)
    return (v, v)


def iso(ts: int) -> str:
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(ts))


@dataclass
class DocSpec:
    template: str = "flat"
    seed: int = 42
    size: str = ""              # "" = natural size, else "1KB" / "512B-4KB"
    pad: str = "random"         # random | compressible | none
    skew: float = 1.1
    cardinality: int = 10_000   # distinct customers
    days: int = 365             # created_at spread
    drift: float = 0.0
    bigints: bool = False
    key_prefix: str = "doc::"
    max_items: int = 8          # arrays/orders: items per doc 1..max_items

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DocSpec":
        keys = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in d.items() if k in keys})


def add_doc_args(p, *, default_template: str = "flat") -> None:
    g = p.add_argument_group("document shape")
    g.add_argument("--template", choices=[t for t in TEMPLATES if t != "customers"],
                   default=default_template)
    g.add_argument("--doc-size", default="",
                   help="target bytes per doc, e.g. 1KB or a range 512B-16KB "
                        "(default: natural size, ~300-900B by template)")
    g.add_argument("--pad", choices=["random", "compressible", "none"], default="random",
                   help="padding content: random (incompressible), compressible "
                        "(repetitive text), none")
    g.add_argument("--skew", type=float, default=1.1,
                   help="Zipf exponent for customerId hot-keys (0 = uniform)")
    g.add_argument("--cardinality", type=int, default=10_000,
                   help="distinct customerId values (default 10000)")
    g.add_argument("--days", type=int, default=365, help="created_at spread in days")
    g.add_argument("--drift", type=float, default=0.0,
                   help="fraction 0..1 of docs with schema drift (default 0)")
    g.add_argument("--bigints", action="store_true",
                   help="include integers > 2^53 (bigId field)")
    g.add_argument("--max-items", type=int, default=8,
                   help="max array items per doc for arrays/orders templates")
    g.add_argument("--seed", type=int, default=42, help="RNG seed (reproducible data)")


def spec_from_args(args, template: str | None = None) -> DocSpec:
    return DocSpec(template=template or args.template, seed=args.seed,
                   size=args.doc_size, pad=args.pad, skew=args.skew,
                   cardinality=args.cardinality, days=args.days, drift=args.drift,
                   bigints=args.bigints, max_items=args.max_items,
                   key_prefix="cust::" if (template or args.template) == "customers" else "doc::")


class Zipf:
    """Samples 0..n-1 with P(k) proportional to 1/(k+1)^s, using a precomputed CDF."""

    def __init__(self, n: int, s: float):
        self.n = max(1, n)
        self.s = s
        if s <= 0:
            self.cdf = None
            return
        acc, cdf = 0.0, []
        for k in range(self.n):
            acc += 1.0 / ((k + 1) ** s)
            cdf.append(acc)
        self.cdf = [c / acc for c in cdf]

    def sample(self, rng: random.Random) -> int:
        if self.cdf is None:
            return rng.randrange(self.n)
        return min(self.n - 1, bisect.bisect_left(self.cdf, rng.random()))


_RAND_POOL: str | None = None
_TEXT = ("the quick brown fox jumps over the lazy dog while analytics "
         "ingests another mutation from the data service ")


def _rand_pool() -> str:
    global _RAND_POOL
    if _RAND_POOL is None:
        r = random.Random(1234567)
        alphabet = string.ascii_letters + string.digits
        _RAND_POOL = "".join(r.choice(alphabet) for _ in range(1 << 20))
    return _RAND_POOL


class DocGen:
    def __init__(self, spec: DocSpec):
        self.spec = spec
        self.size_lo, self.size_hi = parse_size_range(spec.size)
        self.zipf = Zipf(spec.cardinality, spec.skew)

    # -- keys ---------------------------------------------------------------
    def key(self, n: int) -> str:
        return f"{self.spec.key_prefix}{n}"

    def _rng(self, n: int, version: int) -> random.Random:
        return random.Random((self.spec.seed * 1_000_003 + n) * 131 + version)

    # -- docs ---------------------------------------------------------------
    def doc(self, n: int, version: int = 0, now: int | None = None) -> dict:
        sp = self.spec
        r = self._rng(n, version)
        if sp.template == "customers":
            d = self._customer(n, r)
        else:
            d = self._base(n, r)
            if sp.template in ("nested",):
                self._add_nested(d, r)
            if sp.template in ("arrays", "orders"):
                self._add_arrays(d, r)
        if version:
            d["version"] = version
            d["updated_at"] = iso(now) if now else d.get("created_at")
        if sp.bigints:
            d["bigId"] = (1 << 53) + n * 7 + 1
        if sp.drift > 0 and r.random() < sp.drift:
            self._drift(d, r)
        if self.size_hi > 0 and sp.pad != "none":
            self._pad(d, r)
        return d

    def _base(self, n: int, r: random.Random) -> dict:
        ts = EPOCH0 + r.randrange(max(1, self.spec.days) * DAY)
        cust = self.zipf.sample(r)
        return {
            "id": self.key(n),
            "docType": "order" if self.spec.template == "orders" else "record",
            "seq": n,
            "customerId": cust,
            "name": f"user_{cust}",
            "email": f"user_{cust}@example.com",
            "age": 18 + (cust % 62),
            "country": COUNTRIES[r.randrange(len(COUNTRIES))],
            "status": STATUSES[min(len(STATUSES) - 1, int(r.expovariate(0.9)))],
            "category": CATEGORIES[r.randrange(len(CATEGORIES))],
            "amount": round(r.lognormvariate(3.5, 1.0), 2),
            "score": round(r.random() * 100, 2),
            "active": r.random() < 0.7,
            "created_at": iso(ts),
            "ts": ts,
            "tags": r.sample(["new", "promo", "vip", "gift", "bulk", "intl",
                              "repeat", "mobile"], k=r.randint(0, 3)),
        }

    def _add_nested(self, d: dict, r: random.Random) -> None:
        d["address"] = {
            "street": f"{r.randint(1, 9999)} Main St",
            "city": CITIES[r.randrange(len(CITIES))],
            "country": d["country"],
            "geo": {"lat": round(r.uniform(-60, 70), 5), "lon": round(r.uniform(-170, 170), 5)},
        }
        d["profile"] = {
            "tier": TIERS[min(3, int(r.expovariate(1.2)))],
            "prefs": {"email": r.random() < 0.5, "sms": r.random() < 0.2,
                      "lang": r.choice(["en", "de", "fr", "ja", "pt"])},
        }
        d["metrics"] = {"visits": r.randint(0, 500), "clicks": r.randint(0, 5000),
                        "ltv": round(r.lognormvariate(5, 1), 2)}

    def _add_arrays(self, d: dict, r: random.Random) -> None:
        k = r.randint(1, max(1, self.spec.max_items))
        items = []
        total = 0.0
        for _ in range(k):
            price = round(r.lognormvariate(2.5, 0.8), 2)
            qty = r.randint(1, 5)
            total += price * qty
            items.append({"sku": f"SKU-{r.randint(1, 5000):05d}", "qty": qty,
                          "price": price, "cat": CATEGORIES[r.randrange(len(CATEGORIES))]})
        d["items"] = items
        d["total"] = round(total, 2)
        d["events"] = [{"type": r.choice(["view", "cart", "pay", "ship"]),
                        "ts": d["ts"] + i * r.randint(60, 7200)}
                       for i in range(r.randint(1, 5))]

    def _customer(self, n: int, r: random.Random) -> dict:
        return {
            "id": self.key(n),
            "docType": "customer",
            "custNo": n,
            "name": f"user_{n}",
            "email": f"user_{n}@example.com",
            "tier": TIERS[min(3, int(r.expovariate(1.2)))],
            "country": COUNTRIES[r.randrange(len(COUNTRIES))],
            "since": iso(EPOCH0 - r.randrange(5 * 365) * DAY),
            "creditLimit": r.choice([500, 1000, 2500, 5000, 10000]),
        }

    def _drift(self, d: dict, r: random.Random) -> None:
        kind = r.randrange(4)
        if kind == 0:   # missing field
            d.pop(r.choice(["email", "age", "score", "tags", "category"]), None)
        elif kind == 1:  # extra field
            d[f"extra_{r.randint(0, 9)}"] = r.choice([1, "x", True, {"k": 1}, [1, 2]])
        elif kind == 2:  # type change
            if "amount" in d:
                d["amount"] = str(d["amount"])
            if "age" in d:
                d["age"] = str(d["age"])
        else:  # scalar becomes an object
            if "country" in d:
                d["country"] = {"code": d["country"], "legacy": True}

    def _pad(self, d: dict, r: random.Random) -> None:
        target = r.randint(self.size_lo, self.size_hi) if self.size_hi > self.size_lo \
            else self.size_hi
        cur = len(json.dumps(d, separators=(",", ":"))) + len(',"pad":""')
        need = target - cur
        if need <= 0:
            return
        if self.spec.pad == "compressible":
            reps = need // len(_TEXT) + 1
            d["pad"] = (_TEXT * reps)[:need]
        else:
            pool = _rand_pool()
            if need >= len(pool):
                d["pad"] = (pool * (need // len(pool) + 1))[:need]
            else:
                off = r.randrange(len(pool) - need)
                d["pad"] = pool[off: off + need]

    # -- helpers for query placeholders -------------------------------------
    def sample_customer(self, r: random.Random) -> int:
        return self.zipf.sample(r)

    def date_range(self, r: random.Random, span_days: int) -> tuple[str, str]:
        start = EPOCH0 + r.randrange(max(1, self.spec.days - span_days + 1)) * DAY
        return iso(start), iso(start + span_days * DAY)
