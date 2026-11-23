#!/usr/bin/env python3
"""
cbasload.py - data and load generator for Couchbase Analytics
(Enterprise Analytics / Columnar, and Server with the Analytics Service).

  gen          create a collection of synthetic docs (standalone, external S3,
               or remote-link from an auto-created Couchbase source)
  query-load   weighted SQL++ query workload (closed-loop or open-loop QPS)
  ingest-load  KV mutation workload into a remote-link source, with lag measurement
  mixed        query-load and ingest-load at the same time
  preset       named scenarios (including known-issue repro shapes from SupportabilityTool.xlsx)
  status / drop / source / ping / report / chaos-actions

By default it targets the running EA_repro env. Use --host/-u/-p for any other cluster.
Every run writes runs/<id>/run.json and report.html.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import docgen  # noqa: E402
import ingest  # noqa: E402
import querymix  # noqa: E402
import state  # noqa: E402
from chaos import ACTIONS, Chaos, parse_chaos  # noqa: E402
from recorder import (LivePrinter, Recorder, Sampler, add_run_args,  # noqa: E402
                      parse_duration, profile_from_args)
from report import write_report  # noqa: E402
from target import (CbasError, Target, add_target_args, bq, die, first_error,  # noqa: E402
                    fq_name, info, ok, resolve_target, step, warn)

RUNS = HERE / "runs"
PRESETS = HERE / "presets.json"


# --------------------------------------------------------------------------- #
# Collection helpers
# --------------------------------------------------------------------------- #

def scope_of(t: Target, args) -> str:
    return args.scope or t.default_scope()


def ensure_scope(t: Target, scope: str) -> None:
    parts = scope.split(".")
    if parts in (["Default", "Default"], ["Default"]):
        return
    if t.kind == "ea" and len(parts) == 2:
        t.q_any([f"CREATE DATABASE {bq(parts[0])} IF NOT EXISTS;"], ok_if=("exists",))
        t.q_any([f"CREATE SCOPE {bq(parts[0])}.{bq(parts[1])} IF NOT EXISTS;"],
                ok_if=("exists",))
    else:
        name = ".".join(bq(p) for p in parts)
        t.q_any([f"CREATE SCOPE {name} IF NOT EXISTS;",
                 f"CREATE DATAVERSE {name} IF NOT EXISTS;"], ok_if=("exists",))


def drop_coll(t: Target, fq: str) -> None:
    t.q_any([f"DROP COLLECTION {fq} IF EXISTS;",
             f"DROP ANALYTICS COLLECTION {fq} IF EXISTS;",
             f"DROP DATASET {fq} IF EXISTS;"])


def coll_exists(t: Target, fq: str) -> bool:
    r = t.query(f"SELECT VALUE 1 FROM {fq} LIMIT 1;", timeout=30)
    return r.get("status") == "success"


def create_standalone(t: Target, fq: str) -> None:
    t.q_any([f"CREATE COLLECTION {fq} IF NOT EXISTS PRIMARY KEY (id: string);",
             f"CREATE ANALYTICS COLLECTION {fq} IF NOT EXISTS PRIMARY KEY (id: string);"])


def create_indexes(t: Target, fq: str, coll: str, specs: list[str], into: str) -> None:
    if not specs:
        return
    if into == "external":
        warn("secondary indexes are not supported on external collections — skipped")
        return
    for s in specs:
        field, _, typ = s.partition(":")
        typ = typ or "string"
        name = f"idx_{coll}_{field.replace('.', '_')}"
        path = ".".join(bq(p) for p in field.split("."))
        t0 = time.time()
        t.q_any([f"CREATE INDEX {bq(name)} IF NOT EXISTS ON {fq}({path}: {typ});"])
        ok(f"index {name} on {field}:{typ} ({time.time() - t0:.1f}s)")


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #

def load_insert(t: Target, fq: str, gen: docgen.DocGen, n: int, *, threads: int,
                batch_bytes: int, start: int = 0) -> float:
    """Runs UPSERT INTO <standalone> ([docs...]) in parallel batches."""
    t0 = time.time()
    lock = threading.Lock()
    nxt = [start]
    done = [0]
    end = start + n
    errors: list[str] = []
    last_print = [0.0]

    def worker():
        while not errors:
            with lock:
                a = nxt[0]
                if a >= end:
                    return
                nxt[0] = min(end, a + 5000)
                b = nxt[0]
            buf, size, i = [], 0, a
            while i < b:
                s = json.dumps(gen.doc(i), separators=(",", ":"))
                buf.append(s)
                size += len(s) + 1
                i += 1
                if size >= batch_bytes or i == b:
                    stmt = f"UPSERT INTO {fq} ([{','.join(buf)}]);"
                    for attempt in range(4):
                        resp = t.query(stmt, timeout=600)
                        if resp.get("status") == "success":
                            break
                        if attempt == 3:
                            errors.append(first_error(resp))
                            return
                        time.sleep(1 + attempt * 2)
                    with lock:
                        done[0] += len(buf)
                        if time.time() - last_print[0] > 5:
                            last_print[0] = time.time()
                            rate = done[0] / max(1e-9, time.time() - t0)
                            info(f"  loaded {done[0]:,}/{n:,} ({rate:,.0f} docs/s)")
                    buf, size = [], 0

    ths = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, threads))]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    if errors:
        raise CbasError(f"insert load failed: {errors[0]}")
    return time.time() - t0


def _ea_repro():
    """Imports EA_repro's module for its S3/MinIO helpers (needs boto3)."""
    if str(state.HERE.parent / "EA_repro") not in sys.path:
        sys.path.insert(0, str(state.HERE.parent / "EA_repro"))
    import ea_repro  # noqa: WPS433
    return ea_repro


def upload_s3(t: Target, coll: str, gen: docgen.DocGen, n: int, docs_per_file: int,
              threads: int, *, start: int = 0, wipe: bool = True) -> tuple[str, str, str]:
    """Uploads NDJSON to EA_repro's MinIO data bucket. Returns (bucket, prefix, link)."""
    if not t.from_ea_repro:
        die("S3 loading (--into external / --load-method copy) uses EA_repro's MinIO; "
            "for other clusters use --load-method insert")
    ea = _ea_repro()
    cfg = ea.Config(**t.ea)
    s3 = ea.s3_client(cfg)
    prefix = f"cbasload/{coll}"
    if wipe:
        ea._delete_prefix(s3, cfg.data_bucket, prefix + "/")
    n_files = math.ceil(n / docs_per_file) if n else 0
    t0 = time.time()
    lock = threading.Lock()
    nxt = [0]
    done = [0]

    def worker():
        while True:
            with lock:
                fi = nxt[0]
                if fi >= n_files:
                    return
                nxt[0] += 1
            a = start + fi * docs_per_file
            b = min(start + n, a + docs_per_file)
            body = ("\n".join(json.dumps(gen.doc(i), separators=(",", ":"))
                              for i in range(a, b)) + "\n").encode()
            s3.put_object(Bucket=cfg.data_bucket, Key=f"{prefix}/part-{start}-{fi:06d}.json",
                          Body=body)
            with lock:
                done[0] += b - a
                if n_files >= 10 and nxt[0] % max(1, n_files // 10) == 0:
                    info(f"  uploaded {done[0]:,}/{n:,}")

    ths = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, threads))]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    ok(f"uploaded {n:,} docs to s3://{cfg.data_bucket}/{prefix}/ in {n_files} objects "
       f"({time.time() - t0:.1f}s)")
    ea.create_link(cfg)  # idempotent: makes sure the S3 link exists
    return cfg.data_bucket, prefix, cfg.link_name


def avg_doc_size(gen: docgen.DocGen, n: int) -> float:
    k = min(n, 200) or 1
    return sum(len(json.dumps(gen.doc(i), separators=(",", ":"))) for i in range(k)) / k


# --------------------------------------------------------------------------- #
# gen
# --------------------------------------------------------------------------- #

def cmd_gen(args) -> None:
    t = resolve_target(args)
    info(t.describe())
    scope = scope_of(t, args)
    into = args.into
    method = args.load_method or ("copy" if t.from_ea_repro else "insert")
    ensure_scope(t, scope)

    targets = [(args.collection, args.template, args.docs)]
    if args.template == "orders":
        targets.append((f"{args.collection}_customers", "customers", args.cardinality))

    companions = {}
    for coll, tmpl, n in targets:
        fq = fq_name(scope, coll)
        spec = docgen.spec_from_args(args, template=tmpl)
        gen = docgen.DocGen(spec)
        step(f"{fq}: {n:,} × {tmpl} docs → {into}"
             + (f" via {method}" if into == "standalone" else ""))
        info(f"avg doc size ≈ {avg_doc_size(gen, n):,.0f} B "
             f"(≈ {avg_doc_size(gen, n) * n / 1024 ** 2:,.1f} MiB raw JSON)")

        exists = coll_exists(t, fq)
        if exists and not (args.recreate or args.append):
            die(f"{fq} already exists — pass --recreate (drop & rebuild) or --append")
        if exists and args.recreate:
            drop_coll(t, fq)
            ok(f"dropped {fq}")
        start = 0
        if args.append and exists:
            prev = state.get_collection(t, fq) or {}
            start = int(prev.get("keyspace", 0))
            info(f"appending from key #{start:,}")

        t0 = time.time()
        meta_extra: dict = {}
        if into == "standalone":
            create_standalone(t, fq)
            if method == "copy":
                bucket, prefix, link = upload_s3(t, coll, gen, n, args.docs_per_file,
                                                 args.load_threads, start=start)
                t.q(f"COPY INTO {fq} FROM {bq(bucket)} AT {bq(link)} PATH \"{prefix}\" "
                    f"WITH {{\"format\":\"json\"}};", timeout=7200)
            else:
                load_insert(t, fq, gen, n, threads=args.load_threads,
                            batch_bytes=docgen.parse_size(args.batch_bytes), start=start)
        elif into == "external":
            bucket, prefix, link = upload_s3(t, coll, gen, n, args.docs_per_file,
                                             args.load_threads, start=start,
                                             wipe=not args.append)
            if not exists or args.recreate:
                t.q(f"CREATE EXTERNAL COLLECTION {fq} ON {bq(bucket)} AT {bq(link)} "
                    f"PATH \"{prefix}\" WITH {{\"format\":\"json\"}};")
            meta_extra = {"s3": f"s3://{bucket}/{prefix}"}
        else:  # remote
            src = ingest.source_up(t, args, coll)
            info(f"seeding {n:,} docs into {src.bucket}.{src.scope}.{src.collection} "
                 f"via SDK ({args.load_threads} threads)")
            secs = ingest.seed_source(src, gen, n, start=start, threads=args.load_threads)
            ok(f"seeded in {secs:.1f}s ({n / max(1e-9, secs):,.0f} docs/s)")
            ingest.ensure_link_and_collection(t, src, args.link_name, scope, coll)
            meta_extra = {"link": args.link_name, "source": src.to_dict()}
            want = start + n
            info(f"waiting for ingestion to reach {want:,} docs…")
            deadline = time.time() + parse_duration(args.ingest_timeout)
            while True:
                try:
                    c = t.count(fq, timeout=60)
                except CbasError as e:
                    c = -1
                    last = str(e)
                if c >= want:
                    break
                if time.time() > deadline:
                    die(f"ingestion stalled at {c:,}/{want:,}" + (f": {last}" if c < 0 else ""))
                time.sleep(2)
        took = time.time() - t0
        create_indexes(t, fq, coll, ["custNo:bigint"] if tmpl == "customers" else args.index,
                       into)
        counted = t.count(fq)
        mark = "✓" if counted >= start + n else "!"
        ok(f"{fq}: COUNT(*) = {counted:,} {mark}  ({took:.1f}s, "
           f"{n / max(1e-9, took):,.0f} docs/s)")
        if tmpl == "customers":
            companions["customers"] = fq
        state.put_collection(t, fq, {
            "collection": coll, "scope": scope, "into": into, "method": method,
            "keyspace": start + n, "spec": spec.to_dict(), "count": counted,
            "companions": {}, "created": time.time(), **meta_extra})
    if companions:
        fq = fq_name(scope, args.collection)
        m = state.get_collection(t, fq)
        m["companions"] = companions
        state.put_collection(t, fq, m)
    print()
    ok(f"next:  ./cbasload.py query-load --collection {args.collection} --duration 60s")


# --------------------------------------------------------------------------- #
# Run orchestration (query-load / ingest-load / mixed)
# --------------------------------------------------------------------------- #

def new_run(command: str, t: Target, args) -> Recorder:
    run_id = time.strftime("%Y%m%d-%H%M%S")
    meta = {"command": command, "target": t.describe(), "kind": t.kind,
            "argv": sys.argv[1:], "label": getattr(args, "label", ""),
            "args": {k: v for k, v in vars(args).items()
                     if k not in ("func", "password", "source_password")}}
    return Recorder(run_id, meta)


def collection_meta(t: Target, args, coll: str) -> tuple[str, dict, docgen.DocGen]:
    scope = scope_of(t, args)
    fq = fq_name(scope, coll)
    meta = state.get_collection(t, fq)
    if meta is None:
        warn(f"{fq} wasn't created by cbasload; assuming the default flat template "
             f"(placeholders like ${{key}} may miss)")
        try:
            n = t.count(fq)
        except CbasError as e:
            die(f"{fq} isn't queryable: {e}\n  create it with `./cbasload.py gen "
                f"--collection {coll}`")
        meta = {"keyspace": n, "spec": docgen.DocSpec().to_dict(), "companions": {},
                "collection": coll, "scope": scope}
    gen = docgen.DocGen(docgen.DocSpec.from_dict(meta["spec"]))
    return fq, meta, gen


def build_query_load(t: Target, args, rec: Recorder, profile) -> tuple[querymix.QueryLoad, list, querymix.Renderer]:
    coll = getattr(args, "query_collection", None) or args.collection
    fq, meta, gen = collection_meta(t, args, coll)
    tmpl = meta["spec"].get("template", "flat")
    mix = querymix.load_mix(args.mix, tmpl, args.only, args.weight)
    renderer = querymix.Renderer(fq, meta.get("companions", {}), gen, int(meta["keyspace"]))
    params = {}
    if args.scan_consistency:
        params["scan_consistency"] = args.scan_consistency
    ql = querymix.QueryLoad(t, rec, mix, renderer, profile, concurrency=args.concurrency,
                            qps=args.qps, max_inflight=args.max_inflight,
                            arrival=args.arrival,
                            query_timeout=parse_duration(args.query_timeout),
                            cancel_pct=args.cancel_pct, cancel_after=args.cancel_after,
                            seed=args.seed, params=params)
    rec.meta["query_mix"] = mix
    rec.meta["query_collection"] = fq
    return ql, mix, renderer


def build_ingest_load(t: Target, args, rec: Recorder, profile):
    scope = scope_of(t, args)
    coll = args.collection
    fq = fq_name(scope, coll)
    meta = state.get_collection(t, fq)
    if meta and meta.get("into") == "remote" and coll_exists(t, fq):
        src = ingest.Source.from_dict(meta["source"])
        if src.container and not state.container_running(src.container):
            src = ingest.source_up(t, args, src.collection)
        gen = docgen.DocGen(docgen.DocSpec.from_dict(meta["spec"]))
        keyspace = int(meta["keyspace"])
        link = meta.get("link", args.link_name)
        info(f"reusing remote-link collection {fq} (keyspace {keyspace:,})")
    else:
        if meta and meta.get("into") != "remote":
            die(f"{fq} is a {meta.get('into')} collection; ingest-load needs a remote-link "
                f"collection. Use another --collection name (it'll be created).")
        step(f"setting up ingestion source for {fq}")
        ensure_scope(t, scope)
        spec = docgen.spec_from_args(args)
        gen = docgen.DocGen(spec)
        src = ingest.source_up(t, args, coll)
        if args.seed_docs:
            info(f"seeding {args.seed_docs:,} docs")
            ingest.seed_source(src, gen, args.seed_docs, threads=8)
        link = args.link_name
        ingest.ensure_link_and_collection(t, src, link, scope, coll)
        keyspace = max(1, args.seed_docs)
        state.put_collection(t, fq, {
            "collection": coll, "scope": scope, "into": "remote", "method": "sdk",
            "keyspace": keyspace, "spec": spec.to_dict(), "count": None, "companions": {},
            "created": time.time(), "link": link, "source": src.to_dict()})
    il = ingest.IngestLoad(t, rec, src, fq, gen, profile, keyspace=keyspace, rate=args.rate,
                           threads=args.writers, ops=ingest.parse_mix(args.ops),
                           ttl=parse_duration(args.ttl), key_skew=args.key_skew,
                           engine=args.engine, hb_interval=parse_duration(args.hb_interval),
                           probe_interval=parse_duration(args.probe_interval),
                           count_every=parse_duration(args.count_every), seed=args.seed)
    rec.meta["ingest_collection"] = fq
    rec.meta["source"] = {k: v for k, v in src.to_dict().items() if k != "password"}
    return il, src, link


def run_workloads(t: Target, args, rec: Recorder, *, ql=None, il=None, src=None, link=None,
                  mix=None, renderer=None) -> None:
    dur = parse_duration(args.duration)
    sampler = None
    if parse_duration(args.sample_interval) > 0:
        sampler = Sampler(t, rec, parse_duration(args.sample_interval))
    live = LivePrinter(rec, parse_duration(args.interval))
    chaos = None
    if args.chaos:
        chaos = Chaos(t, rec, parse_chaos(args.chaos), link=link, scope=scope_of(t, args),
                      ingest=il, source=src,
                      source_args={"ram": getattr(args, "source_bucket_ram", 512),
                                   "compression": getattr(args, "source_compression", "passive")})
    step(f"running for {dur:g}s  (Ctrl-C stops early; report is still written)")
    if ql:
        info("queries: " + ql.describe())
    if il:
        info("ingest:  " + il.describe())
    rec.t0 = time.time()
    rec.event("run", "start")
    try:
        if il:
            il.start()
        if ql:
            ql.start()
        if sampler:
            sampler.start()
        live.start()
        if chaos:
            chaos.start()
        end = time.time() + dur
        while time.time() < end:
            time.sleep(min(0.5, max(0.0, end - time.time())))
    except KeyboardInterrupt:
        warn("interrupted — stopping and writing the report")
    finally:
        rec.event("run", "stop")
        for w in (chaos, ql, il, sampler, live):
            if w:
                try:
                    w.stop()
                except Exception as e:  # noqa: BLE001
                    warn(f"stop {type(w).__name__}: {e}")
    _persist_keyspace(t, il)
    if il and args.drain:
        _drain(t, il, rec, parse_duration(args.drain))
    if ql and args.post_check:
        step("post-check: each query, uncancelled, after the load")
        rec.extra["post_check"] = querymix.post_check(t, mix, renderer, rec,
                                                      repeats=args.post_check)
        bad = [r for r in rec.extra["post_check"] if not r["ok"]]
        (warn if bad else ok)(f"post-check: {len(bad)} failure(s) of "
                              f"{len(rec.extra['post_check'])}")
    finish(rec, args)


def _drain(t: Target, il, rec: Recorder, max_wait: float) -> None:
    step(f"drain: waiting up to {max_wait:g}s for Analytics to catch up with KV")
    t0 = time.time()
    equal_streak = 0
    while time.time() - t0 < max_wait:
        kv = il.src.item_count()
        try:
            cb = t.count(il.fq, timeout=60)
        except CbasError:
            cb = None
        gap = (kv - cb) if kv is not None and cb is not None else None
        rec.add_ingest_sample({"kv_items": kv, "cbas_items": cb, "gap": gap, "phase": "drain"})
        # KV stats refresh every few seconds, so require two equal readings in a row
        equal_streak = equal_streak + 1 if gap == 0 else 0
        if equal_streak >= 2:
            ok(f"caught up after {time.time() - t0:.1f}s (kv={kv:,} analytics={cb:,})")
            rec.extra["drain_s"] = round(time.time() - t0, 1)
            return
        info(f"  kv={kv} analytics={cb} gap={gap}")
        time.sleep(2)
    warn("drain timed out")
    rec.extra["drain_s"] = None


def _persist_keyspace(t: Target, il) -> None:
    """Inserts grow the keyspace, so save it and later ${key} lookups can hit the new docs."""
    if il is None or il.engine != "sdk":
        return
    meta = state.get_collection(t, il.fq)
    if meta and il.next_new > int(meta.get("keyspace", 0)):
        meta["keyspace"] = il.next_new
        state.put_collection(t, il.fq, meta)


def finish(rec: Recorder, args) -> None:
    run = rec.to_json()
    out = Path(args.out) if args.out else RUNS / f"{rec.run_id}-{rec.meta['command']}"
    p = write_report(run, out)
    s = run["summary"]
    step("summary")
    q = s["queries"]
    if q["total"]:
        print(f"  queries   {q['total']:,} ({q['qps']:.1f}/s)  errors {q['errors']:,}  "
              f"p50 {_fmt(q['p50'])}  p95 {_fmt(q['p95'])}  p99 {_fmt(q['p99'])}  "
              f"max {_fmt(q['max'])}" + (f"  cancelled {q['cancelled']:,}" if q['cancelled'] else ""))
        for name, st in s["per_query"].items():
            errs = st["count"] - st["ok"] - st["errors"].get("cancelled", 0)
            print(f"    {name:<22} n={st['count']:<7,} err={errs:<5} p50={_fmt(st['p50']):>8} "
                  f"p99={_fmt(st['p99']):>8}"
                  + (f"  top error {max((k for k in st['errors'] if k != 'cancelled'), key=st['errors'].get)}" if errs else "")
                  + (f"  cancelled {st['errors']['cancelled']}" if st['errors'].get('cancelled') else ""))
    if s.get("ingest"):
        i = s["ingest"]
        print(f"  ingest    {i['ops']:,} ops ({i['ops_per_s']:.0f}/s)  "
              f"lag p50 {_fmt(i['lag_ms_p50'])} max {_fmt(i['lag_ms_max'])}  "
              f"by op {json.dumps(i['by_op'])}")
    ok(f"report: {p}")


def _fmt(v) -> str:
    if v is None:
        return "-"
    return f"{v / 1000:.2f}s" if v >= 10_000 else f"{v:.0f}ms"


def cmd_query_load(args) -> None:
    t = resolve_target(args)
    info(t.describe())
    rec = new_run("query-load", t, args)
    prof = profile_from_args(args)
    ql, mix, renderer = build_query_load(t, args, rec, prof)
    args.drain = None
    run_workloads(t, args, rec, ql=ql, mix=mix, renderer=renderer)


def cmd_ingest_load(args) -> None:
    t = resolve_target(args)
    info(t.describe())
    rec = new_run("ingest-load", t, args)
    prof = profile_from_args(args)
    il, src, link = build_ingest_load(t, args, rec, prof)
    args.post_check = 0
    run_workloads(t, args, rec, il=il, src=src, link=link)


def cmd_mixed(args) -> None:
    t = resolve_target(args)
    info(t.describe())
    rec = new_run("mixed", t, args)
    prof = profile_from_args(args)
    il, src, link = build_ingest_load(t, args, rec, prof)
    ql, mix, renderer = build_query_load(t, args, rec, prof)
    run_workloads(t, args, rec, ql=ql, il=il, src=src, link=link, mix=mix, renderer=renderer)


# --------------------------------------------------------------------------- #
# Misc commands
# --------------------------------------------------------------------------- #

def cmd_ping(args) -> None:
    t = resolve_target(args)
    print(t.describe())
    if t.ea_state_path:
        print(f"EA_repro state: {t.ea_state_path}")
    for u in t.query_urls:
        t2 = t
        r = t2.query("SELECT VALUE 1;", timeout=10, url=u)
        print(f"  {u}: {'ok' if r.get('status') == 'success' else first_error(r)}")


def cmd_status(args) -> None:
    t = resolve_target(args)
    print(t.describe())
    st = state.all_for(t)
    colls = st.get("collections", {})
    if not colls:
        print("  no cbasload collections recorded for this target")
    for fq, m in colls.items():
        try:
            c = f"{t.count(fq, timeout=30):,}"
        except Exception as e:  # noqa: BLE001
            c = f"unavailable ({str(e).splitlines()[0][:80]})"
        print(f"  {fq:<40} {m['into']:<10} template={m['spec']['template']:<9} "
              f"keyspace={m['keyspace']:<10,} count={c}")
    src = st.get("source")
    if src:
        run = state.container_running(src["container"]) if src.get("container") else None
        print(f"  source: {src['mode']} {src['mgmt_url']} bucket={src['bucket']} "
              + (f"container={src['container']} running={run}" if src.get("container") else ""))


def cmd_drop(args) -> None:
    t = resolve_target(args)
    scope = scope_of(t, args)
    names = [args.collection]
    m = state.get_collection(t, fq_name(scope, args.collection))
    if m:
        names += [v.split(".")[-1].strip("`") for v in m.get("companions", {}).values()]
    for n in names:
        fq = fq_name(scope, n)
        drop_coll(t, fq)
        state.drop_collection(t, fq)
        ok(f"dropped {fq}")
    if args.with_source:
        ingest.source_down(t)


def cmd_source(args) -> None:
    t = resolve_target(args)
    if args.action == "up":
        ingest.source_up(t, args, args.source_collection or "cbasload")
    elif args.action == "down":
        ingest.source_down(t)
    else:
        src = state.get_source(t)
        if not src:
            print("no source recorded")
            return
        s = ingest.Source.from_dict(src)
        print(json.dumps({k: v for k, v in src.items() if k != "password"}, indent=2))
        print(f"bucket item count: {s.item_count()}")


def cmd_report(args) -> None:
    p = Path(args.run_json)
    if p.is_dir():
        p = p / "run.json"
    run = json.loads(p.read_text())
    out = write_report(run, Path(args.out) if args.out else p.parent)
    ok(f"report: {out}")


def cmd_chaos_actions(_args) -> None:
    for k, v in ACTIONS.items():
        print(f"  {k:<24} {v}")


def load_presets() -> dict:
    return json.loads(PRESETS.read_text())


def cmd_preset(args, extra: list[str]) -> None:
    presets = load_presets()
    if args.action == "list" or not args.name:
        for name, p in presets.items():
            supp = f" [{', '.join(p['supp'])}]" if p.get("supp") else ""
            print(f"  {name:<22} {p['command']:<12} {p['description']}{supp}")
        return
    if args.name not in presets:
        die(f"unknown preset '{args.name}'. `./cbasload.py preset list`")
    p = presets[args.name]
    argv = [p["command"], *p["args"], "--label", f"preset:{args.name}", *extra]
    if args.action == "show":
        print(json.dumps(p, indent=2))
        print("\nequivalent:\n  ./cbasload.py " + " ".join(argv))
        return
    info("preset → ./cbasload.py " + " ".join(argv))
    if p.get("notes"):
        info(p["notes"])
    main(argv)


# --------------------------------------------------------------------------- #
# Arguments
# --------------------------------------------------------------------------- #

def _coll_args(p, default="cbasload") -> None:
    p.add_argument("--collection", "-c", default=default,
                   help=f"analytics collection name (default {default})")
    p.add_argument("--scope", default=None,
                   help="database.scope (EA, default Default.Default) or dataverse "
                        "(Server, default Default)")


def _query_args(p) -> None:
    g = p.add_argument_group("query workload")
    g.add_argument("--mix", default=None, help="JSON mix file (default: built-in for the template)")
    g.add_argument("--only", nargs="+", default=None, help="only these query names from the mix")
    g.add_argument("--weight", action="append", default=[], metavar="NAME=W",
                   help="override a query's weight (0 removes it)")
    g.add_argument("--concurrency", type=int, default=8, help="closed-loop workers (default 8)")
    g.add_argument("--qps", type=float, default=0.0,
                   help="open-loop target queries/s (overrides --concurrency)")
    g.add_argument("--max-inflight", type=int, default=128,
                   help="open-loop worker pool size (default 128)")
    g.add_argument("--arrival", choices=["poisson", "uniform"], default="poisson")
    g.add_argument("--query-timeout", default="120s")
    g.add_argument("--scan-consistency", choices=["not_bounded", "request_plus"], default=None)
    g.add_argument("--cancel-pct", type=float, default=0.0,
                   help="percent of queries to cancel mid-flight (SUPP-121/129 shape)")
    g.add_argument("--cancel-after", default="0.2-2s",
                   help="cancel delay, fixed or range (default 0.2-2s)")
    g.add_argument("--post-check", type=int, default=0, metavar="N",
                   help="after the load, run each query N times uncancelled and report failures")
    g.add_argument("--query-collection", default=None,
                   help="mixed: query this collection instead of the ingest one")


def _ingest_args(p) -> None:
    g = p.add_argument_group("ingest workload")
    g.add_argument("--engine", choices=["sdk", "pillowfight"], default="sdk")
    g.add_argument("--rate", type=float, default=2000,
                   help="target KV ops/s at profile level 1.0 (0 means unthrottled, default 2000)")
    g.add_argument("--writers", type=int, default=8, help="writer threads (default 8)")
    g.add_argument("--ops", default="upsert=70,insert=15,delete=10,expire=5",
                   help="op mix: upsert,insert,delete,expire,get weights")
    g.add_argument("--ttl", default="30s", help="expiry for 'expire' ops")
    g.add_argument("--key-skew", type=float, default=0.0,
                   help="Zipf exponent for which existing keys get updated (0 = uniform)")
    g.add_argument("--seed-docs", type=int, default=10_000,
                   help="docs to seed when the source collection is first created")
    g.add_argument("--hb-interval", default="1s", help="heartbeat write interval (lag resolution)")
    g.add_argument("--probe-interval", default="2s", help="lag probe interval")
    g.add_argument("--count-every", default="10s",
                   help="KV vs Analytics count comparison interval (0 disables)")
    g.add_argument("--drain", default="120s",
                   help="after writes stop, wait up to this long for Analytics to catch up "
                        "(0 disables)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cbasload.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    fmt = argparse.ArgumentDefaultsHelpFormatter

    g = sub.add_parser("gen", help="create a collection of synthetic docs", formatter_class=fmt)
    _coll_args(g)
    g.add_argument("--docs", "-n", type=int, default=100_000, help="number of documents")
    g.add_argument("--into", choices=["standalone", "external", "remote"], default="standalone",
                   help="standalone collection, external (S3) collection, or remote-link "
                        "collection fed from an auto-created Couchbase source")
    g.add_argument("--load-method", choices=["copy", "insert"], default=None,
                   help="standalone: COPY INTO from MinIO (fast, EA_repro) or UPSERT batches "
                        "over HTTP (any cluster). Defaults to copy on EA_repro, otherwise insert")
    g.add_argument("--load-threads", type=int, default=4)
    g.add_argument("--batch-bytes", default="2MB", help="insert: bytes per UPSERT statement")
    g.add_argument("--docs-per-file", type=int, default=10_000, help="S3: docs per NDJSON object")
    g.add_argument("--index", action="append", default=[], metavar="FIELD[:TYPE]",
                   help="secondary index, e.g. customerId:bigint, created_at:string (repeatable)")
    g.add_argument("--recreate", action="store_true", help="drop and rebuild if it exists")
    g.add_argument("--append", action="store_true", help="add docs after the existing keyspace")
    g.add_argument("--ingest-timeout", default="30m", help="remote: max wait for ingestion")
    docgen.add_doc_args(g)
    ingest.add_source_args(g)
    add_target_args(g)
    g.set_defaults(func=cmd_gen)

    q = sub.add_parser("query-load", help="run a SQL++ query workload", formatter_class=fmt)
    _coll_args(q)
    _query_args(q)
    add_run_args(q)
    q.add_argument("--seed", type=int, default=1)
    add_target_args(q)
    q.set_defaults(func=cmd_query_load)

    i = sub.add_parser("ingest-load", help="KV mutations into a remote-link source",
                       formatter_class=fmt)
    _coll_args(i, default="cbasingest")
    _ingest_args(i)
    add_run_args(i)
    docgen.add_doc_args(i)
    ingest.add_source_args(i)
    add_target_args(i)
    i.set_defaults(func=cmd_ingest_load)

    m = sub.add_parser("mixed", help="query and ingest workloads together", formatter_class=fmt)
    _coll_args(m, default="cbasingest")
    _query_args(m)
    _ingest_args(m)
    add_run_args(m)
    docgen.add_doc_args(m)
    ingest.add_source_args(m)
    add_target_args(m)
    m.set_defaults(func=cmd_mixed)

    pr = sub.add_parser("preset", help="named scenarios: list | show NAME | run NAME [overrides]")
    pr.add_argument("action", choices=["list", "show", "run"])
    pr.add_argument("name", nargs="?")

    st = sub.add_parser("status", help="collections created by cbasload and the source state")
    _coll_args(st)
    add_target_args(st)
    st.set_defaults(func=cmd_status)

    d = sub.add_parser("drop", help="drop a cbasload collection (and companions)")
    _coll_args(d)
    d.add_argument("--with-source", action="store_true", help="also remove the source container")
    add_target_args(d)
    d.set_defaults(func=cmd_drop)

    so = sub.add_parser("source", help="manage the ingestion source: up | down | status")
    so.add_argument("action", choices=["up", "down", "status"])
    ingest.add_source_args(so)
    add_target_args(so)
    so.set_defaults(func=cmd_source)

    pi = sub.add_parser("ping", help="show the resolved target and ping each analytics node")
    add_target_args(pi)
    pi.set_defaults(func=cmd_ping)

    r = sub.add_parser("report", help="re-render report.html from a run.json")
    r.add_argument("run_json")
    r.add_argument("--out", default=None)
    r.set_defaults(func=cmd_report)

    ca = sub.add_parser("chaos-actions", help="list --chaos actions")
    ca.set_defaults(func=cmd_chaos_actions)
    return p


def main(argv=None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if argv and argv[0] == "preset":
        args, extra = parser.parse_known_args(argv)
        return cmd_preset(args, extra)
    args = parser.parse_args(argv)
    if getattr(args, "chaos", None):
        parse_chaos(args.chaos)  # validate before any setup work
    try:
        args.func(args)
    except CbasError as e:
        die(str(e))
    except KeyboardInterrupt:
        die("interrupted", code=130)


if __name__ == "__main__":
    main()
