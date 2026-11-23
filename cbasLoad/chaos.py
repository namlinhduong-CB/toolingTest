"""
chaos.py - timed disruptions during a run, recorded as events on the timeline.
The node, rebalance and source actions use docker, so they need an EA_repro
target (and the source actions need --source container).

  --chaos 30s:restart-node:ea-node1     docker restart an EA node
  --chaos 30s:kill-node:ea-node2        docker kill (SIGKILL)
  --chaos 30s:stop-node / start-node    docker stop / start
  --chaos 60s:rebalance                 couchbase-cli rebalance
  --chaos 45s:disconnect-link[:name]    DISCONNECT LINK
  --chaos 75s:connect-link[:name]       CONNECT LINK
  --chaos 20s:pause-source              docker pause source (KV outage, SUPP-42)
  --chaos 50s:unpause-source
  --chaos 20s:kill-source               docker kill source, then start it again. The
  --chaos 30s:start-source              unpersisted mutations are lost, which leads
                                        to a DCP rollback (SUPP-83/86)
  --chaos 40s:restart-source
  --chaos 60s:recreate-source-bucket    drop and recreate the source bucket (SUPP-33)
  --chaos 30s:cancel-all                cancel every active analytics request
  --chaos 30s:pause-writes / resume-writes   stop/start the ingest writers only
"""

from __future__ import annotations

import threading
import time

import state
from recorder import Recorder, parse_duration
from target import Target, bq, fq_name

ACTIONS = {
    "restart-node": "docker restart an EA node (arg: ea-nodeN, default ea-node1)",
    "kill-node": "docker kill an EA node (arg: ea-nodeN)",
    "stop-node": "docker stop an EA node (arg: ea-nodeN)",
    "start-node": "docker start an EA node (arg: ea-nodeN)",
    "rebalance": "couchbase-cli rebalance on ea-node1",
    "disconnect-link": "DISCONNECT LINK (arg: link name, default the ingest link)",
    "connect-link": "CONNECT LINK (arg: link name)",
    "pause-source": "docker pause the source container (KV outage)",
    "unpause-source": "docker unpause the source container",
    "kill-source": "docker kill the source container (loses unpersisted mutations)",
    "start-source": "docker start the source container",
    "restart-source": "docker restart the source container",
    "recreate-source-bucket": "delete and recreate the source bucket/scope/collection",
    "cancel-all": "DELETE every active analytics request",
    "pause-writes": "pause the ingest writers (KV quiet, no outage)",
    "resume-writes": "resume the ingest writers",
}


def parse_chaos(specs: list[str]) -> list[tuple[float, str, str]]:
    out = []
    for s in specs:
        parts = s.split(":", 2)
        if len(parts) < 2:
            raise SystemExit(f"bad --chaos '{s}' (want AT:ACTION[:ARG])")
        at, action = parse_duration(parts[0]), parts[1]
        if action not in ACTIONS:
            raise SystemExit(f"unknown chaos action '{action}'. Known: {', '.join(ACTIONS)}")
        out.append((at, action, parts[2] if len(parts) > 2 else ""))
    return sorted(out)


class Chaos(threading.Thread):
    def __init__(self, t: Target, rec: Recorder, plan: list[tuple[float, str, str]], *,
                 link: str | None = None, scope: str = "", ingest=None, source=None,
                 source_args: dict | None = None):
        super().__init__(daemon=True, name="chaos")
        self.t, self.rec, self.plan = t, rec, plan
        self.link, self.scope = link, scope
        self.ingest, self.source = ingest, source
        self.source_args = source_args or {}
        self.stop_ev = threading.Event()

    def run(self) -> None:
        t0 = time.time()
        for at, action, arg in self.plan:
            if self.stop_ev.wait(max(0.0, at - (time.time() - t0))):
                return
            try:
                detail = self.do(action, arg)
                self.rec.event("chaos", f"{action}{':' + arg if arg else ''} → {detail}",
                               action=action)
            except Exception as e:  # noqa: BLE001
                self.rec.event("chaos", f"{action} FAILED: {e}", action=action, failed=True)

    def stop(self) -> None:
        self.stop_ev.set()

    # ---------------------------------------------------------------------
    def do(self, action: str, arg: str) -> str:
        t = self.t
        if action in ("restart-node", "kill-node", "stop-node", "start-node"):
            svc = arg or "ea-node1"
            verb = action.split("-")[0]
            state.docker([verb, t.container(svc)], timeout=180)
            return f"docker {verb} {t.container(svc)}"
        if action == "rebalance":
            r = state.docker(["exec", t.container("ea-node1"),
                              "/opt/enterprise-analytics/bin/couchbase-cli", "rebalance",
                              "--no-progress-bar", "-c", "localhost:8091",
                              "-u", t.username, "-p", t.password], check=False, timeout=1800)
            return ((r.stdout or "") + (r.stderr or "")).strip()[:200]
        if action in ("disconnect-link", "connect-link"):
            link = arg or self.link
            if not link:
                raise RuntimeError("no link name (pass one: connect-link:NAME)")
            verb = "DISCONNECT" if action.startswith("dis") else "CONNECT"
            s, _ = t.q_any([f"{verb} LINK {bq(link)};",
                            f"{verb} LINK {fq_name(self.scope, link)};"],
                           ok_if=("already connected", "not connected"))
            return s
        if action.endswith("-source") and action != "recreate-source-bucket":
            cname = self._source_container()
            verb = action.split("-")[0]
            state.docker([verb, cname], timeout=180)
            return f"docker {verb} {cname}"
        if action == "recreate-source-bucket":
            src = self.source
            if src is None:
                raise RuntimeError("no ingest source in this run")
            src.delete_bucket()
            time.sleep(3)
            src.ensure_keyspace(ram=self.source_args.get("ram", 512),
                                compression=self.source_args.get("compression", "passive"))
            return f"bucket {src.bucket} dropped and recreated"
        if action == "cancel-all":
            n = 0
            for u in t.query_urls:
                reqs = t.http.get_json(f"{u}/analytics/admin/active_requests", timeout=10)
                for r in reqs or []:
                    cid = r.get("clientContextID") or r.get("client_context_id")
                    if cid:
                        st, _ = t.http.request("DELETE", f"{u}/analytics/admin/active_requests",
                                               form={"client_context_id": cid}, timeout=10)
                        n += int(st == 200)
            return f"cancelled {n} requests"
        if action in ("pause-writes", "resume-writes"):
            if not self.ingest:
                raise RuntimeError("no ingest workload in this run")
            (self.ingest.paused.set if action == "pause-writes" else self.ingest.paused.clear)()
            return "writers " + ("paused" if action == "pause-writes" else "resumed")
        raise RuntimeError(f"unhandled {action}")

    def _source_container(self) -> str:
        if self.source and self.source.container:
            return self.source.container
        s = state.get_source(self.t)
        if s and s.get("container"):
            return s["container"]
        raise RuntimeError("no source container (source actions need --source container)")
