"""state.py - a small JSON state file that remembers what `gen` / `source up` built,
so query-load and ingest-load know the template, keyspace and source details."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from target import Target, info

HERE = Path(__file__).resolve().parent
STATE = HERE / ".cbasload_state.json"


def _load() -> dict:
    if STATE.exists():
        try:
            return json.loads(STATE.read_text())
        except json.JSONDecodeError:
            pass
    return {}


def _save(d: dict) -> None:
    STATE.write_text(json.dumps(d, indent=2))


def target_id(t: Target) -> str:
    return f"ea_repro:{t.project}" if t.from_ea_repro else f"host:{t.mgmt_url}"


def get_collection(t: Target, fq: str) -> dict | None:
    return _load().get(target_id(t), {}).get("collections", {}).get(fq)


def put_collection(t: Target, fq: str, meta: dict) -> None:
    d = _load()
    d.setdefault(target_id(t), {}).setdefault("collections", {})[fq] = meta
    _save(d)


def drop_collection(t: Target, fq: str) -> None:
    d = _load()
    d.get(target_id(t), {}).get("collections", {}).pop(fq, None)
    _save(d)


def get_source(t: Target) -> dict | None:
    return _load().get(target_id(t), {}).get("source")


def put_source(t: Target, src: dict | None) -> None:
    d = _load()
    tid = d.setdefault(target_id(t), {})
    if src is None:
        tid.pop("source", None)
    else:
        tid["source"] = src
    _save(d)


def all_for(t: Target) -> dict:
    return _load().get(target_id(t), {})


# --------------------------------------------------------------------------- #
# docker helpers (shared by ingest.py and chaos.py)
# --------------------------------------------------------------------------- #

def docker(argv: list[str], *, check: bool = True, quiet: bool = True,
           capture: bool = True, timeout: float | None = None) -> subprocess.CompletedProcess:
    if not quiet:
        info("$ docker " + " ".join(argv))
    r = subprocess.run(["docker"] + argv, text=True, timeout=timeout,
                       stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.PIPE if capture else None)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(argv[:3])}… failed: "
                           f"{(r.stderr or r.stdout or '').strip()[:500]}")
    return r


def container_running(name: str) -> bool:
    r = docker(["inspect", "-f", "{{.State.Running}} {{.State.Paused}}", name], check=False)
    return r.returncode == 0 and r.stdout.strip().startswith("true")
