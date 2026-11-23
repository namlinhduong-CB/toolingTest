#!/usr/bin/env python3
"""
ea_repro_ui.py - a small local web UI for ea_repro.py.

Wraps the CLI in a web server that only uses the Python standard library, so a
support engineer can drive the repro from the browser. You set the sizing and
document options, click a button, watch the logs as they come in and run SQL++
queries. Every action runs the same `ea_repro.py` commands underneath.

    ./ea_repro_ui.py                 # serve on http://localhost:8500
    ./ea_repro_ui.py --port 9500

Then open the printed URL. This is a local control panel for your own machine's
Docker and is not meant to be exposed to a network.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FROZEN = getattr(sys, "frozen", False)
# Keep the state and compose files next to the executable (frozen) or the
# script (dev).
if FROZEN:
    HERE = Path(sys.executable).resolve().parent
else:
    HERE = Path(__file__).resolve().parent
STATE_FILE = HERE / ".ea_repro_state.json"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# CLI subcommands that ea_repro understands. When the UI shells out, it calls
# this same entrypoint again with one of these as the first argument (see main()).
CLI_COMMANDS = {"up", "down", "status", "query", "load", "info", "logs", "bundle"}


def cli_base_cmd() -> list[str]:
    """Command prefix for running the ea_repro CLI as a subprocess.

    When frozen, we run the bundled executable again, which hands off to the CLI.
    In dev, we run this script with the current interpreter.
    """
    if FROZEN:
        return [sys.executable]
    return [sys.executable, str(Path(__file__).resolve())]


# --------------------------------------------------------------------------- #
# Background job runner (one long-running command at a time)
# --------------------------------------------------------------------------- #

class Job:
    def __init__(self) -> None:
        # RLock, because _run appends the final line whilst already holding the lock.
        self.lock = threading.RLock()
        self.name: str | None = None
        self.argv: list[str] = []
        self.lines: list[str] = []
        self.running = False
        self.returncode: int | None = None
        self.started_at: float | None = None
        self._proc: subprocess.Popen | None = None

    def start(self, name: str, argv: list[str]) -> tuple[bool, str]:
        with self.lock:
            if self.running:
                return False, f"'{self.name}' is still running."
            self.name = name
            self.argv = argv
            self.lines = []
            self.running = True
            self.returncode = None
            self.started_at = time.time()
        threading.Thread(target=self._run, daemon=True).start()
        return True, "started"

    def _append(self, text: str) -> None:
        with self.lock:
            self.lines.append(ANSI_RE.sub("", text.rstrip("\n")))

    def _run(self) -> None:
        cmd = [*cli_base_cmd(), *self.argv]
        self._append(f"$ ea_repro {' '.join(self.argv)}")
        try:
            proc = subprocess.Popen(
                cmd, cwd=str(HERE),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            self._proc = proc
            assert proc.stdout is not None
            for line in proc.stdout:
                self._append(line)
            proc.wait()
            rc = proc.returncode
        except Exception as e:  # noqa: BLE001
            self._append(f"UI ERROR: {e}")
            rc = 1
        with self.lock:
            self.returncode = rc
            self.running = False
            self._append(f"— finished (exit {rc}) —")

    def snapshot(self, since: int) -> dict:
        with self.lock:
            return {
                "name": self.name,
                "running": self.running,
                "returncode": self.returncode,
                "total": len(self.lines),
                "lines": self.lines[since:],
                "elapsed": round(time.time() - self.started_at, 1)
                if self.started_at else None,
            }


JOB = Job()


# --------------------------------------------------------------------------- #
# argv builders
# --------------------------------------------------------------------------- #

def _num(params: dict, key: str) -> str | None:
    v = params.get(key)
    if v is None or v == "":
        return None
    return str(v)


def build_up_argv(p: dict) -> list[str]:
    argv = ["up", "--force"]
    for flag, key in [
        ("--nodes", "nodes"),
        ("--analytics-ramsize", "analytics_ramsize"),
        ("--partitions", "partitions"),
        ("--documents", "documents"),
        ("--docs-per-file", "docs_per_file"),
        ("--payload-bytes", "payload_bytes"),
        ("--ea-memory", "ea_memory"),
        ("--ea-image", "ea_image"),
        ("--project", "project"),
        ("--ea-ui-port", "ea_ui_port"),
        ("--ea-query-port", "ea_query_port"),
        ("--minio-api-port", "minio_api_port"),
        ("--minio-console-port", "minio_console_port"),
    ]:
        val = _num(p, key)
        if val is not None:
            argv += [flag, val]
    return argv


def build_load_argv(p: dict) -> list[str]:
    argv = ["load"]
    for flag, key in [
        ("--documents", "documents"),
        ("--docs-per-file", "docs_per_file"),
        ("--payload-bytes", "payload_bytes"),
    ]:
        val = _num(p, key)
        if val is not None:
            argv += [flag, val]
    if p.get("append"):
        argv.append("--append")
    return argv


def build_down_argv(p: dict) -> list[str]:
    argv = ["down"]
    if p.get("purge"):
        argv.append("--purge")
    return argv


# --------------------------------------------------------------------------- #
# Synchronous helpers (status / query / state)
# --------------------------------------------------------------------------- #

def run_sync(argv: list[str], timeout: float = 620.0) -> dict:
    try:
        r = subprocess.run(
            [*cli_base_cmd(), *argv], cwd=str(HERE),
            capture_output=True, text=True, timeout=timeout,
        )
        out = ANSI_RE.sub("", (r.stdout or "") + (r.stderr or ""))
        return {"ok": r.returncode == 0, "returncode": r.returncode, "output": out}
    except subprocess.TimeoutExpired:
        return {"ok": False, "returncode": -1, "output": "timed out"}


def list_ea_images() -> list[str]:
    """Return the couchbase/enterprise-analytics image tags pulled locally, latest first."""
    try:
        r = subprocess.run(
            ["docker", "images", "couchbase/enterprise-analytics",
             "--format", "{{.Repository}}:{{.Tag}}"],
            capture_output=True, text=True, timeout=15,
        )
        tags = [t for t in r.stdout.split() if t and "<none>" not in t]
        # remove duplicates, keep the order and put :latest first
        seen, out = set(), []
        for t in sorted(tags, key=lambda x: (":latest" not in x, x)):
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out
    except Exception:  # noqa: BLE001
        return []


def read_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    server_version = "ea_repro_ui"

    def log_message(self, *args) -> None:  # silence default logging
        pass

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj).encode(), "application/json")

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode() or "{}")
        except Exception:  # noqa: BLE001
            return {}

    # -- routing -----------------------------------------------------------
    def do_GET(self) -> None:
        if self.path == "/" or self.path.startswith("/index"):
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif self.path == "/api/log" or self.path.startswith("/api/log?"):
            since = 0
            if "?" in self.path:
                q = dict(kv.split("=", 1) for kv in self.path.split("?", 1)[1].split("&") if "=" in kv)
                since = int(q.get("since", "0") or 0)
            self._json(JOB.snapshot(since))
        elif self.path == "/api/state":
            self._json({"state": read_state(), "running": JOB.running,
                        "job": JOB.name})
        elif self.path == "/api/images":
            self._json({"images": list_ea_images()})
        elif self.path.startswith("/api/logs"):
            q = urllib.parse.parse_qs(self.path.split("?", 1)[1]) \
                if "?" in self.path else {}
            svc = (q.get("service", ["all"])[0]) or "all"
            tail = (q.get("tail", ["200"])[0]) or "200"
            self._json(run_sync(["logs", "--service", svc, "--tail", tail],
                                timeout=60))
        elif self.path.startswith("/download"):
            self._serve_download()
        else:
            self._json({"error": "not found"}, 404)

    def _serve_download(self) -> None:
        q = urllib.parse.parse_qs(self.path.split("?", 1)[1]) \
            if "?" in self.path else {}
        name = (q.get("name", [""])[0])
        # Only allow the plain file name of a bundle zip that sits next to the app.
        if not name or "/" in name or "\\" in name or not name.endswith(".zip"):
            return self._json({"error": "invalid file"}, 400)
        fp = (HERE / name).resolve()
        if fp.parent != HERE.resolve() or not fp.is_file():
            return self._json({"error": "not found"}, 404)
        data = fp.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        body = self._read_body()
        if self.path == "/api/run":
            command = body.get("command")
            params = body.get("params", {})
            builders = {"up": build_up_argv, "load": build_load_argv,
                        "down": build_down_argv}
            if command not in builders:
                return self._json({"ok": False, "error": "unknown command"}, 400)
            argv = builders[command](params)
            ok, msg = JOB.start(command, argv)
            self._json({"ok": ok, "message": msg, "argv": argv},
                       200 if ok else 409)
        elif self.path == "/api/status":
            self._json(run_sync(["status"], timeout=90))
        elif self.path == "/api/query":
            stmt = (body.get("statement") or "").strip()
            if not stmt:
                return self._json({"ok": False, "output": "empty statement"}, 400)
            self._json(run_sync(["query", stmt], timeout=610))
        elif self.path == "/api/bundle":
            res = run_sync(["bundle"], timeout=120)
            name = ""
            for line in res.get("output", "").splitlines():
                if line.startswith("BUNDLE_PATH="):
                    name = Path(line.split("=", 1)[1].strip()).name
            res["name"] = name
            self._json(res)
        else:
            self._json({"error": "not found"}, 404)


# --------------------------------------------------------------------------- #
# The page (one HTML file with the CSS and JS inline)
# --------------------------------------------------------------------------- #

PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EA Repro Control</title>
<style>
  :root{
    /* soft, low-contrast slate theme with a single muted accent */
    --bg:#1b1e24; --panel:#22262e; --panel2:#282d36; --border:#343a44;
    --text:#cdd2da; --muted:#868d9b; --accent:#b4666b; --accent2:#7f9cbb;
    --ok:#7fb08a; --warn:#c6a763; --err:#c07d78;
    --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
  header{display:flex;align-items:center;gap:12px;padding:14px 20px;
         border-bottom:1px solid var(--border);background:var(--panel)}
  header .dot{width:10px;height:10px;border-radius:50%;background:var(--muted)}
  header .dot.on{background:var(--ok)}
  header h1{font-size:16px;margin:0;font-weight:600;letter-spacing:.2px}
  header .sub{color:var(--muted);font-size:12px}
  header .spacer{flex:1}
  main{display:grid;grid-template-columns:360px 1fr;gap:16px;padding:16px;
       max-width:1400px;margin:0 auto}
  @media(max-width:900px){main{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;
        padding:16px;margin-bottom:16px}
  .card h2{font-size:13px;text-transform:uppercase;letter-spacing:.6px;
           color:var(--muted);margin:0 0 12px}
  label{display:block;font-size:12px;color:var(--muted);margin:10px 0 4px}
  input[type=number],input[type=text],textarea,select{
    width:100%;background:var(--panel2);border:1px solid var(--border);
    color:var(--text);border-radius:7px;padding:8px 10px;font-size:13px;font-family:inherit}
  textarea{font-family:var(--mono);resize:vertical}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  .chk{display:flex;align-items:center;gap:8px;margin-top:10px;color:var(--text);font-size:13px}
  .chk input{width:auto}
  .hint{font-size:11px;color:var(--muted);margin-top:4px}
  button{cursor:pointer;border:none;border-radius:7px;padding:9px 14px;font-size:13px;
         font-weight:600;font-family:inherit}
  button:disabled{opacity:.5;cursor:not-allowed}
  .btn-primary{background:var(--accent);color:#fff;width:100%}
  .btn-primary:hover:not(:disabled){filter:brightness(1.08)}
  .btn-2{background:var(--panel2);color:var(--text);border:1px solid var(--border)}
  .btn-2:hover:not(:disabled){border-color:var(--muted)}
  .btn-danger{background:transparent;color:var(--err);border:1px solid var(--err)}
  .btn-danger:hover:not(:disabled){background:rgba(192,125,120,.12)}
  .btnrow{display:flex;gap:8px;margin-top:12px}
  .btnrow button{flex:1}
  #log{background:#15181e;border:1px solid var(--border);border-radius:8px;
       font-family:var(--mono);font-size:12.5px;padding:12px;height:420px;
       overflow:auto;white-space:pre-wrap;word-break:break-word}
  #log .l-ok{color:var(--ok)} #log .l-warn{color:var(--warn)}
  #log .l-err{color:var(--err)} #log .l-step{color:var(--accent2);font-weight:700}
  #log .l-cmd{color:var(--muted)}
  .status-line{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);
       margin-bottom:8px}
  .badge{font-size:11px;padding:2px 8px;border-radius:20px;border:1px solid var(--border);color:var(--muted)}
  .badge.run{color:var(--warn);border-color:var(--warn)}
  .badge.ok{color:var(--ok);border-color:var(--ok)}
  .badge.err{color:var(--err);border-color:var(--err)}
  .endpoints a{color:var(--accent2);text-decoration:none}
  .endpoints a:hover{text-decoration:underline}
  .endpoints div{margin:4px 0;font-size:12.5px}
  .endpoints .k{color:var(--muted);display:inline-block;width:130px}
  .tabs{display:flex;gap:6px;margin-bottom:10px}
  .tab{padding:6px 12px;border-radius:7px;background:var(--panel2);border:1px solid var(--border);
       cursor:pointer;font-size:12px;color:var(--muted)}
  .tab.active{color:var(--text);border-color:var(--accent);background:rgba(180,102,107,.14)}
  code{font-family:var(--mono);background:var(--panel2);padding:1px 5px;border-radius:4px}
  .qout{background:#15181e;border:1px solid var(--border);border-radius:8px;
        font-family:var(--mono);font-size:12px;padding:10px;max-height:260px;overflow:auto;
        white-space:pre-wrap;margin-top:10px}
</style>
</head>
<body>
<header>
  <span class="dot" id="hdot"></span>
  <div>
    <h1>Enterprise Analytics · Repro Control</h1>
    <div class="sub">MinIO (S3) + Couchbase Enterprise Analytics — local Docker</div>
  </div>
  <div class="spacer"></div>
  <span class="badge" id="envbadge">no environment</span>
</header>

<main>
  <!-- LEFT: controls -->
  <div>
    <div class="card">
      <div class="tabs">
        <div class="tab active" data-tab="create" onclick="setTab('create')">Create</div>
        <div class="tab" data-tab="dataset" onclick="setTab('dataset')">Dataset</div>
      </div>

      <div id="tab-create">
        <h2>Cluster sizing</h2>
        <div class="row">
          <div><label>EA nodes</label><input type="number" id="nodes" value="1" min="1"></div>
          <div><label>Partitions</label><input type="number" id="partitions" value="2" min="1"></div>
        </div>
        <label>Analytics memory quota (MiB)</label>
        <input type="number" id="analytics_ramsize" value="2048" min="1024" step="256">
        <label>Per-container RAM limit (optional, e.g. 6g)</label>
        <input type="text" id="ea_memory" placeholder="unlimited">
        <div class="hint">EA's data service is metadata-only; analytics quota is the real lever.</div>

        <h2 style="margin-top:16px">Documents in S3</h2>
        <div class="row">
          <div><label>Total documents</label><input type="number" id="documents" value="10000" min="0"></div>
          <div><label>Docs / object</label><input type="number" id="docs_per_file" value="1000" min="1"></div>
        </div>
        <label>Padding bytes per doc (optional)</label>
        <input type="number" id="payload_bytes" value="0" min="0">

        <details style="margin-top:14px">
          <summary style="cursor:pointer;color:var(--muted);font-size:12px">Advanced: project &amp; host ports</summary>
          <div class="hint" style="margin-top:8px">Change these if you already run Couchbase/MinIO on the defaults.</div>
          <label>EA image / version</label>
          <input type="text" id="ea_image" list="imagelist" value="couchbase/enterprise-analytics:latest">
          <datalist id="imagelist"></datalist>
          <div class="hint">Pick a pulled tag or type any; an un-pulled tag is downloaded on create.</div>
          <label>Project name</label>
          <input type="text" id="project" value="ea-repro">
          <div class="row">
            <div><label>EA UI port</label><input type="number" id="ea_ui_port" value="8091"></div>
            <div><label>EA query port</label><input type="number" id="ea_query_port" value="8095"></div>
          </div>
          <div class="row">
            <div><label>MinIO S3 port</label><input type="number" id="minio_api_port" value="9000"></div>
            <div><label>MinIO console port</label><input type="number" id="minio_console_port" value="9001"></div>
          </div>
        </details>

        <button class="btn-primary" id="btn-up" style="margin-top:16px" onclick="runUp()">
          ▶ Create &amp; provision environment
        </button>
        <div class="hint">First run pulls the ~2.6&nbsp;GB EA image. Takes a few minutes.</div>
      </div>

      <div id="tab-dataset" style="display:none">
        <h2>Resize dataset (load)</h2>
        <div class="hint">Regenerate the S3 dataset without rebuilding the cluster.</div>
        <div class="row" style="margin-top:8px">
          <div><label>Total documents</label><input type="number" id="l_documents" value="50000" min="0"></div>
          <div><label>Docs / object</label><input type="number" id="l_docs_per_file" value="1000" min="1"></div>
        </div>
        <label>Padding bytes per doc</label>
        <input type="number" id="l_payload_bytes" value="0" min="0">
        <label class="chk"><input type="checkbox" id="l_append"> Append (don't wipe existing)</label>
        <button class="btn-2" style="width:100%;margin-top:14px" id="btn-load" onclick="runLoad()">
          ⟳ Regenerate dataset
        </button>
      </div>
    </div>

    <div class="card">
      <h2>Lifecycle</h2>
      <div class="btnrow">
        <button class="btn-2" id="btn-status" onclick="runStatus()">Status</button>
        <button class="btn-danger" id="btn-down" onclick="runDown()">Tear down</button>
      </div>
      <label class="chk"><input type="checkbox" id="purge"> Purge volumes &amp; state on tear down</label>
    </div>

    <div class="card endpoints" id="endpoints" style="display:none">
      <h2>Endpoints</h2>
      <div id="ep-body"></div>
    </div>
  </div>

  <!-- RIGHT: logs + query -->
  <div>
    <div class="card">
      <div class="status-line">
        <span id="job-name">idle</span>
        <span class="badge" id="job-badge">—</span>
        <span id="job-elapsed"></span>
        <span class="spacer" style="flex:1"></span>
        <button class="btn-2" style="padding:4px 10px" onclick="clearLog()">Clear</button>
      </div>
      <div id="log"></div>
    </div>

    <div class="card">
      <h2>Run SQL++</h2>
      <textarea id="stmt" rows="3">SELECT VALUE COUNT(*) FROM ext_data;</textarea>
      <div class="btnrow">
        <button class="btn-primary" id="btn-query" onclick="runQuery()">Run query</button>
      </div>
      <div class="qout" id="qout" style="display:none"></div>
    </div>

    <div class="card">
      <h2>Logs &amp; diagnostics</h2>
      <div class="btnrow" style="margin-bottom:0">
        <select id="log_service" style="flex:2">
          <option value="all">All containers</option>
          <option value="minio">MinIO</option>
        </select>
        <input type="number" id="log_tail" value="200" title="lines" style="flex:1">
        <button class="btn-2" id="btn-logs" onclick="fetchLogs()" style="flex:1">Fetch logs</button>
        <button class="btn-2" id="btn-bundle" onclick="makeBundle()" style="flex:2">Download support bundle</button>
      </div>
      <div class="qout" id="logout" style="display:none"></div>
    </div>
  </div>
</main>

<script>
let logSince = 0, polling = false, pollTimer = null;

function el(id){return document.getElementById(id)}
function setTab(t){
  document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.tab===t));
  el('tab-create').style.display = t==='create'?'block':'none';
  el('tab-dataset').style.display = t==='dataset'?'block':'none';
}
function setStmt(s){el('stmt').value=s}
function clearLog(){el('log').innerHTML=''; logSince=0;}

function lineClass(t){
  if(t.startsWith('✓')) return 'l-ok';
  if(t.startsWith('✗')||t.startsWith('UI ERROR')||/ERROR/.test(t)) return 'l-err';
  if(t.startsWith('!')) return 'l-warn';
  if(t.startsWith('■')) return 'l-step';
  if(t.startsWith('$')||t.startsWith('›')) return 'l-cmd';
  return '';
}
function appendLines(lines){
  const log = el('log');
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  for(const t of lines){
    const d=document.createElement('div');
    d.className=lineClass(t); d.textContent=t; log.appendChild(d);
  }
  if(atBottom) log.scrollTop = log.scrollHeight;
}

async function poll(){
  const r = await fetch('/api/log?since='+logSince);
  const s = await r.json();
  if(s.lines && s.lines.length){ appendLines(s.lines); logSince = s.total; }
  el('job-name').textContent = s.name ? ('command: '+s.name) : 'idle';
  const badge = el('job-badge');
  if(s.running){
    badge.textContent='running'; badge.className='badge run';
    el('job-elapsed').textContent = s.elapsed!=null ? s.elapsed+'s' : '';
  }else if(s.returncode===0){
    badge.textContent='done'; badge.className='badge ok';
  }else if(s.returncode!=null){
    badge.textContent='failed ('+s.returncode+')'; badge.className='badge err';
  }else{ badge.textContent='—'; badge.className='badge'; }

  if(!s.running){
    setBusy(false);
    if(polling){ polling=false; refreshState(); }
  }
}
function startPolling(){
  if(pollTimer) clearInterval(pollTimer);
  polling=true; setBusy(true);
  pollTimer=setInterval(poll, 1000); poll();
}
function setBusy(b){
  ['btn-up','btn-load','btn-down','btn-status'].forEach(id=>el(id).disabled=b);
}

function num(id){const v=el(id).value; return v===''?null:Number(v)}
function txt(id){const v=el(id).value.trim(); return v===''?null:v}

async function runCmd(command, params){
  clearLogIfDone();
  const r = await fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({command,params})});
  const j = await r.json();
  if(!j.ok){ appendLines(['! '+(j.message||j.error||'could not start')]); return; }
  startPolling();
}
function clearLogIfDone(){ /* keep history; no-op */ }

function runUp(){
  runCmd('up',{
    nodes:num('nodes'), partitions:num('partitions'),
    analytics_ramsize:num('analytics_ramsize'), ea_memory:txt('ea_memory'),
    documents:num('documents'), docs_per_file:num('docs_per_file'),
    payload_bytes:num('payload_bytes'), project:txt('project'),
    ea_image:txt('ea_image'),
    ea_ui_port:num('ea_ui_port'), ea_query_port:num('ea_query_port'),
    minio_api_port:num('minio_api_port'), minio_console_port:num('minio_console_port')
  });
}
function runLoad(){
  runCmd('load',{
    documents:num('l_documents'), docs_per_file:num('l_docs_per_file'),
    payload_bytes:num('l_payload_bytes'), append:el('l_append').checked
  });
}
function runDown(){
  if(el('purge').checked && !confirm('Purge will delete all containers, volumes and data. Continue?')) return;
  runCmd('down',{purge:el('purge').checked});
}
async function runStatus(){
  setBusy(true);
  const r=await fetch('/api/status',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  const j=await r.json(); appendLines((j.output||'').split('\n')); setBusy(false);
  refreshState();
}
async function runQuery(){
  const stmt=el('stmt').value.trim(); if(!stmt) return;
  const btn=el('btn-query'); btn.disabled=true; btn.textContent='…';
  const out=el('qout'); out.style.display='block'; out.textContent='running…';
  try{
    const r=await fetch('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({statement:stmt})});
    const j=await r.json();
    try{ out.textContent = JSON.stringify(JSON.parse(j.output), null, 2); }
    catch(e){ out.textContent = j.output; }
  }catch(e){ out.textContent='error: '+e; }
  btn.disabled=false; btn.textContent='Run query';
}

async function refreshState(){
  const r=await fetch('/api/state'); const j=await r.json();
  const st=j.state, ep=el('endpoints'), body=el('ep-body');
  const badge=el('envbadge'), dot=el('hdot');
  syncLogServices(st);
  if(st){
    badge.textContent = 'project: '+st.project+' · '+st.nodes+' node(s)';
    badge.className='badge ok'; dot.classList.add('on');
    const ui=st.ea_ui_host_base, q=st.ea_query_host_base,
          mc=st.minio_console_host, ma=st.minio_api_host;
    body.innerHTML =
      '<div class="btnrow" style="margin-bottom:12px">'+
        '<button class="btn-primary" onclick="window.open(\'http://localhost:'+mc+'\',\'_blank\')">Open MinIO Console</button>'+
        '<button class="btn-2" onclick="window.open(\'http://localhost:'+ui+'\',\'_blank\')">Open EA Console</button>'+
      '</div>'+
      row('EA Web Console','http://localhost:'+ui, st.ea_username+' / '+st.ea_password)+
      row('SQL++ query','http://localhost:'+q+'/analytics/service','')+
      row('MinIO Console','http://localhost:'+mc, st.minio_user+' / '+st.minio_password)+
      row('MinIO S3 API','http://localhost:'+ma,'')+
      '<div style="margin-top:8px"><span class="k">Dataset</span>'+
        Number(st.documents).toLocaleString()+' docs → <code>'+st.collection+'</code></div>'+
      '<div><span class="k">S3 buckets</span><code>'+st.storage_bucket+'</code> (EA), <code>'+st.data_bucket+'</code> (data)</div>';
    ep.style.display='block';
  }else{
    badge.textContent='no environment'; badge.className='badge'; dot.classList.remove('on');
    ep.style.display='none';
  }
}
async function loadImages(){
  try{
    const r=await fetch('/api/images'); const j=await r.json();
    const dl=el('imagelist'); dl.innerHTML='';
    (j.images||[]).forEach(t=>{const o=document.createElement('option');o.value=t;dl.appendChild(o);});
  }catch(e){}
}
function syncLogServices(st){
  const sel=el('log_service'); const cur=sel.value;
  let opts='<option value="all">All containers</option><option value="minio">MinIO</option>';
  const n=st?Number(st.nodes):1;
  for(let i=1;i<=n;i++) opts+='<option value="ea-node'+i+'">EA node '+i+'</option>';
  sel.innerHTML=opts; sel.value=cur||'all';
}
async function fetchLogs(){
  const out=el('logout'); out.style.display='block'; out.textContent='fetching…';
  const btn=el('btn-logs'); btn.disabled=true;
  try{
    const svc=el('log_service').value, tail=el('log_tail').value||'200';
    const r=await fetch('/api/logs?service='+encodeURIComponent(svc)+'&tail='+encodeURIComponent(tail));
    const j=await r.json(); out.textContent=(j.output||'(no output)').trim();
  }catch(e){ out.textContent='error: '+e; }
  btn.disabled=false;
}
async function makeBundle(){
  const out=el('logout'); out.style.display='block'; out.textContent='collecting support bundle…';
  const btn=el('btn-bundle'); btn.disabled=true;
  try{
    const r=await fetch('/api/bundle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    const j=await r.json();
    out.textContent=(j.output||'').trim();
    if(j.ok && j.name){
      out.textContent += '\n\n↓ downloading '+j.name;
      window.location='/download?name='+encodeURIComponent(j.name);
    }
  }catch(e){ out.textContent='error: '+e; }
  btn.disabled=false;
}
function row(k,url,cred){
  return '<div><span class="k">'+k+'</span>'+
    '<a href="'+url+'" target="_blank">'+url+'</a>'+
    (cred?' <span style="color:var(--muted)">('+cred+')</span>':'')+'</div>';
}

loadImages();
refreshState();
poll();
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main() -> None:
    # This entrypoint does two jobs. If the first argument is an ea_repro CLI
    # command, we act as the CLI, which is how the bundled executable does the
    # actual work. Otherwise we start the web UI. As such, one binary covers both.
    if len(sys.argv) > 1 and sys.argv[1] in CLI_COMMANDS:
        import ea_repro
        ea_repro.main(sys.argv[1:])
        return

    ap = argparse.ArgumentParser(description="Local web UI for the EA repro tool")
    ap.add_argument("--port", type=int, default=8500)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-open", action="store_true", help="do not open a browser automatically")
    args = ap.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"EA Repro Control  →  {url}")
    print("Press Ctrl+C to stop.")
    if not args.no_open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping.")
        httpd.shutdown()


if __name__ == "__main__":
    main()
