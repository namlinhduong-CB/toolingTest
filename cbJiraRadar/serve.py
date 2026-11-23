#!/usr/bin/env python3
"""
cbJiraRadar - local web app.

Run this and a browser opens to a live UI over Couchbase's public JIRA:

    python3 serve.py

All controls live in the page (look-back window, projects, customer-only,
Refresh). The browser can't call JIRA directly (the API sends no CORS
headers), so this tiny server fetches server-side and hands the page JSON.
It reuses the exact fetch + scoring engine in radar.py.

Nothing is written to disk and nothing leaves your machine except anonymous
read-only calls to issues.couchbase.com.
"""

import argparse
import json
import threading
import time
import traceback
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import radar
import forums

_PROJECT_CACHE = {"ts": 0, "data": None}
# Cache whole forum feeds so a Refresh (or several viewers) don't re-scrape the
# rate-limited anonymous API. Keyed by look-back window.
_FEED_CACHE = {}
_FEED_TTL = 180  # seconds
_FEED_LOCK = threading.Lock()


def get_projects():
    now = time.time()
    if _PROJECT_CACHE["data"] and now - _PROJECT_CACHE["ts"] < 600:
        return _PROJECT_CACHE["data"]
    data = radar.list_projects()
    _PROJECT_CACHE.update(ts=now, data=data)
    return data


def get_forum_feed(days, cap, force=False):
    """Fast list-only feed (no per-topic body reads), cached. Serialised so
    concurrent requests don't launch parallel scrapes of the throttled API.
    Enrichment happens in a second call (/api/forums_enrich)."""
    key = int(days)
    now = time.time()
    with _FEED_LOCK:
        hit = _FEED_CACHE.get(key)
        if hit and not force and now - hit["ts"] < _FEED_TTL:
            return hit["records"], dict(hit["meta"], cached=True)
        records, meta = forums.collect(days=days, cap=cap, do_enrich=False)
        _FEED_CACHE[key] = {"ts": now, "records": records, "meta": meta}
        return records, dict(meta, cached=False)


def enrich_forum(days, ids):
    """Enrich the given topic ids (OP body + poster status) using the cached
    feed's record objects, re-score them, and return the updated records."""
    key = int(days)
    hit = _FEED_CACHE.get(key)
    if not hit:
        return []
    by_id = {r["id"]: r for r in hit["records"]}
    targets = [by_id[i] for i in ids if i in by_id]
    forums.enrich_records(targets)  # mutates the cached objects in place
    return targets


class Handler(BaseHTTPRequestHandler):
    # Quieter logging.
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False)
        raw = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path in ("/", "/index.html", "/forums"):
                self._send(200, PAGE_FORUMS, "text/html; charset=utf-8")
            elif path == "/jira":
                self._send(200, PAGE, "text/html; charset=utf-8")
            elif path == "/api/forums":
                self._handle_forums(parse_qs(parsed.query))
            elif path == "/api/forums_enrich":
                self._handle_forums_enrich(parse_qs(parsed.query))
            elif path == "/api/projects":
                self._send(200, {"projects": get_projects(),
                                 "curated": radar.DEFAULT_PROJECTS})
            elif path == "/api/issues":
                self._handle_issues(parse_qs(parsed.query))
            else:
                self._send(404, {"error": "not found"})
        except BrokenPipeError:
            pass
        except Exception as e:  # surface errors to the UI instead of dying
            traceback.print_exc()
            try:
                self._send(500, {"error": str(e)})
            except Exception:
                pass

    def _handle_forums(self, q):
        def one(name, default=""):
            v = q.get(name, [default])
            return v[0] if v else default
        days = int(one("days", "45") or 45)
        cap = min(int(one("cap", "400") or 400), 800)
        min_score = float(one("minScore", "0") or 0)
        force = one("force", "0") in ("1", "true", "on")
        t0 = time.time()
        records, meta = get_forum_feed(days, cap, force=force)
        if min_score > 0:
            records = [r for r in records if r["signalScore"] >= min_score]
        meta = dict(meta, elapsed=round(time.time() - t0, 1),
                    shown=len(records), minScore=min_score)
        self._send(200, {"topics": records, "meta": meta})

    def _handle_forums_enrich(self, q):
        days = int((q.get("days", ["45"]) or ["45"])[0] or 45)
        ids_raw = (q.get("ids", [""]) or [""])[0]
        ids = [int(x) for x in ids_raw.split(",") if x.strip().isdigit()]
        with _FEED_LOCK:
            updated = enrich_forum(days, ids)
        self._send(200, {"topics": updated})

    def _handle_issues(self, q):
        def one(name, default=""):
            v = q.get(name, [default])
            return v[0] if v else default

        days = int(one("days", "7") or 7)
        cap = min(int(one("cap", "800") or 800), 2000)
        min_score = float(one("minScore", "0") or 0)
        customer_only = one("customerOnly", "0") in ("1", "true", "on")
        include_minor = one("includeMinor", "0") in ("1", "true", "on")
        extra = one("jql", "")
        projects_raw = one("projects", "curated").strip()

        if projects_raw in ("", "curated"):
            projects = list(radar.DEFAULT_PROJECTS)
        elif projects_raw == "all":
            projects = []
        else:
            projects = [p.strip() for p in projects_raw.split(",") if p.strip()]

        t0 = time.time()
        records, meta = radar.collect(
            days=days, projects=projects, customer_only=customer_only,
            include_minor=include_minor, min_score=min_score, cap=cap,
            extra=extra)
        meta["elapsed"] = round(time.time() - t0, 1)
        meta["cap"] = cap
        meta["capped"] = len(records) >= cap
        self._send(200, {"issues": records, "meta": meta})


def main():
    ap = argparse.ArgumentParser(description="Local web app for cbJiraRadar.")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-open", action="store_true", help="don't auto-open a browser")
    ap.add_argument("--insecure", action="store_true", help="skip TLS verification")
    args = ap.parse_args()
    if args.insecure:
        radar._INSECURE = True

    # If the chosen port is busy (often a leftover instance), advance to the
    # next free one rather than crashing with a raw traceback.
    srv = None
    port = args.port
    for port in range(args.port, args.port + 20):
        try:
            srv = ThreadingHTTPServer((args.host, port), Handler)
            break
        except OSError as e:
            if e.errno in (48, 98):  # EADDRINUSE (macOS 48 / Linux 98)
                print(f"port {port} in use, trying {port + 1} ...")
                continue
            raise
    if srv is None:
        print(f"no free port in {args.port}-{args.port + 19}. "
              f"Stop the other instance (`lsof -ti :{args.port} | xargs kill`) "
              f"or pass --port.")
        return

    url = f"http://{args.host}:{port}/"
    print(f"cbForumRadar running at {url}  (JIRA view: {url}jira)")
    print("Press Ctrl+C to stop.")
    if not args.no_open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping...")
        srv.shutdown()


PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>cbJiraRadar</title>
<style>
  :root{--bg:#0f1420;--panel:#161c2b;--edge:#26304a;--fg:#e6ebf5;--mut:#8a97b3;
        --acc:#e5322d;--cust:#ff8c42;--ok:#3ecf8e;--warn:#f5c451;--chip:#22304d;--btn:#2b5cff;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
  header{padding:12px 18px;border-bottom:1px solid var(--edge);display:flex;gap:14px;align-items:baseline;flex-wrap:wrap}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.3px}
  header h1 span{color:var(--acc)}
  header .meta{color:var(--mut);font-size:12px}
  .controls{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap;padding:11px 18px;border-bottom:1px solid var(--edge);background:var(--panel)}
  .controls .grp{display:flex;flex-direction:column;gap:3px}
  .controls label.cap{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
  .controls input,.controls select{background:#0c1220;color:var(--fg);border:1px solid var(--edge);border-radius:6px;padding:6px 8px;font-size:12px}
  .controls input[type=number]{width:66px}
  .controls .chk{flex-direction:row;align-items:center;gap:6px;font-size:12px;color:var(--fg)}
  .controls .chk input{width:auto}
  button.refresh{background:var(--btn);color:#fff;border:none;border-radius:6px;padding:8px 16px;font-size:13px;font-weight:600;cursor:pointer}
  button.refresh:disabled{opacity:.5;cursor:default}
  .status{color:var(--mut);font-size:12px;margin-left:auto;align-self:center;min-width:120px;text-align:right}
  .status.err{color:var(--acc)}
  .bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:8px 18px;border-bottom:1px solid var(--edge)}
  .bar input,.bar select{background:#0c1220;color:var(--fg);border:1px solid var(--edge);border-radius:6px;padding:5px 8px;font-size:12px}
  .bar input[type=search]{min-width:240px}
  .bar label{color:var(--mut);font-size:11px;display:flex;gap:5px;align-items:center}
  .count{margin-left:auto;color:var(--mut)}
  .wrap{padding:4px 12px 40px}
  table{width:100%;border-collapse:collapse}
  th,td{text-align:left;padding:7px 9px;border-bottom:1px solid #1c2437;vertical-align:top}
  th{position:sticky;top:0;background:var(--panel);color:var(--mut);font-weight:600;font-size:11px;cursor:pointer;user-select:none;white-space:nowrap}
  th.sorted::after{content:" \25BC";font-size:9px}
  th.asc::after{content:" \25B2";font-size:9px}
  tr:hover td{background:#141b2b}
  a.key{color:#7fb0ff;text-decoration:none;font-weight:600;white-space:nowrap}
  a.key:hover{text-decoration:underline}
  .sum{max-width:640px}
  .desc{color:var(--mut);font-size:11px;margin-top:3px;max-height:2.9em;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  .chip{display:inline-block;background:var(--chip);border:1px solid var(--edge);border-radius:10px;padding:1px 7px;font-size:10px;color:#bcd;margin:1px 3px 1px 0;white-space:nowrap}
  .svc{background:#1d2b46;border-color:#2c4067;color:#9fc0ff}
  .lab-cust{background:#42230f;border-color:#7a4218;color:#ffb27d}
  .pri-Blocker,.pri-Critical{color:var(--acc);font-weight:600}
  .pri-Major,.pri-High{color:var(--warn)}
  .score{font-weight:700;text-align:right;font-variant-numeric:tabular-nums}
  .s-hi{color:var(--cust)} .s-md{color:var(--warn)} .s-lo{color:var(--mut)}
  .stat{color:var(--mut);font-size:11px;white-space:nowrap}
  .dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:4px}
  .cat-new .dot{background:#7fb0ff}.cat-indeterminate .dot{background:var(--warn)}.cat-done .dot{background:var(--ok)}
  .empty{padding:56px 40px;text-align:center;color:var(--mut)}
  .spin{display:inline-block;width:12px;height:12px;border:2px solid var(--edge);border-top-color:var(--btn);border-radius:50%;animation:sp .7s linear infinite;vertical-align:-2px;margin-right:6px}
  .spin-lg{width:30px;height:30px;border-width:3px;display:block;margin:0 auto 14px}
  @keyframes sp{to{transform:rotate(360deg)}}
  .loadwrap{padding:60px 40px;text-align:center;color:var(--mut)}
  .loadwrap .sub{font-size:12px;margin-top:4px;color:#6d7a97}
</style></head>
<body>
<header>
  <h1>cb<span>Jira</span>Radar</h1>
  <span class="meta" id="meta">recent customer-facing issues on Couchbase public JIRA</span>
</header>

<div class="controls">
  <div class="grp"><label class="cap">Last N days</label>
    <input type="number" id="cDays" value="7" min="1" max="90"></div>
  <div class="grp"><label class="cap">Projects</label>
    <select id="cProjMode">
      <option value="curated">Curated (support-relevant)</option>
      <option value="all">All public projects</option>
      <option value="custom">Custom keys…</option>
    </select></div>
  <div class="grp" id="cCustomWrap" style="display:none"><label class="cap">Keys (comma-sep)</label>
    <input type="text" id="cProjKeys" placeholder="MB,CBL,K8S" style="width:200px"></div>
  <div class="grp"><label class="cap">Min signal</label>
    <input type="number" id="cMinScore" value="0" min="0" max="15" step="0.5"></div>
  <div class="grp"><label class="cap">Cap</label>
    <input type="number" id="cCap" value="800" min="50" max="2000" step="50"></div>
  <div class="grp chk"><input type="checkbox" id="cCustomerOnly"><label for="cCustomerOnly">customer-labelled only</label></div>
  <div class="grp chk"><input type="checkbox" id="cIncludeMinor"><label for="cIncludeMinor">include Minor</label></div>
  <button class="refresh" id="btnRefresh">Refresh</button>
  <span class="status" id="status"></span>
</div>

<div class="bar">
  <input type="search" id="q" placeholder="filter loaded results: summary / key / component / label ...">
  <label>Service <select id="fSvc"><option value="">all</option></select></label>
  <label>Priority <select id="fPri"><option value="">all</option></select></label>
  <label>Status <select id="fCat"><option value="">all</option><option>new</option><option>indeterminate</option><option>done</option></select></label>
  <label><input type="checkbox" id="fCust"> customer-signal only</label>
  <label>Open only <input type="checkbox" id="fOpen"></label>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
<table id="tbl">
  <thead><tr>
    <th data-k="customerScore" class="sorted">Signal</th>
    <th data-k="key">Key</th>
    <th data-k="service">Service</th>
    <th data-k="priority">Pri</th>
    <th data-k="summary" class="sum">Summary</th>
    <th data-k="status">Status</th>
    <th data-k="updated">Updated</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
<div class="empty" id="empty" style="display:none">No issues loaded yet.</div>
</div>

<script>
const $=s=>document.querySelector(s);
let issues=[], meta={}, sortK="customerScore", sortAsc=false;

// ---- fetch controls -> server -------------------------------------------
$('#cProjMode').addEventListener('change',e=>{
  $('#cCustomWrap').style.display = e.target.value==='custom' ? 'flex':'none';});

function projectsParam(){
  const mode=$('#cProjMode').value;
  if(mode==='all')return 'all';
  if(mode==='custom')return $('#cProjKeys').value.trim()||'curated';
  return 'curated';
}
async function refresh(){
  const btn=$('#btnRefresh'); btn.disabled=true;
  setStatus('<span class="spin"></span>fetching from JIRA…');
  const p=new URLSearchParams({
    days:$('#cDays').value||7, projects:projectsParam(),
    minScore:$('#cMinScore').value||0, cap:$('#cCap').value||800,
    customerOnly:$('#cCustomerOnly').checked?1:0,
    includeMinor:$('#cIncludeMinor').checked?1:0,
  });
  try{
    const r=await fetch('/api/issues?'+p.toString());
    const d=await r.json();
    if(d.error) throw new Error(d.error);
    issues=d.issues; meta=d.meta;
    onLoaded();
    setStatus(`${meta.total} issues · ${meta.elapsed}s`+(meta.capped?' · ⚠ capped':''));
  }catch(e){ setStatus('error: '+e.message, true); }
  finally{ btn.disabled=false; }
}
function setStatus(html,err){const el=$('#status');el.innerHTML=html;el.classList.toggle('err',!!err);}

function onLoaded(){
  $('#meta').textContent =
    `created/updated in last ${meta.days}d · ${meta.projects} projects · fetched ${meta.fetchedAt}`;
  // repopulate view dropdowns from the loaded set
  const svcs=[...new Set(issues.map(i=>i.service))].sort();
  const pris=[...new Set(issues.map(i=>i.priority).filter(Boolean))];
  const priOrder=["Blocker","Critical","Major","High","Minor","Low","Trivial"];
  pris.sort((a,b)=>priOrder.indexOf(a)-priOrder.indexOf(b));
  fill($('#fSvc'),svcs); fill($('#fPri'),pris);
  apply();
}
function fill(sel,vals){const cur=sel.value;sel.length=1;
  for(const v of vals){const o=document.createElement('option');o.textContent=v;sel.appendChild(o);}
  if([...sel.options].some(o=>o.value===cur))sel.value=cur;}

// ---- client-side view filtering -----------------------------------------
function fmtDate(s){if(!s)return"";const d=new Date(s);const h=(Date.now()-d)/3.6e6;
  if(h<24)return Math.max(0,Math.round(h))+"h ago";return Math.round(h/24)+"d ago";}
function esc(s){return String(s==null?"":s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function scoreCls(v){return v>=6?"s-hi":v>=3?"s-md":"s-lo";}

function apply(){
  const q=$('#q').value.toLowerCase().trim();
  const svc=$('#fSvc').value, pri=$('#fPri').value, cat=$('#fCat').value;
  const cust=$('#fCust').checked, openOnly=$('#fOpen').checked;
  let rows=issues.filter(i=>{
    if(svc&&i.service!==svc)return false;
    if(pri&&i.priority!==pri)return false;
    if(cat&&(i.statusCategory||"").toLowerCase()!==cat)return false;
    if(cust&&i.customerScore<3)return false;
    if(openOnly&&(i.statusCategory||"").toLowerCase()==="done")return false;
    if(q){const hay=(i.key+" "+i.summary+" "+i.description+" "+i.components.join(" ")+" "+i.labels.join(" ")).toLowerCase();
      if(!hay.includes(q))return false;}
    return true;
  });
  rows.sort((a,b)=>{let x=a[sortK],y=b[sortK];
    if(typeof x==="string"){x=x.toLowerCase();y=(y||"").toLowerCase();}
    if(x<y)return sortAsc?-1:1; if(x>y)return sortAsc?1:-1; return 0;});
  const tb=$('#rows'); tb.innerHTML="";
  for(const i of rows){
    const tr=document.createElement('tr');
    const cat2=(i.statusCategory||"").toLowerCase();
    const labs=i.labels.map(l=>`<span class="chip ${/customer|support|escalation|field/i.test(l)?'lab-cust':''}">${esc(l)}</span>`).join("");
    const comps=i.components.map(c=>`<span class="chip">${esc(c)}</span>`).join("");
    tr.innerHTML=`
      <td class="score ${scoreCls(i.customerScore)}">${i.customerScore}</td>
      <td><a class="key" href="${i.url}" target="_blank" rel="noopener">${i.key}</a></td>
      <td><span class="chip svc">${esc(i.service)}</span><div class="stat">${esc(i.project)}</div></td>
      <td class="pri-${esc(i.priority)}">${esc(i.priority||"-")}</td>
      <td class="sum"><div>${esc(i.summary)}</div>
          ${i.description?`<div class="desc">${esc(i.description)}</div>`:""}
          <div>${comps}${labs}</div></td>
      <td class="cat-${cat2}"><span class="dot"></span>${esc(i.status)}
          ${i.reporter?`<div class="stat">by ${esc(i.reporter)}</div>`:""}</td>
      <td class="stat">${fmtDate(i.updated)}<div class="stat">${i.votes?('⬆'+i.votes+' '):''}${i.watches?('\u{1f441}'+i.watches):''}</div></td>`;
    tb.appendChild(tr);
  }
  $('#count').textContent=`${rows.length} shown`;
  $('#empty').style.display=issues.length? (rows.length?"none":"block") : "block";
  $('#empty').textContent=issues.length?"No issues match the current filters.":"No issues loaded yet.";
}
document.querySelectorAll('th[data-k]').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k;
    if(sortK===k)sortAsc=!sortAsc;else{sortK=k;sortAsc=(k==="key"||k==="service"||k==="summary");}
    document.querySelectorAll('th').forEach(x=>x.classList.remove('sorted','asc'));
    th.classList.add('sorted'); if(sortAsc)th.classList.add('asc'); apply();};
});
['#q','#fSvc','#fPri','#fCat','#fCust','#fOpen'].forEach(s=>{
  const el=$(s); el.addEventListener(el.type==='search'||el.tagName==='SELECT'?'input':'change',apply);});
$('#btnRefresh').addEventListener('click',refresh);
[$('#cDays'),$('#cMinScore'),$('#cCap'),$('#cProjKeys')].forEach(el=>
  el.addEventListener('keydown',e=>{if(e.key==='Enter')refresh();}));

refresh();  // initial load
</script>
</body></html>"""


PAGE_FORUMS = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>cbForumRadar</title>
<style>
  :root{--bg:#0f1420;--panel:#161c2b;--edge:#26304a;--fg:#e6ebf5;--mut:#8a97b3;
        --acc:#e5322d;--cust:#ff8c42;--ok:#3ecf8e;--warn:#f5c451;--chip:#22304d;--btn:#2b5cff;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
  header{padding:12px 18px;border-bottom:1px solid var(--edge);display:flex;gap:14px;align-items:baseline;flex-wrap:wrap}
  header h1{font-size:16px;margin:0;font-weight:650;letter-spacing:.3px}
  header h1 span{color:var(--acc)}
  header .meta{color:var(--mut);font-size:12px}
  header a{color:#7fb0ff;text-decoration:none;font-size:12px}
  .controls{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap;padding:11px 18px;border-bottom:1px solid var(--edge);background:var(--panel)}
  .controls .grp{display:flex;flex-direction:column;gap:3px}
  .controls label.cap{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
  .controls input,.controls select{background:#0c1220;color:var(--fg);border:1px solid var(--edge);border-radius:6px;padding:6px 8px;font-size:12px}
  .controls input[type=number]{width:66px}
  button.refresh{background:var(--btn);color:#fff;border:none;border-radius:6px;padding:8px 16px;font-size:13px;font-weight:600;cursor:pointer}
  button.refresh:disabled{opacity:.5;cursor:default}
  .status{color:var(--mut);font-size:12px;margin-left:auto;align-self:center;min-width:130px;text-align:right}
  .status.err{color:var(--acc)}
  .bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:8px 18px;border-bottom:1px solid var(--edge)}
  .bar input,.bar select{background:#0c1220;color:var(--fg);border:1px solid var(--edge);border-radius:6px;padding:5px 8px;font-size:12px}
  .bar input[type=search]{min-width:240px}
  .bar label{color:var(--mut);font-size:11px;display:flex;gap:5px;align-items:center}
  .count{margin-left:auto;color:var(--mut)}
  .wrap{padding:4px 12px 40px}
  table{width:100%;border-collapse:collapse}
  th,td{text-align:left;padding:8px 9px;border-bottom:1px solid #1c2437;vertical-align:top}
  th{position:sticky;top:0;background:var(--panel);color:var(--mut);font-weight:600;font-size:11px;cursor:pointer;user-select:none;white-space:nowrap}
  th.sorted::after{content:" \25BC";font-size:9px}
  th.asc::after{content:" \25B2";font-size:9px}
  tr:hover td{background:#141b2b}
  a.title{color:#cfe0ff;text-decoration:none;font-weight:600}
  a.title:hover{color:#fff;text-decoration:underline}
  .excerpt{color:var(--mut);font-size:11px;margin-top:3px;max-height:2.9em;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  .chip{display:inline-block;background:var(--chip);border:1px solid var(--edge);border-radius:10px;padding:1px 7px;font-size:10px;color:#bcd;margin:2px 3px 0 0;white-space:nowrap}
  .svc{background:#1d2b46;border-color:#2c4067;color:#9fc0ff}
  .tag{background:#1a2233}
  .score{font-weight:700;text-align:right;font-variant-numeric:tabular-nums}
  .s-hi{color:var(--cust)} .s-md{color:var(--warn)} .s-lo{color:var(--mut)}
  .num{text-align:right;font-variant-numeric:tabular-nums;color:#c7d2e6}
  .stat{color:var(--mut);font-size:11px;white-space:nowrap}
  .ans-y{color:var(--ok);font-weight:600}.ans-n{color:var(--mut)}
  .badge{font-size:10px;padding:1px 6px;border-radius:9px;white-space:nowrap}
  .b-cust{background:#42230f;border:1px solid #7a4218;color:#ffb27d}
  .b-staff{background:#12314a;border:1px solid #1d4f77;color:#86c7ff}
  .b-noreply{background:#3a1418;border:1px solid #6e2630;color:#ff9aa6}
  .empty{padding:56px 40px;text-align:center;color:var(--mut)}
  .spin{display:inline-block;width:12px;height:12px;border:2px solid var(--edge);border-top-color:var(--btn);border-radius:50%;animation:sp .7s linear infinite;vertical-align:-2px;margin-right:6px}
  .spin-lg{width:30px;height:30px;border-width:3px;display:block;margin:0 auto 14px}
  @keyframes sp{to{transform:rotate(360deg)}}
  .loadwrap{padding:60px 40px;text-align:center;color:var(--mut)}
  .loadwrap .sub{font-size:12px;margin-top:4px;color:#6d7a97}
</style></head>
<body>
<header>
  <h1>cb<span>Forum</span>Radar</h1>
  <span class="meta" id="meta">recent customer questions on the Couchbase community forums</span>
  <a href="/jira" title="switch to the JIRA source">JIRA view →</a>
</header>

<div class="controls">
  <div class="grp"><label class="cap">Last N days</label>
    <input type="number" id="cDays" value="45" min="1" max="365"></div>
  <div class="grp"><label class="cap">Min signal</label>
    <input type="number" id="cMinScore" value="0" min="0" max="15" step="0.5"></div>
  <div class="grp"><label class="cap">Cap</label>
    <input type="number" id="cCap" value="400" min="30" max="800" step="10"></div>
  <button class="refresh" id="btnRefresh">Refresh</button>
  <span class="status" id="status"></span>
</div>

<div class="bar">
  <input type="search" id="q" placeholder="filter: title / body / category / tag / author ...">
  <label>Service <select id="fSvc"><option value="">all</option></select></label>
  <label>Category <select id="fCat"><option value="">all</option></select></label>
  <label><input type="checkbox" id="fUnans"> unanswered only</label>
  <label><input type="checkbox" id="fCust"> customers only</label>
  <label><input type="checkbox" id="fNoReply"> no replies</label>
  <span class="count" id="count"></span>
</div>

<div class="wrap">
<table id="tbl">
  <thead><tr>
    <th data-k="signalScore" class="sorted">Signal</th>
    <th data-k="title">Topic</th>
    <th data-k="service">Service</th>
    <th data-k="replies" class="num">Repl</th>
    <th data-k="views" class="num">Views</th>
    <th data-k="answered">Ans</th>
    <th data-k="created">Created</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
<div class="empty" id="empty"><span class="spin spin-lg"></span>Loading recent forum topics…</div>
</div>

<script>
const $=s=>document.querySelector(s);
let topics=[], meta={}, sortK="signalScore", sortAsc=false, loading=true;

async function refresh(force){
  const btn=$('#btnRefresh'); btn.disabled=true;
  loading=true; apply();                                // show content-area spinner
  setStatus('<span class="spin"></span>loading topics…');
  const p=new URLSearchParams({days:$('#cDays').value||45, minScore:$('#cMinScore').value||0,
    cap:$('#cCap').value||400, force:force?1:0});
  try{
    const r=await fetch('/api/forums?'+p.toString());
    const d=await r.json();
    if(d.error) throw new Error(d.error);
    topics=d.topics; meta=d.meta; loading=false; onLoaded();  // instant list render
    await enrichAll();                                        // fill bodies + badges
  }catch(e){ loading=false; apply(); setStatus('error: '+e.message,true); }
  finally{ btn.disabled=false; }
}

// Second phase: read each topic's original post (body + customer/staff) in
// small chunks, patching rows as they arrive so nothing blocks on a full scrape.
async function enrichAll(){
  const ids=topics.filter(t=>!t.enriched).map(t=>t.id);
  const N=Math.min(ids.length, 120), chunk=12; let done=0;
  if(!N){ setStatus(`${meta.total} topics · ${meta.days}d`); return; }
  for(let i=0;i<N;i+=chunk){
    const slice=ids.slice(i,i+chunk);
    setStatus(`<span class="spin"></span>${meta.total} topics · reading ${done}/${N} bodies`);
    try{
      const r=await fetch(`/api/forums_enrich?days=${meta.days}&ids=${slice.join(',')}`);
      const d=await r.json();
      for(const u of (d.topics||[])){const idx=topics.findIndex(t=>t.id===u.id); if(idx>=0)topics[idx]=u;}
    }catch(e){/* keep going; partial enrichment is fine */}
    done+=slice.length; apply();
  }
  setStatus(`${meta.total} topics · ${meta.days}d · ${done} bodies read`);
}
function setStatus(html,err){const el=$('#status');el.innerHTML=html;el.classList.toggle('err',!!err);}

function onLoaded(){
  $('#meta').textContent=`created in last ${meta.days}d · ${meta.total} topics · fetched ${meta.fetchedAt}`+(meta.cached?' · cached':'');
  fill($('#fSvc'),[...new Set(topics.map(t=>t.service))].sort());
  fill($('#fCat'),[...new Set(topics.map(t=>t.category).filter(Boolean))].sort());
  apply();
}
function fill(sel,vals){const cur=sel.value;sel.length=1;
  for(const v of vals){const o=document.createElement('option');o.textContent=v;sel.appendChild(o);}
  if([...sel.options].some(o=>o.value===cur))sel.value=cur;}

function fmtDate(s){if(!s)return"";const d=new Date(s);const h=(Date.now()-d)/3.6e6;
  if(h<24)return Math.max(0,Math.round(h))+"h ago";const dd=Math.round(h/24);return dd+"d ago";}
function esc(s){return String(s==null?"":s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function scoreCls(v){return v>=6?"s-hi":v>=3?"s-md":"s-lo";}

function apply(){
  const q=$('#q').value.toLowerCase().trim();
  const svc=$('#fSvc').value, cat=$('#fCat').value;
  const unans=$('#fUnans').checked, cust=$('#fCust').checked, noRep=$('#fNoReply').checked;
  let rows=topics.filter(t=>{
    if(svc&&t.service!==svc)return false;
    if(cat&&t.category!==cat)return false;
    if(unans&&t.answered)return false;
    if(noRep&&t.replies>0)return false;
    if(cust&&(t.opStaff||!t.opAuthor))return false;
    if(q){const hay=(t.key+" "+t.title+" "+t.excerpt+" "+t.category+" "+(t.tags||[]).join(" ")+" "+(t.opAuthor||"")).toLowerCase();
      if(!hay.includes(q))return false;}
    return true;
  });
  rows.sort((a,b)=>{let x=a[sortK],y=b[sortK];
    if(typeof x==="string"){x=x.toLowerCase();y=(y||"").toLowerCase();}
    if(x<y)return sortAsc?-1:1; if(x>y)return sortAsc?1:-1; return 0;});
  const tb=$('#rows'); tb.innerHTML="";
  for(const t of rows){
    const tr=document.createElement('tr');
    const tags=(t.tags||[]).map(x=>`<span class="chip tag">${esc(x)}</span>`).join("");
    let badges="";
    if(t.opAuthor && !t.opStaff) badges+=`<span class="badge b-cust">customer</span> `;
    else if(t.opStaff) badges+=`<span class="badge b-staff">staff</span> `;
    if(!t.answered && t.replies===0) badges+=`<span class="badge b-noreply">no reply</span> `;
    if(t.closed) badges+=`<span class="chip">closed</span>`;
    tr.innerHTML=`
      <td class="score ${scoreCls(t.signalScore)}">${t.signalScore}</td>
      <td><a class="title" href="${t.url}" target="_blank" rel="noopener">${esc(t.title)}</a>
          ${t.excerpt?`<div class="excerpt">${esc(t.excerpt)}</div>`:""}
          <div><span class="chip">${esc(t.category)}</span>${tags} ${badges}</div></td>
      <td><span class="chip svc">${esc(t.service)}</span></td>
      <td class="num">${t.replies}</td>
      <td class="num">${t.views}</td>
      <td class="${t.answered?'ans-y':'ans-n'}">${t.answered?'✓':'—'}</td>
      <td class="stat">${fmtDate(t.created)}${t.opAuthor?`<div class="stat">by ${esc(t.opAuthor)}</div>`:""}</td>`;
    tb.appendChild(tr);
  }
  $('#count').textContent=loading?'':`${rows.length} shown`;
  const emp=$('#empty');
  if(loading && !topics.length){
    emp.style.display="block";
    emp.innerHTML='<span class="spin spin-lg"></span>Loading recent forum topics…<div class="sub">reading the forums and each post — first load takes a few seconds</div>';
  }else if(!rows.length){
    emp.style.display="block";
    emp.innerHTML=topics.length?"No topics match the current filters.":"No topics loaded yet.";
  }else{
    emp.style.display="none";
  }
}
document.querySelectorAll('th[data-k]').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k;
    if(sortK===k)sortAsc=!sortAsc;else{sortK=k;sortAsc=(k==="title"||k==="service");}
    document.querySelectorAll('th').forEach(x=>x.classList.remove('sorted','asc'));
    th.classList.add('sorted'); if(sortAsc)th.classList.add('asc'); apply();};
});
['#q','#fSvc','#fCat','#fUnans','#fCust','#fNoReply'].forEach(s=>{
  const el=$(s); el.addEventListener(el.type==='search'||el.tagName==='SELECT'?'input':'change',apply);});
$('#btnRefresh').addEventListener('click',()=>refresh(true));
[$('#cDays'),$('#cMinScore'),$('#cCap')].forEach(el=>
  el.addEventListener('keydown',e=>{if(e.key==='Enter')refresh(true);}));
refresh(false);
</script>
</body></html>"""


if __name__ == "__main__":
    main()
