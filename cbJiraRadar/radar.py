#!/usr/bin/env python3
"""
cbJiraRadar - a recent-issues radar over Couchbase's public JIRA.

Scrapes issues.couchbase.com (Atlassian Cloud, anonymous read) for tickets
created or updated in a recent window, scores how "customer-raised" each looks,
and emits a structured JSON feed plus a self-contained HTML digest.

Stdlib only (urllib) - no API key, no third-party deps. Portable onto any node.

  python3 radar.py --days 7
  python3 radar.py --days 14 --projects MB,CBL,K8S --customer-only
  python3 radar.py --days 7 --out both --json-out radar.json --html-out radar.html

The AI triage layer is a separate, optional pass (see ai_enrich.py) that reads
the JSON this tool emits; the fetch pipeline never requires it.
"""

import argparse
import datetime as dt
import html
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://jira.issues.couchbase.com"
SEARCH = BASE + "/rest/api/3/search/jql"
UA = "cbJiraRadar/1.0 (Couchbase Support tooling; anonymous read)"

# Fields we ask JIRA for on every issue.
FIELDS = [
    "summary", "created", "updated", "status", "priority", "issuetype",
    "reporter", "labels", "components", "votes", "watches", "resolution",
    "description", "project", "fixVersions",
]

# Curated default project set: the public projects a support engineer most
# often needs to watch. Empty --projects means "all accessible projects".
DEFAULT_PROJECTS = [
    "MB",      # Couchbase Server (KV, Query, Index, ns_server, ...)
    "CBL", "CM",  # Mobile / Lite
    "K8S",     # Kubernetes / Autonomous Operator
    "CBEVT",   # Eventing
    # SDKs
    "JCBC", "NCBC", "PYCBC", "GOCBC", "JSCBC", "PCBC", "CCBC", "CXXCBC",
    "SCBC", "RCBC", "KCBC", "JVMCBC",
    # Connectors people file against
    "KAFKAC", "CBES", "SPARKC",
]

# project key -> coarse service area (for grouping/filtering in the digest)
SERVICE = {
    "MB": "Server", "CBL": "Mobile", "CM": "Mobile", "CBG": "Mobile",
    "K8S": "Kubernetes", "CBEVT": "Eventing",
    "JCBC": "SDK", "NCBC": "SDK", "PYCBC": "SDK", "GOCBC": "SDK",
    "JSCBC": "SDK", "PCBC": "SDK", "CCBC": "SDK", "CXXCBC": "SDK",
    "SCBC": "SDK", "RCBC": "SDK", "KCBC": "SDK", "JVMCBC": "SDK",
    "PLCBC": "SDK", "JDCP": "SDK",
    "GOCO": "Analytics-SDK", "JCO": "Analytics-SDK", "JSCO": "Analytics-SDK",
    "NCO": "Analytics-SDK", "PYCO": "Analytics-SDK",
    "KAFKAC": "Connector", "CBES": "Connector", "SPARKC": "Connector",
    "CBHADOOP": "Connector", "TACO": "Connector", "TALENDC": "Connector",
    "POWERBI": "Connector", "SMT": "Connector", "DATADOG": "Connector",
    "CBASP": "Integration", "CBEF": "Integration", "LINQ": "Integration",
    "QUARK": "Integration", "SBEE": "Integration", "NGXCBM": "Integration",
    "TXNCXX": "Transactions", "TXNN": "Transactions",
    "CMOS": "Observability", "PE": "Observability",
    "DOC": "Docs", "SUPERSET": "Analytics", "SUPP": "Support",
}

# Labels that strongly suggest a customer actually hit this.
CUSTOMER_LABELS = {
    "customer", "customer-issue", "customer-found", "customer-reported",
    "customer-request", "support", "supportability", "escalation",
    "field-issue",
}

PRIORITY_WEIGHT = {
    "Blocker": 4, "Critical": 4, "Major": 3, "High": 3,
    "Minor": 1, "Trivial": 0, "Low": 1, "Test Blocker": 3,
}


# macOS' Python.framework doesn't trust the system keychain, so a plain
# verified context can fail with CERTIFICATE_VERIFY_FAILED even though curl
# works. Build a context that tries the default trust store, then common CA
# bundle locations, so the tool stays portable without third-party deps.
_CA_PATHS = [
    os.environ.get("SSL_CERT_FILE", ""),
    "/etc/ssl/cert.pem",              # macOS / LibreSSL
    "/etc/pki/tls/certs/ca-bundle.crt",  # RHEL / CentOS
    "/etc/ssl/certs/ca-certificates.crt",  # Debian / Ubuntu
]
_INSECURE = False


def _ssl_context():
    if _INSECURE:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    try:
        ctx = ssl.create_default_context()
        if ctx.get_ca_certs():
            return ctx
    except Exception:
        pass
    for path in _CA_PATHS:
        if path and os.path.exists(path):
            try:
                return ssl.create_default_context(cafile=path)
            except Exception:
                continue
    return ssl.create_default_context()


_CTX = None


def http_get_json(url, tries=3):
    global _CTX
    if _CTX is None:
        _CTX = _ssl_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": UA})
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30, context=_CTX) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 4xx won't get better on retry; surface the body for debugging.
            body = e.read().decode("utf-8", "replace")[:300] if hasattr(e, "read") else ""
            raise urllib.error.HTTPError(e.url, e.code, f"{e.reason} :: {body}", e.headers, None)
        except (urllib.error.URLError, TimeoutError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise last


def flatten_adf(node, out):
    """Recursively pull plain text out of an Atlassian Document Format tree."""
    if node is None:
        return
    if isinstance(node, list):
        for n in node:
            flatten_adf(n, out)
        return
    if not isinstance(node, dict):
        return
    t = node.get("type")
    if t == "text":
        out.append(node.get("text", ""))
    elif t == "hardBreak":
        out.append("\n")
    if node.get("content"):
        flatten_adf(node["content"], out)
    # Put a break after block-level containers so text stays readable.
    if t in ("paragraph", "heading", "codeBlock", "listItem", "blockquote", "rule"):
        out.append("\n")


def adf_to_text(desc, limit=4000):
    if not desc:
        return ""
    if isinstance(desc, str):
        text = desc
    else:
        parts = []
        flatten_adf(desc, parts)
        text = "".join(parts)
    # Collapse runaway whitespace.
    lines = [ln.rstrip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln.strip() != "")
    return text[:limit]


# Priorities we treat as noise and drop by default.
MINOR_PRIORITIES = ["Minor", "Trivial", "Low"]


def build_jql(days, projects, customer_only, extra, drop_minor=True):
    clauses = []
    window = f"(created >= -{days}d OR updated >= -{days}d)"
    clauses.append(window)
    if projects:
        keys = ", ".join(projects)
        clauses.append(f"project in ({keys})")
    if customer_only:
        labels = ", ".join(sorted(CUSTOMER_LABELS))
        clauses.append(f"labels in ({labels})")
    if drop_minor:
        # Keep unprioritised issues (priority may be EMPTY); only exclude the
        # explicitly-minor ones.
        mins = ", ".join(MINOR_PRIORITIES)
        clauses.append(f"(priority not in ({mins}) OR priority is EMPTY)")
    if extra:
        clauses.append(f"({extra})")
    return " AND ".join(clauses) + " ORDER BY updated DESC"


def fetch(jql, cap, page_size=100, verbose=False):
    """Page through /search/jql until isLast or we hit cap issues."""
    issues = []
    token = None
    while len(issues) < cap:
        params = {
            "jql": jql,
            "maxResults": min(page_size, cap - len(issues)),
            "fields": ",".join(FIELDS),
        }
        if token:
            params["nextPageToken"] = token
        url = SEARCH + "?" + urllib.parse.urlencode(params)
        data = http_get_json(url)
        batch = data.get("issues", [])
        issues.extend(batch)
        if verbose:
            print(f"  fetched {len(issues)} ...", file=sys.stderr)
        if data.get("isLast") or not data.get("nextPageToken") or not batch:
            break
        token = data["nextPageToken"]
        time.sleep(0.3)  # be polite to the anonymous endpoint
    return issues


def get(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur or cur[p] is None:
            return default
        cur = cur[p]
    return cur


def parse_dt(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def normalize(issue):
    f = issue.get("fields", {})
    labels = f.get("labels") or []
    priority = get(f, "priority", "name", default="")
    votes = get(f, "votes", "votes", default=0) or 0
    watches = get(f, "watches", "watchCount", default=0) or 0
    proj = get(f, "project", "key", default="?")
    rec = {
        "key": issue.get("key"),
        "url": f"{BASE}/browse/{issue.get('key')}",
        "project": proj,
        "service": SERVICE.get(proj, "Other"),
        "summary": f.get("summary") or "",
        "type": get(f, "issuetype", "name", default=""),
        "status": get(f, "status", "name", default=""),
        "statusCategory": get(f, "status", "statusCategory", "name", default=""),
        "priority": priority,
        "resolution": get(f, "resolution", "name", default=None),
        "reporter": get(f, "reporter", "displayName", default=None),
        "labels": labels,
        "components": [c.get("name") for c in (f.get("components") or [])],
        "fixVersions": [v.get("name") for v in (f.get("fixVersions") or [])],
        "votes": votes,
        "watches": watches,
        "created": f.get("created"),
        "updated": f.get("updated"),
        "description": adf_to_text(f.get("description")),
    }
    rec["customerScore"], rec["customerSignals"] = score(rec)
    return rec


def score(rec):
    """Heuristic 0-10ish score for 'a customer actually raised/hit this'."""
    s = 0.0
    signals = []
    lab = {l.lower() for l in rec["labels"]}
    hit = lab & CUSTOMER_LABELS
    if hit:
        s += 5
        signals.append("label:" + ",".join(sorted(hit)))
    s += PRIORITY_WEIGHT.get(rec["priority"], 0)
    if rec["priority"] in ("Blocker", "Critical", "Major", "High"):
        signals.append("prio:" + rec["priority"])
    if rec["votes"]:
        s += min(rec["votes"], 3)
        signals.append(f"votes:{rec['votes']}")
    if rec["watches"] and rec["watches"] > 1:
        s += min(rec["watches"] / 2.0, 2)
        signals.append(f"watches:{rec['watches']}")
    if rec["type"] in ("Bug", "Improvement", "Task") and rec["priority"] in ("Blocker", "Critical"):
        signals.append("critical-bug")
    return round(s, 1), signals


# ----------------------------------------------------------------------------- shared engine

def list_projects():
    """Return [{key,name,service}] for every anonymously-visible project."""
    data = http_get_json(BASE + "/rest/api/3/project")
    out = []
    for p in data:
        out.append({
            "key": p["key"], "name": p.get("name", ""),
            "service": SERVICE.get(p["key"], "Other"),
        })
    return sorted(out, key=lambda x: x["key"])


def collect(days=7, projects=None, customer_only=False, include_minor=False,
            min_score=0.0, cap=800, extra="", verbose=False):
    """Full pipeline: build JQL -> fetch -> normalize -> score -> sort.

    Shared by the CLI and the local web server. Returns (records, meta).
    """
    projects = projects if projects is not None else list(DEFAULT_PROJECTS)
    jql = build_jql(days, projects, customer_only, extra, drop_minor=not include_minor)
    raw = fetch(jql, cap, verbose=verbose)
    records = [normalize(i) for i in raw]
    if min_score > 0:
        records = [r for r in records if r["customerScore"] >= min_score]
    records.sort(key=lambda r: (r["customerScore"], r["updated"] or ""), reverse=True)
    meta = {
        "fetchedAt": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "days": days,
        "projects": "all" if not projects else len(projects),
        "projectList": projects or "all",
        "customerOnly": customer_only,
        "includeMinor": include_minor,
        "minScore": min_score,
        "jql": jql,
        "total": len(records),
    }
    return records, meta


# ----------------------------------------------------------------------------- HTML

def render_html(records, meta):
    payload = json.dumps({"issues": records, "meta": meta}, ensure_ascii=False)
    # Keep the page self-contained; data is embedded, rendered client-side.
    return HTML_TEMPLATE.replace("/*__DATA__*/", payload)


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>cbJiraRadar - recent Couchbase JIRA issues</title>
<style>
  :root{--bg:#0f1420;--panel:#161c2b;--edge:#26304a;--fg:#e6ebf5;--mut:#8a97b3;
        --acc:#e5322d;--cust:#ff8c42;--ok:#3ecf8e;--warn:#f5c451;--chip:#22304d;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
  header{padding:14px 18px;border-bottom:1px solid var(--edge);display:flex;gap:16px;align-items:baseline;flex-wrap:wrap}
  header h1{font-size:16px;margin:0;font-weight:650}
  header .meta{color:var(--mut);font-size:12px}
  .bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:10px 18px;border-bottom:1px solid var(--edge);background:var(--panel)}
  .bar input,.bar select{background:#0c1220;color:var(--fg);border:1px solid var(--edge);border-radius:6px;padding:6px 8px;font-size:12px}
  .bar input[type=search]{min-width:240px}
  .bar label{color:var(--mut);font-size:11px;display:flex;gap:5px;align-items:center}
  .count{margin-left:auto;color:var(--mut)}
  .wrap{padding:6px 12px 40px}
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
  .empty{padding:40px;text-align:center;color:var(--mut)}
</style></head>
<body>
<header>
  <h1>cbJiraRadar</h1>
  <span class="meta" id="meta"></span>
</header>
<div class="bar">
  <input type="search" id="q" placeholder="search summary / key / component / label ...">
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
<div class="empty" id="empty" style="display:none">No issues match the current filters.</div>
</div>
<script>
const DATA = /*__DATA__*/;
const issues = DATA.issues, meta = DATA.meta;
const $=s=>document.querySelector(s);
document.getElementById('meta').textContent =
  `${meta.total} issues · created/updated in last ${meta.days}d · ${meta.projects} projects · fetched ${meta.fetchedAt}`;

// populate filter dropdowns
const svcs=[...new Set(issues.map(i=>i.service))].sort();
const pris=[...new Set(issues.map(i=>i.priority).filter(Boolean))];
const priOrder=["Blocker","Critical","Major","High","Minor","Low","Trivial"];
pris.sort((a,b)=>priOrder.indexOf(a)-priOrder.indexOf(b));
for(const s of svcs){const o=document.createElement('option');o.textContent=s;$('#fSvc').appendChild(o);}
for(const p of pris){const o=document.createElement('option');o.textContent=p;$('#fPri').appendChild(o);}

let sortK="customerScore", sortAsc=false;
function fmtDate(s){if(!s)return"";const d=new Date(s);const now=Date.now();const h=(now-d)/3.6e6;
  if(h<24)return Math.max(0,Math.round(h))+"h ago";return Math.round(h/24)+"d ago";}
function esc(s){return (s||"").replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
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
    if(sortK==="updated"||sortK==="created"){x=x||"";y=y||"";}
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
  $('#empty').style.display=rows.length?"none":"block";
}
document.querySelectorAll('th[data-k]').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k;
    if(sortK===k)sortAsc=!sortAsc;else{sortK=k;sortAsc=(k==="key"||k==="service"||k==="summary");}
    document.querySelectorAll('th').forEach(x=>x.classList.remove('sorted','asc'));
    th.classList.add('sorted'); if(sortAsc)th.classList.add('asc'); apply();};
});
['#q','#fSvc','#fPri','#fCat','#fCust','#fOpen'].forEach(s=>{
  const el=$(s); el.addEventListener(el.type==='search'||el.tagName==='SELECT'?'input':'change',apply);});
apply();
</script>
</body></html>"""


def main():
    ap = argparse.ArgumentParser(description="Recent-issues radar over Couchbase public JIRA.")
    ap.add_argument("--days", type=int, default=7, help="look-back window in days (created OR updated)")
    ap.add_argument("--projects", default=",".join(DEFAULT_PROJECTS),
                    help="comma-separated project keys, or 'all' for every accessible project")
    ap.add_argument("--customer-only", action="store_true",
                    help="restrict to issues carrying a customer/support/escalation label")
    ap.add_argument("--include-minor", action="store_true",
                    help="keep Minor/Trivial/Low priority issues (dropped by default)")
    ap.add_argument("--jql", default="", help="extra JQL AND-ed into the query")
    ap.add_argument("--cap", type=int, default=800, help="max issues to fetch")
    ap.add_argument("--out", choices=["json", "html", "both"], default="both")
    ap.add_argument("--json-out", default="radar.json")
    ap.add_argument("--html-out", default="radar.html")
    ap.add_argument("--min-score", type=float, default=0.0, help="drop issues below this customer score")
    ap.add_argument("--insecure", action="store_true", help="skip TLS verification (last resort)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    if args.insecure:
        global _INSECURE
        _INSECURE = True

    projects = [] if args.projects.strip().lower() == "all" else \
        [p.strip() for p in args.projects.split(",") if p.strip()]
    records, meta = collect(
        days=args.days, projects=projects, customer_only=args.customer_only,
        include_minor=args.include_minor, min_score=args.min_score,
        cap=args.cap, extra=args.jql, verbose=args.verbose)
    if args.verbose:
        print("JQL:", meta["jql"], file=sys.stderr)

    if args.out in ("json", "both"):
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump({"meta": meta, "issues": records}, fh, ensure_ascii=False, indent=2)
        print(f"wrote {args.json_out} ({len(records)} issues)")
    if args.out in ("html", "both"):
        with open(args.html_out, "w", encoding="utf-8") as fh:
            fh.write(render_html(records, meta))
        print(f"wrote {args.html_out}")

    # tiny terminal summary
    cust = [r for r in records if r["customerScore"] >= 5]
    print(f"\n{len(records)} recent issues, {len(cust)} with strong customer signal (score>=5). Top:")
    for r in records[:10]:
        print(f"  [{r['customerScore']:>4}] {r['key']:<10} {r['priority'][:4]:<4} {r['service'][:10]:<10} {r['summary'][:70]}")


if __name__ == "__main__":
    main()
