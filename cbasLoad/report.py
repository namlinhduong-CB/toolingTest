"""report.py - renders run.json into a single report.html file, with the same
dark look as the Log Timeline builders. Chart.js is loaded from cdnjs."""

from __future__ import annotations

import json
from pathlib import Path

TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>cbasLoad report</title>
<style>
  :root {
    --bg:#0d1117; --panel:#161b22; --panel-2:#1c2230; --border:#2d333b;
    --text:#e6edf3; --muted:#8b949e; --dim:#6e7681; --accent:#ed2226;
    --ok:#3fb950; --info:#3987e5; --warn:#eda100; --err:#e34948; --violet:#8b7bd8;
    --font-sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    --font-mono:"SF Mono","Roboto Mono",Menlo,Consolas,monospace;
  }
  @media (prefers-color-scheme: light) { :root:not([data-theme="dark"]) {
    --bg:#f6f8fa; --panel:#ffffff; --panel-2:#f0f3f6; --border:#d0d7de;
    --text:#1f2328; --muted:#59636e; --dim:#818b98; } }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--text); font-family:var(--font-sans); font-size:14px;
         line-height:1.5; padding:20px 16px; max-width:1400px; margin:0 auto; }
  h1 { font-size:20px; font-weight:600; } h1 .mark { color:var(--accent); }
  h2 { font-size:15px; font-weight:600; margin:0 0 10px; }
  .sub { color:var(--muted); font-size:13px; margin:2px 0 18px; word-break:break-word; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:16px; }
  .tile { background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:10px 12px; }
  .tile .k { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.04em; }
  .tile .v { font-size:20px; font-weight:600; font-variant-numeric:tabular-nums; }
  .tile .v.bad { color:var(--err); } .tile .v.good { color:var(--ok); }
  .panel { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:14px; margin-bottom:16px; }
  .chart { position:relative; height:260px; }
  .two { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
  @media (max-width: 900px) { .two { grid-template-columns:1fr; } }
  table { width:100%; border-collapse:collapse; font-size:12.5px; font-variant-numeric:tabular-nums; }
  th, td { text-align:right; padding:5px 8px; border-bottom:1px solid var(--border); white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; }
  th { color:var(--muted); font-weight:500; position:sticky; top:0; background:var(--panel); }
  .scroll { overflow-x:auto; }
  .mono { font-family:var(--font-mono); font-size:12px; }
  .err { color:var(--err); } .muted { color:var(--muted); }
  .ev { display:flex; gap:10px; padding:4px 0; border-bottom:1px solid var(--border); font-size:12.5px; }
  .ev .t { font-family:var(--font-mono); color:var(--muted); min-width:64px; text-align:right; }
  .ev .kind { font-family:var(--font-mono); min-width:70px; color:var(--violet); }
  .ev.chaos .kind { color:var(--warn); } .ev.failed .kind { color:var(--err); }
  select { background:var(--panel-2); color:var(--text); border:1px solid var(--border); border-radius:6px;
           padding:4px 6px; font-size:12.5px; max-width:100%; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:8px; }
  details summary { cursor:pointer; color:var(--muted); font-size:12.5px; }
  pre { white-space:pre-wrap; word-break:break-word; font-family:var(--font-mono); font-size:12px; color:var(--muted); }
  .hidden { display:none; }
</style>
</head>
<body>
<h1><span class="mark">■</span> cbasLoad <span id="title"></span></h1>
<div class="sub" id="subtitle"></div>
<div class="grid" id="tiles"></div>

<div class="panel" id="p-throughput"><h2>Query throughput &amp; errors (per second)</h2><div class="chart"><canvas id="c-tput"></canvas></div></div>
<div class="panel" id="p-lat"><h2>Query latency percentiles (ms, per second; from intended start)</h2><div class="chart"><canvas id="c-lat"></canvas></div></div>
<div class="two">
  <div class="panel" id="p-kv"><h2>KV ingestion ops/s</h2><div class="chart"><canvas id="c-kv"></canvas></div></div>
  <div class="panel" id="p-lag"><h2>Ingestion lag (ms) &amp; KV→Analytics item gap</h2><div class="chart"><canvas id="c-lag"></canvas></div></div>
</div>
<div class="panel" id="p-server">
  <h2>Server metrics</h2>
  <div class="row"><select id="metric" multiple size="1" style="min-width:320px"></select>
  <span class="muted" style="font-size:12px">pick one or more (⌘/ctrl-click)</span></div>
  <div class="chart"><canvas id="c-server"></canvas></div>
</div>
<div class="panel" id="p-perq"><h2>Per query</h2><div class="scroll"><table id="t-perq"></table></div></div>
<div class="panel" id="p-errors"><h2>Errors</h2><div id="errors"></div></div>
<div class="panel" id="p-post"><h2>Post-check (each query re-run after the load)</h2><div class="scroll"><table id="t-post"></table></div></div>
<div class="panel"><h2>Events</h2><div id="events"></div></div>
<div class="panel"><details><summary>Run configuration</summary><pre id="meta"></pre></details></div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const RUN = __RUN_JSON__;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmt = (v, d=0) => v==null ? '–' : Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
const ms = v => v==null ? '–' : (v >= 10000 ? (v/1000).toFixed(1)+'s' : fmt(v, v<10?1:0)+'ms');
const S = RUN.summary, M = RUN.meta || {};
document.getElementById('title').textContent = '· ' + (M.command || '') + (M.label ? ' · ' + M.label : '');
document.getElementById('subtitle').textContent =
  `run ${RUN.run_id} · ${new Date(RUN.started*1000).toLocaleString()} · ${fmt(S.elapsed_s,1)}s · ${M.target || ''}`;
document.getElementById('meta').textContent = JSON.stringify(M, null, 2);

const tiles = [];
const Q = S.queries || {};
if (Q.total) {
  if (Q.cancelled) tiles.push(['cancelled', fmt(Q.cancelled)]);
  tiles.push(['queries', fmt(Q.total)], ['q/s', fmt(Q.qps,1)],
    ['errors', fmt(Q.errors), Q.errors ? 'bad':'good'],
    ['p50', ms(Q.p50)], ['p95', ms(Q.p95)], ['p99', ms(Q.p99)], ['max', ms(Q.max)]);
}
if (S.ingest) {
  const I = S.ingest;
  const errs = Object.entries(I.by_op||{}).filter(([k])=>k.startsWith('err:')).reduce((a,[,v])=>a+v,0);
  tiles.push(['kv ops', fmt(I.ops)], ['kv ops/s', fmt(I.ops_per_s,0)], ['kv errors', fmt(errs), errs?'bad':'good'],
    ['lag p50', ms(I.lag_ms_p50)], ['lag max', ms(I.lag_ms_max)], ['lag final', ms(I.lag_ms_final)]);
}
const chaosN = (RUN.events||[]).filter(e=>e.kind==='chaos').length;
if (chaosN) tiles.push(['chaos events', chaosN]);
document.getElementById('tiles').innerHTML = tiles.map(([k,v,c]) =>
  `<div class="tile"><div class="k">${k}</div><div class="v ${c||''}">${v}</div></div>`).join('');

if (typeof Chart === 'undefined') {  // offline, so keep the tiles and tables and explain the blank charts
  window.Chart = class { constructor(el){ el.parentElement.innerHTML =
    '<div class="muted" style="padding:20px">Chart.js could not be loaded from cdnjs (offline?) — ' +
    'charts unavailable; all numbers are still in the tables below and in run.json.</div>'; } destroy(){} };
  Chart.defaults = {font:{}};
}
Chart.defaults.color = css('--muted'); Chart.defaults.borderColor = css('--border');
Chart.defaults.font.family = css('--font-sans'); Chart.defaults.font.size = 11;
Chart.defaults.animation = false;
const chaosLines = (RUN.events||[]).filter(e=>e.kind==='chaos');
const eventPlugin = { id:'events', afterDatasetsDraw(chart) {
  const x = chart.scales.x; if (!x) return; const {top,bottom} = chart.chartArea; const c = chart.ctx;
  c.save(); c.strokeStyle = css('--warn'); c.setLineDash([4,3]); c.lineWidth = 1;
  for (const e of chaosLines) { const px = x.getPixelForValue(e.t); if (px<x.left||px>x.right) continue;
    c.beginPath(); c.moveTo(px,top); c.lineTo(px,bottom); c.stroke(); }
  c.restore(); } };
function line(id, datasets, opts={}) {
  return new Chart(document.getElementById(id), { type:'line', data:{datasets},
    options:{ maintainAspectRatio:false, parsing:false, normalized:true, spanGaps:false,
      interaction:{mode:'nearest', axis:'x', intersect:false},
      elements:{point:{radius:0}, line:{borderWidth:1.5, tension:0}},
      scales:{ x:{type:'linear', title:{display:true,text:'seconds'}, min:0}, y:{beginAtZero:true, ...(opts.y||{})},
               ...(opts.scales||{}) },
      plugins:{ legend:{labels:{boxWidth:10}}, tooltip:{callbacks:{title:it=>`t=${it[0].parsed.x}s`}} } },
    plugins:[eventPlugin] });
}
const ser = RUN.series || [];
if (Q.total) {
  line('c-tput', [
    {label:'ok/s', data:ser.map(s=>({x:s.t,y:s.ok})), borderColor:css('--ok'), backgroundColor:css('--ok')},
    {label:'errors/s', data:ser.map(s=>({x:s.t,y:s.err})), borderColor:css('--err'), backgroundColor:css('--err')},
    ...(RUN.level && RUN.level.length > 1 ? [{label:'profile level ×100', yAxisID:'y',
      data:RUN.level.flatMap(([t,l],i,a)=>[{x:t,y:l*100}].concat(i+1<a.length?[{x:a[i+1][0],y:l*100}]:[])),
      borderColor:css('--dim'), borderDash:[3,3], backgroundColor:css('--dim')}] : []) ]);
  line('c-lat', [
    {label:'p50', data:ser.map(s=>({x:s.t,y:s.p50})), borderColor:css('--info'), backgroundColor:css('--info')},
    {label:'p95', data:ser.map(s=>({x:s.t,y:s.p95})), borderColor:css('--violet'), backgroundColor:css('--violet')},
    {label:'p99', data:ser.map(s=>({x:s.t,y:s.p99})), borderColor:css('--warn'), backgroundColor:css('--warn')},
    {label:'max', data:ser.map(s=>({x:s.t,y:s.max})), borderColor:css('--err'), backgroundColor:css('--err'), hidden:true},
  ], {y:{type:'logarithmic', beginAtZero:false}});
} else { ['p-throughput','p-lat','p-perq','p-errors'].forEach(i=>document.getElementById(i).classList.add('hidden')); }

if (S.ingest) {
  const ops = new Set(); ser.forEach(s=>Object.keys(s.kv||{}).forEach(k=>ops.add(k)));
  const pal = [css('--ok'), css('--info'), css('--violet'), css('--warn'), css('--dim'), css('--accent')];
  line('c-kv', [...ops].map((op,i)=>({label:op, data:ser.map(s=>({x:s.t,y:(s.kv||{})[op]||0})),
    borderColor: op==='err'?css('--err'):pal[i%pal.length], backgroundColor: op==='err'?css('--err'):pal[i%pal.length]})));
  const ing = RUN.ingest || [];
  line('c-lag', [
    {label:'lag ms', data:ing.filter(s=>s.lag_ms!=null).map(s=>({x:s.t,y:s.lag_ms})), borderColor:css('--warn'), backgroundColor:css('--warn'), yAxisID:'y'},
    {label:'item gap (KV − Analytics)', data:ing.filter(s=>s.gap!=null).map(s=>({x:s.t,y:s.gap})), borderColor:css('--info'), backgroundColor:css('--info'), yAxisID:'y2', pointRadius:2},
  ], {scales:{y2:{position:'right', grid:{drawOnChartArea:false}}}});
} else { ['p-kv','p-lag'].forEach(i=>document.getElementById(i).classList.add('hidden')); }

// server metrics picker
const srv = RUN.server || [];
const keys = [...new Set(srv.flatMap(s=>Object.keys(s)).filter(k=>k!=='t'))].sort();
if (!keys.length) document.getElementById('p-server').classList.add('hidden');
else {
  const sel = document.getElementById('metric'); sel.size = Math.min(8, keys.length);
  const prefer = [/active_requests$/, /cpu_pct$/, /heap_memory_used_mib$/, /pending_merge_ops$/, /queued_jobs$/];
  const defaults = keys.filter(k=>prefer.some(r=>r.test(k))).slice(0,4);
  sel.innerHTML = keys.map(k=>`<option ${defaults.includes(k)?'selected':''}>${k}</option>`).join('');
  let ch = null;
  const draw = () => { const picked = [...sel.selectedOptions].map(o=>o.value);
    const pal = [css('--info'), css('--ok'), css('--warn'), css('--violet'), css('--err'), css('--accent')];
    if (ch) ch.destroy();
    ch = line('c-server', picked.map((k,i)=>({label:k, data:srv.filter(s=>s[k]!=null).map(s=>({x:s.t,y:s[k]})),
      borderColor:pal[i%pal.length], backgroundColor:pal[i%pal.length], yAxisID: i===0?'y':'y'+i})),
      {scales:Object.fromEntries(picked.slice(1).map((k,i)=>['y'+(i+1),{position:'right', grid:{drawOnChartArea:false}, display:i<2}]))});
  };
  sel.addEventListener('change', draw); draw();
}

// per query table
const PQ = S.per_query || {};
const rows = Object.entries(PQ).sort((a,b)=>b[1].count-a[1].count);
document.getElementById('t-perq').innerHTML =
  '<tr><th>query</th><th>count</th><th>ok</th><th>errors</th><th>p50</th><th>p90</th><th>p95</th><th>p99</th><th>max</th><th>avg server</th><th>avg rows</th><th>cancel req</th><th>cancelled</th></tr>' +
  rows.map(([n,s])=>{ const e = s.count - s.ok - ((s.errors||{}).cancelled||0); return `<tr><td class="mono">${n}</td><td>${fmt(s.count)}</td><td>${fmt(s.ok)}</td>
   <td class="${e?'err':''}">${fmt(e)}</td><td>${ms(s.p50)}</td><td>${ms(s.p90)}</td><td>${ms(s.p95)}</td><td>${ms(s.p99)}</td>
   <td>${ms(s.max)}</td><td>${ms(s.avg_server_ms)}</td><td>${fmt(s.avg_rows,1)}</td><td>${fmt(s.cancel_requested)}</td><td>${fmt(s.cancelled)}</td></tr>`}).join('');

// errors
const errBlocks = [];
for (const [n,s] of rows) for (const [code,cnt] of Object.entries(s.errors||{}))
  errBlocks.push(`<div class="ev"><span class="t">${fmt(cnt)}×</span><span class="kind">${code}</span><span><b class="mono">${n}</b> — <span class="muted">${(s.error_samples||{})[code]||''}</span></span></div>`);
if (S.ingest) for (const [k,v] of Object.entries(S.ingest.by_op||{})) if (k.startsWith('err:'))
  errBlocks.push(`<div class="ev"><span class="t">${fmt(v)}×</span><span class="kind">kv</span><span class="mono">${k.slice(4)}</span></div>`);
document.getElementById('errors').innerHTML = errBlocks.join('') || '<span class="muted">none</span>';
if (!errBlocks.length && !Q.total) document.getElementById('p-errors').classList.add('hidden');

// post check
const PC = (RUN.extra||{}).post_check;
if (PC && PC.length) {
  document.getElementById('t-post').innerHTML = '<tr><th>query</th><th>result</th><th>ms</th><th style="text-align:left">error</th></tr>' +
    PC.map(r=>`<tr><td class="mono">${r.name}</td><td class="${r.ok?'':'err'}">${r.ok?'ok':'FAIL'}</td><td>${fmt(r.ms)}</td><td style="text-align:left;white-space:normal" class="muted">${r.error||''}</td></tr>`).join('');
} else document.getElementById('p-post').classList.add('hidden');

// events
document.getElementById('events').innerHTML = (RUN.events||[]).map(e=>
  `<div class="ev ${e.kind} ${e.failed?'failed':''}"><span class="t">${e.t.toFixed(1)}s</span><span class="kind">${e.kind}</span><span>${e.detail}</span></div>`).join('')
  || '<span class="muted">none</span>';
</script>
</body>
</html>
"""


def write_report(run: dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "run.json").write_text(json.dumps(run, indent=1, default=str))
    blob = json.dumps(run, default=str).replace("</", "<\\/")
    html = TEMPLATE.replace("__RUN_JSON__", blob)
    p = out_dir / "report.html"
    p.write_text(html)
    return p
