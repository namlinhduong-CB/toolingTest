# cbForumRadar / cbJiraRadar

A recent-issues radar over Couchbase's public sources. Two sources, one local
web app:

- **Forums** (default, `/`) — recent **customer questions** on the Couchbase
  community forums (`forums.couchbase.com`, Discourse). This is where customers
  raise problems directly, often before any ticket exists.
- **JIRA** (`/jira`) — tickets created/updated across the 46 public projects on
  `issues.couchbase.com`.

No API key. No third-party deps (stdlib only). Anonymous read-only.

## Web app

```bash
python3 serve.py
```

Opens a browser to <http://127.0.0.1:8787> (forums view; JIRA view linked in the
header). All controls are in the page.

### Forums view (`cbForumRadar`)

Reads each recent topic's **original post** — the customer's actual question —
and the poster's `staff`/trust level, so "raised by a real customer" is a fact,
not a guess. Scores by **support signal**: unanswered + nobody-replied +
customer-raised + view count. Columns: Signal / Topic (title + body excerpt +
category + tags + customer|staff|no-reply badges) / Service / Replies / Views /
Answered / Created. Filters: unanswered-only, customers-only, no-replies,
service, category, search. Controls: Last N days, Min signal, Cap, Refresh.

Notes:
- The forum is **low-volume** (~1 topic/week/category) — use a wide window
  (30–90d). Default is 45.
- The anonymous forum API **throttles under bursts**, so the server caches each
  feed for 3 min (a Refresh forces a re-scrape) and only uses the *cached*
  Discourse endpoints (`latest.json`, `categories.json`); the slow uncached
  `order=created` / `site.json` views are avoided.

### JIRA view (`cbJiraRadar`)

All controls are in the page:

- **Last N days** – look-back window (matches created *or* updated).
- **Projects** – Curated (support-relevant subset) / All public / custom keys.
- **Min signal**, **Cap**, **customer-labelled only**, **include Minor**.
- **Refresh** – re-pulls live from JIRA.
- Below that: instant client-side filtering of the loaded set (search,
  service, priority, status, customer-signal-only, open-only) and sortable
  columns. Click a key to open the ticket.

The browser can't call JIRA directly (the API returns no CORS headers), so
`serve.py` fetches server-side and hands the page JSON. Nothing is written to
disk; nothing leaves the machine except anonymous reads of `issues.couchbase.com`.

## CLI (for scripting / snapshots)

```bash
python3 radar.py --days 7                       # curated projects, HTML + JSON
python3 radar.py --days 14 --customer-only       # only customer/support-labelled
python3 radar.py --projects MB --min-score 5     # Server, strong signal only
python3 radar.py --projects all --days 3
```

Writes `radar.json` (structured, for diffing/scheduling) and `radar.html`
(self-contained, data embedded). `--include-minor` keeps Minor/Trivial/Low
(dropped by default). `--insecure` skips TLS verification if the CA bundle
can't be located.

## Scoring

`customerScore` is a heuristic 0–11ish signal for "a customer actually hit this":
`customer-issue`/`support`/`escalation`/`field-issue` labels (+5), priority
(Critical/Blocker +4, Major/High +3), votes (≤+3), watchers (≤+2). It ranks the
feed; it is not a JIRA field. `--customer-only` is the stricter, query-level
"only tickets with a customer label" gate.

### Forums CLI

```bash
python3 forums.py --days 45           # writes forums.json + prints top by signal
```

## Files

- `serve.py` – local web app. Serves both views and proxies each source
  server-side (the browser can't fetch either API cross-origin). Run this.
- `forums.py` – forums engine + CLI (`collect()`), the default source.
- `radar.py` – JIRA engine + CLI (`collect()` / `list_projects()`); shared
  TLS-hardened `http_get_json` reused by `forums.py`.

## Next

- Optional AI triage pass (one-line summaries, service/severity classification,
  bug-vs-config, cross-ref to `SupportabilityTool.xlsx` MBs) — the forum OP body
  is already captured, so it's ready to feed a model.
- Cluster/trend detection across a rolling window ("N new reports of X this
  week"), and dedupe forum topics against JIRA tickets.
