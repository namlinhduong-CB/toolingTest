#!/usr/bin/env python3
"""
Forums source for cbJiraRadar - recent customer issues on the Couchbase
Discourse forums (forums.couchbase.com).

Why forums beat the ticket tracker for "what are customers hitting right now":
these are problems customers raise directly, often before (or without) any
JIRA ever existing. Discourse hands us clean signals JIRA can't:

  * the original poster's `staff` / trust level -> is this an actual customer
    or a Couchbase employee (no label-guessing);
  * `has_accepted_answer` + `reply_count` -> unresolved and unanswered, i.e.
    a customer still stuck and waiting;
  * views / likes -> how many others are hitting the same thing.

Anonymous public JSON API, stdlib only. Reuses radar.http_get_json for the
TLS-hardened fetch.
"""

import argparse
import datetime as dt
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from radar import http_get_json  # shared TLS-hardened GET

BASE = "https://forums.couchbase.com"

# category-name -> coarse service area (matches radar.py's vocabulary)
def service_for(name):
    n = (name or "").lower()
    if "sdk" in n or "client librar" in n:
        return "SDK"
    if "connector" in n or "odbc" in n or "jdbc" in n or "kafka" in n or "spark" in n or "elasticsearch" in n:
        return "Connector"
    if "mobile" in n or "lite" in n or "sync gateway" in n or "edge server" in n:
        return "Mobile"
    if "full text" in n or "search" in n:
        return "Search"
    if "sql++" in n or "query" in n or "n1ql" in n:
        return "Query"
    if "analytics" in n or "columnar" in n:
        return "Analytics"
    if "eventing" in n:
        return "Eventing"
    if "capella" in n:
        return "Capella"
    if "server" in n:
        return "Server"
    if "announce" in n or "community" in n or "beta" in n:
        return "Community"
    return "Other"


def load_categories():
    """id -> category name, from the cached categories.json (with subcats).

    site.json carries the same map but is large and slow; categories.json is
    the cached, fast endpoint.
    """
    data = http_get_json(BASE + "/categories.json?include_subcategories=true")
    out = {}
    for c in data.get("category_list", {}).get("categories", []):
        out[c["id"]] = c.get("name", "")
        for s in c.get("subcategory_list", []) or []:
            out[s["id"]] = s.get("name", "")
    return out


def strip_html(cooked, limit=400):
    text = re.sub(r"<[^>]+>", " ", cooked or "")
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def fetch_recent(days, cap, max_pages=25, verbose=False):
    """Collect topics *created* in the window.

    We page the plain (cached, bumped-ordered) latest.json rather than the
    uncached order=created view, which the server answers 50x slower. A topic
    created inside the window was necessarily bumped inside it too (creation is
    a bump), so bumped-order paging is a complete superset; we keep those whose
    created_at is in-window and stop once a whole page has bumped past the
    cutoff.
    """
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    topics = []
    for page in range(max_pages):
        url = f"{BASE}/latest.json?page={page}"
        data = http_get_json(url)
        tl = data.get("topic_list", {})
        batch = tl.get("topics", [])
        if not batch:
            break
        newest_bump = None
        for t in batch:
            if t.get("pinned_globally"):
                continue  # site-wide sticky, not a recent issue
            bumped = parse_dt(t.get("bumped_at") or t.get("last_posted_at"))
            if bumped and (newest_bump is None or bumped > newest_bump):
                newest_bump = bumped
            created = parse_dt(t.get("created_at"))
            if created and created >= cutoff:
                topics.append(t)
        if verbose:
            print(f"  page {page}: {len(topics)} in-window so far", file=sys.stderr)
        if not tl.get("more_topics_url") or len(topics) >= cap:
            break
        if newest_bump is not None and newest_bump < cutoff:
            break  # everything from here on was created before the window
        time.sleep(0.4)  # be gentle: anonymous API throttles under bursts
    return topics[:cap]


def parse_dt(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


_ENRICH_CACHE = {}  # topic id -> enrich dict (OP bodies rarely change)


def enrich(topic_id):
    """Fetch a topic's OP: body text + poster staff/trust (customer signal)."""
    if topic_id in _ENRICH_CACHE:
        return _ENRICH_CACHE[topic_id]
    try:
        d = http_get_json(f"{BASE}/t/{topic_id}.json")
    except Exception:
        return {}
    posts = d.get("post_stream", {}).get("posts", [])
    op = posts[0] if posts else {}
    res = {
        "excerpt": strip_html(op.get("cooked", "")),
        "opAuthor": op.get("username"),
        "opName": op.get("name"),
        "opStaff": bool(op.get("staff")),
        "opTrust": op.get("trust_level"),
        "opAdmin": bool(op.get("admin")) or bool(op.get("moderator")),
        "enriched": True,
    }
    _ENRICH_CACHE[topic_id] = res
    return res


def enrich_records(records, workers=4):
    """Fill OP body + poster status into records in place, then re-score."""
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(enrich, r["id"]): r for r in records}
        for fut in as_completed(futs):
            futs[fut].update(fut.result())
    for r in records:
        r["signalScore"], r["signals"] = score(r)
    return records


CATS = {}


def tag_names(tags):
    # Discourse returns tags either as plain strings or as {id,name,slug} objects.
    out = []
    for x in tags or []:
        out.append(x.get("name") if isinstance(x, dict) else x)
    return [t for t in out if t]


def normalize(t):
    cat_name = CATS.get(t.get("category_id"), "")
    return {
        "key": "#" + str(t.get("id")),
        "id": t.get("id"),
        "url": f"{BASE}/t/{t.get('slug')}/{t.get('id')}",
        "title": t.get("title") or "",
        "category": cat_name,
        "service": service_for(cat_name),
        "created": t.get("created_at"),
        "lastActivity": t.get("last_posted_at") or t.get("bumped_at"),
        "replies": t.get("reply_count", 0),
        "posts": t.get("posts_count", 0),
        "views": t.get("views", 0),
        "likes": t.get("like_count", 0),
        "answered": bool(t.get("has_accepted_answer")),
        "closed": bool(t.get("closed")),
        "tags": tag_names(t.get("tags")),
        "lastPoster": t.get("last_poster_username"),
        # filled by enrich(); opStaff None = not yet read
        "excerpt": "", "opAuthor": None, "opStaff": None, "opTrust": None,
        "enriched": False,
    }


def score(rec):
    """Support/customer signal: unresolved + customer-raised + hot floats up."""
    s = 0.0
    signals = []
    if not rec["answered"]:
        s += 2
        if rec["replies"] == 0:
            s += 2.5
            signals.append("no-reply")
        else:
            signals.append("unanswered")
    else:
        signals.append("answered")
    if rec["opStaff"] is False and rec["opAuthor"]:
        # a genuine community/customer poster, not a Couchbase employee
        s += 2
        signals.append("customer")
    elif rec["opStaff"]:
        signals.append("staff-post")
    v = rec["views"] or 0
    if v:
        s += min(v / 60.0, 3)
        if v >= 120:
            signals.append(f"views:{v}")
    if rec["replies"]:
        s += min(rec["replies"], 3) * 0.5
    if rec["likes"]:
        s += min(rec["likes"], 2)
    if rec["closed"] and not rec["answered"]:
        signals.append("closed-unanswered")
    return round(s, 1), signals


def collect(days=30, cap=400, enrich_cap=120, do_enrich=True, verbose=False):
    """Collect recent topics. With do_enrich=False this is the *fast* path
    (list endpoints only, no per-topic body reads) used for instant first paint;
    the web app then enriches the top rows in a second call."""
    global CATS
    if not CATS:
        CATS = load_categories()
    raw = fetch_recent(days, cap, verbose=verbose)
    records = [normalize(t) for t in raw]

    enriched = 0
    if do_enrich:
        to_enrich = sorted(records, key=lambda r: r["created"] or "", reverse=True)[:enrich_cap]
        if verbose:
            print(f"  enriching {len(to_enrich)} topics ...", file=sys.stderr)
        enrich_records(to_enrich)
        enriched = len(to_enrich)

    for r in records:
        r["signalScore"], r["signals"] = score(r)
    records.sort(key=lambda r: (r["signalScore"], r["created"] or ""), reverse=True)

    meta = {
        "source": "forums",
        "fetchedAt": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "days": days,
        "total": len(records),
        "enriched": enriched,
    }
    return records, meta


def main():
    ap = argparse.ArgumentParser(description="Recent customer issues on Couchbase forums.")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--cap", type=int, default=400)
    ap.add_argument("--enrich-cap", type=int, default=120)
    ap.add_argument("--json-out", default="forums.json")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    records, meta = collect(days=args.days, cap=args.cap,
                            enrich_cap=args.enrich_cap, verbose=args.verbose)
    with open(args.json_out, "w", encoding="utf-8") as fh:
        json.dump({"meta": meta, "topics": records}, fh, ensure_ascii=False, indent=2)
    print(f"wrote {args.json_out} ({len(records)} topics, last {args.days}d)")
    print(f"\nTop by support signal:")
    for r in records[:12]:
        who = "staff" if r["opStaff"] else "cust"
        flag = "✓" if r["answered"] else ("·" if r["replies"] else "!")
        print(f"  [{r['signalScore']:>4}] {flag} {who:<5} {r['service'][:9]:<9} r{r['replies']:<3} v{r['views']:<5} {r['title'][:60]}")


if __name__ == "__main__":
    main()
