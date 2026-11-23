# cbasLoad

cbasLoad creates collections of synthetic data and generates query and/or
ingestion load against Enterprise Analytics (EA / Columnar) or Server with the
Analytics Service. It can disrupt the cluster at set times during a run
("chaos"), and writes an HTML report for each run.

By default it uses the running EA_repro environment, which it finds through
`../EA_repro/.ea_repro_state.json` (or `../EA_repro/dist/...` when EA_repro
was built with PyInstaller). To use another cluster, pass `--host`/`-u`/`-p`.

```bash
./cbasload.py gen --template orders --docs 1000000 --doc-size 512B-2KB --index customerId:bigint
./cbasload.py query-load --duration 5m --concurrency 16
./cbasload.py ingest-load --rate 3000 --duration 5m
./cbasload.py mixed --duration 5m --concurrency 8 --rate 1500 --chaos 60s:pause-source --chaos 120s:unpause-source
./cbasload.py preset list
```

Each load command writes `runs/<run-id>-<command>/run.json` and `report.html`.

## Requirements

- Python 3.9+.
- `boto3` for loading through S3 (already in EA_repro's requirements).
- The Couchbase Python SDK for the ingestion writer and for seeding remote link
  collections: `pip install couchbase`.
- Docker, for the source container it creates and for the chaos actions.

## Commands

### gen

Creates a collection.

```bash
./cbasload.py gen -c orders -n 500000 --template orders --into standalone
```

The `--into` option decides what kind of collection is built:

- `standalone` (the default) creates a standalone collection with
  `PRIMARY KEY (id: string)`. It can be loaded in two ways. `--load-method copy`
  uploads NDJSON to EA_repro's MinIO and then runs `COPY INTO`, which is quick
  and is the default on EA_repro. `--load-method insert` sends batches of
  `UPSERT INTO … ([docs])` in parallel over HTTP, and works on any cluster.
- `external` writes NDJSON to MinIO and creates an external collection on the
  S3 link (EA_repro only).
- `remote` creates a Couchbase source, seeds it with the SDK, creates the remote
  link and collection, connects the link, and waits until `COUNT(*)` reaches N.

The shape of the documents is controlled with:

- `--template flat|nested|arrays|orders`. `orders` also builds a
  `<name>_customers` collection, which joins on `customerId = custNo`.
- `--doc-size 1KB`, or a range such as `512B-16KB`.
- `--pad random|compressible` for what the padding looks like.
- `--skew` for a Zipf hot set on `customerId`, and `--cardinality` for the
  number of customers.
- `--drift 0.1` to give 10% of the documents a missing, extra or retyped field.
- `--bigints` to add values over 2^53.
- `--seed`.

The documents are deterministic, i.e. doc n is always the same for a given
seed. This is what lets `${key}` and `${customer}` in the queries match real
data. Other options are `--index field:type` (can be repeated), and
`--recreate` or `--append` when the collection already exists.

### query-load

Runs a SQL++ workload.

- With `--concurrency N` a fixed number of workers send queries back to back.
- With `--qps R` queries are sent at a target rate instead, together with
  `--max-inflight` and `--arrival poisson|uniform`. Latency is measured from
  when the query should have been sent, so time spent queueing is included in
  the numbers rather than hidden.
- `--profile steady|ramp|step|spike` shapes the load over the run, using
  `--duration`, `--ramp`, `--steps` and `--spike-base/--spike-every/--spike-len`.
- Each template has a built-in mix of queries: point lookup, range scan, filter
  with GROUP BY, customer history, top N, window RANK and a heavy scan. The
  `arrays` template adds UNNEST and ANY…SATISFIES queries, and `orders` adds
  joins.
- The mix can be changed with `--only name …` and `--weight name=W`, or replaced
  with a file through `--mix file.json`:

  ```json
  {"queries": [{"name": "by_country", "weight": 5, "timeout": "30s",
                "sql": "SELECT COUNT(*) FROM ${coll} c WHERE c.country = ${country};"}]}
  ```

  The placeholders are `${coll}`, `${coll:customers}`, `${key}`, `${customer}`,
  `${int:a,b}`, `${float:a,b}`, `${choice:A,B}`, `${country}`, `${status}`,
  `${category}`, `${tier}`, `${daterange:N}` (used after `BETWEEN`) and
  `${date}`.
- `--cancel-pct 20 --cancel-after 0.05-1.5s` cancels queries mid-flight with
  `DELETE /analytics/admin/active_requests`. A `23010` response is counted as
  cancelled rather than as an error.
- `--post-check N` runs every query N times after the load has finished, without
  cancelling them. This picks up collections that are left broken after
  cancelled queries, which is what SUPP-121/129 look like.
- Each request has a `client_context_id` of `cbasload-<run>-<query>-<n>`, so it
  can be found in the analytics request logs and in the CBAS Logs Timeline.

### ingest-load

Writes KV mutations that reach Analytics through a remote link.

The source is set with `--source`:

- `container` (the default on EA_repro) runs a Couchbase Server container called
  `<project>-cbsource` on the EA_repro Docker network. It is reachable from the
  host through alternate addresses. Related options are `--source-image` and
  `--source-compression off|passive|active`.
- `existing` uses the cluster given with `--source-host`, along with
  `--source-user` and `--source-password`. If Analytics reaches it under another
  name, set that with `--source-link-host`.
- `local` is for Server clusters with the Analytics Service, and uses the
  cluster's own bucket through the Local link.

The writer is set with `--engine`:

- `sdk` (the default) uses the Python SDK with an operation mix set by
  `--ops upsert=70,insert=15,delete=10,expire=5`, along with `--rate`,
  `--writers`, `--ttl`, `--key-skew` and the load profile. It uses the same
  templates as `gen`.
- `pillowfight` runs `cbc-pillowfight` inside the source container, for the
  highest throughput. It runs at a constant rate and only does upserts and gets.
  Note however, its `--rate-limit` is only approximate and its ops are counted
  in steps of 10,000. It is started again automatically after a `kill-source`.

For the lag, a heartbeat document is upserted every `--hb-interval`. The probe
reports how long the oldest heartbeat that KV has acknowledged has been waiting
to show up in Analytics. As such, the lag is 0 when Analytics is caught up, and
it isn't limited by the heartbeat interval. Every `--count-every` it also
reports the KV collection item count minus the Analytics `COUNT(*)` (the
"gap"). `--drain 120s` waits at the end of the run until the gap has been 0
twice in a row.

### mixed

Runs `query-load` and `ingest-load` together, with the queries going to the
ingestion collection, or to `--query-collection` if given. It takes the options
of both commands.

### Chaos

Disruptions are added with `--chaos AT:ACTION[:ARG]`, which can be repeated.
`./cbasload.py chaos-actions` lists them:

- EA nodes: `restart-node`, `kill-node`, `stop-node` and `start-node` (with an
  `ea-nodeN` argument), and `rebalance`.
- The link: `disconnect-link` and `connect-link`.
- The source container: `pause-source`, `unpause-source`, `kill-source`,
  `start-source` and `restart-source`.
- The source bucket: `recreate-source-bucket`.
- Queries: `cancel-all`.
- The writers: `pause-writes` and `resume-writes`.

Each action is shown as a dashed line on the charts and in the list of events.

### Presets

`preset list`, `preset show NAME` and `preset run NAME [overrides…]` work with
saved sets of options. Some are based on known issues in
`SupportabilityTool.xlsx`:

- `cancel-storm` (SUPP-121, SUPP-129)
- `kv-outage` (SUPP-42)
- `rollback` (SUPP-83, SUPP-86)
- `relink-recreate` (SUPP-33)
- `compressed-dcp` (SUPP-30)
- `merge-pressure` (SUPP-170)

The others are `smoke`, `query-ramp`, `query-spike`, `schema-drift`,
`rebalance-under-load` and `node-restart`.

Anything given after the preset name overrides it, e.g.
`preset run cancel-storm --duration 30s`. The presets recreate the conditions
for each issue. Whether the bug itself shows up depends on the product version,
so the logs still need checking afterwards, e.g. with cbKnownIssueAnalyzer.

### Housekeeping

- `status` lists the collections cbasLoad has created, with their live counts and
  the state of the source.
- `drop -c NAME [--with-source]` drops a collection and its companion
  collections, and can also remove the source.
- `source up|down|status` manages the source.
- `ping` shows which cluster is being used and checks each analytics node.
- `report runs/<dir>` builds the HTML report again from `run.json`.

## The report

`report.html` contains everything it needs apart from Chart.js, which is loaded
from cdnjs. Without internet access the charts show a message instead, but the
numbers are still in the tables and in `run.json`. The report has:

- Summary tiles.
- Query throughput, errors and the load profile level.
- p50/p95/p99/max latency per second.
- KV ops by type, ingestion lag and the item gap.
- A picker for the server metrics, covering every `cbas_*` metric from
  `:8095/_prometheusMetrics` plus node CPU and RAM. Counters are turned into
  rates per second.
- A table per query, the errors with sample messages, the post-check results
  and the events.

## Files

- `cbasload.py`: the CLI, the `gen` loaders and running the workloads.
- `target.py`: finding the cluster (EA_repro or `--host`), the keep-alive HTTP
  client and the SQL++ helpers.
- `docgen.py`: the document templates, Zipf sampling, padding and drift.
- `querymix.py`: the built-in mixes, placeholders, the query workload,
  cancellation and the post-check.
- `ingest.py`: the source container, remote link and collection, the SDK and
  pillowfight writers, and the lag probe.
- `recorder.py`: metrics, load profiles, the Prometheus sampler and the live
  console line.
- `chaos.py`, `report.py` and `state.py`: the disruptions, the HTML report, and
  `.cbasload_state.json`, which records what `gen` has built.

## What has been tested

Tested on EA 2.1.1 (EA_repro, 1 node) with a `couchbase/server:latest` source
container:

- `gen` for standalone (with both copy and insert), external and remote link
  collections
- `query-load` with a fixed number of workers and with a target rate, with
  cancellations and the post-check
- `ingest-load` with both the sdk and pillowfight writers
- `mixed` with the source paused and killed, the link disconnected and
  reconnected, and an EA node restarted

`--host` was also tried against the same EA cluster. The following haven't
been tested yet:

- Server with the Analytics Service (`--kind server`, `--source local`)
- rebalancing a multi-node cluster
- `--source existing`

The SQL++ syntax differs between the products, so for these the EA form is
tried first and then the Server form.
