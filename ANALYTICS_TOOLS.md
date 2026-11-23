# Couchbase Analytics support tools

This page covers the Analytics (CBAS / Enterprise Analytics) tools in
`namTooling/`, what each one is for, how to run it, and how they fit together
when working a ticket. Each tool also has its own README with the full details.

## The tools

- `EA_repro/` stands up a disposable Enterprise Analytics cluster in Docker,
  backed by MinIO for S3 storage. It can be driven from the CLI, a local web UI
  or a macOS app.
- `cbasLoad/` creates collections of synthetic data and generates query and/or
  ingestion load against EA or Server Analytics. It can also disrupt the
  cluster at set times, and writes an HTML report for each run.
- `cbKnownIssueAnalyzer/` scans a customer's `cbcollect_info` for the known
  Analytics MBs listed in `SupportabilityTool.xlsx`, per node, with the log
  evidence and a confidence level.
- `CBAS_Logs_Timeline.html` merges a node's `analytics_*.log` files into one
  timeline that can be filtered and charted. It runs in the browser.
- `SupportabilityTool.xlsx` is the catalogue of known Analytics issues
  (SUPP-30 to SUPP-170). The analyser and the cbasLoad presets are built on it.
- `cbasSupportability/` holds redacted cbcollect bundles from real cases, used
  to validate the analyser. `RedactedClusterLogs_65973` is an 11 node 7.2.5
  cluster and `RedactedClusterLogs_66552` is a single 7.6.3 Capella node.

## How they fit together

A ticket would usually go through the tools like this:

* Triage the customer's logs. Run `cbKnownIssueAnalyzer` over the cbcollect to
  see if a known MB matches, then open the affected node's analytics logs in
  `CBAS_Logs_Timeline.html` to look at the sequence of events around the
  incident.
* Reproduce. Stand up an environment with `EA_repro` (or use a lab Server
  cluster), then use `cbasLoad` to get close to the customer's setup, i.e. data
  volume and document size, query mix and concurrency, ingestion rate, and the
  event that triggers the problem (a KV outage, a node restart, cancelled
  queries etc.). Several of the cbasLoad presets already match the conditions
  of MBs in the catalogue.
* Confirm. Collect the logs from the repro with `./ea_repro.py bundle` and run
  them back through the analyser and the timeline. cbasLoad tags its requests
  with a `client_context_id` of `cbasload-<run>-...`, which together with its
  report makes it easier to match load and disruptions to log lines.

## Requirements

- Python 3.9+ for the Python tools.
- Docker Desktop for `EA_repro`, and for the cbasLoad source container and
  chaos actions. Give Docker at least 8 GB of RAM, as a single EA node needs
  around 4 GB.
- Python packages per tool: `boto3` (EA_repro, and cbasLoad S3 loading),
  `couchbase` (cbasLoad ingestion) and `openpyxl` (cbKnownIssueAnalyzer).
  ```bash
  pip install boto3 couchbase openpyxl
  ```
- A recent browser for the timeline and the cbasLoad report. The charts load
  Chart.js from cdnjs, so they need internet access.

---

## EA_repro

EA_repro builds an Enterprise Analytics cluster in Docker with MinIO used in
two ways. Firstly, as EA's own columnar blob store (bucket `ea-storage`).
Secondly, as an external data source (bucket `ea-data`) holding N generated
JSON documents, which EA reads through an S3 link (`s3link`) and an external
collection (`ext_data`). It takes care of the EA specific provisioning order
and gotchas (blob storage has to be set before `cluster-init`, multi-node
joins need FQDNs and HTTPS, and the MinIO hostname can't contain an
underscore). At the end of `up` it checks `COUNT(*)` against the number of
documents requested.

To use it, from `EA_repro/`:

```bash
pip install -r requirements.txt

./ea_repro.py up                                    # 1 node, 10k docs
./ea_repro.py up --nodes 3 --analytics-ramsize 4096 --documents 1000000
./ea_repro.py status                                # containers, nodes, live doc count
./ea_repro.py info                                  # endpoints + credentials
./ea_repro.py query 'SELECT country, COUNT(*) n FROM ext_data GROUP BY country;'
./ea_repro.py load --documents 250000               # resize the dataset in place
./ea_repro.py logs --service ea-node1 --tail 300
./ea_repro.py bundle                                # zip logs + config + cluster state
./ea_repro.py down --purge                          # remove containers, volumes, state
```

There is also a local web UI, `./ea_repro_ui.py`, at `http://localhost:8500`
which can create and resize the environment, run SQL++, fetch logs and download
a bundle. `./build_macos.sh` builds `dist/EA-Repro`, a single binary that
works as both the UI and the CLI.

Some examples:

- Use a specific EA version:
  `./ea_repro.py up --ea-image couchbase/enterprise-analytics:2.1.0`
- Limit resources to match a small customer node:
  `./ea_repro.py up --ea-memory 4g --ea-cpus 2`
- Larger documents for object size repros:
  `./ea_repro.py up --documents 100000 --payload-bytes 1800`
- Two environments at the same time: give each its own `--project` and port
  bases (`--ea-ui-port`, `--ea-query-port`, `--minio-api-port`,
  `--minio-console-port`). Ports that are already in use get shifted to free
  ones automatically, unless `--strict-ports` is set.

The default credentials are `Administrator / password123` for EA and
`minioadmin / minioadmin123` for MinIO. cbasLoad finds the cluster through the
state file `.ea_repro_state.json`, which sits next to the script, or next to
the binary in `dist/` when using the built app.

Full details are in [`EA_repro/README.md`](EA_repro/README.md).

---

## cbasLoad

cbasLoad creates collections of synthetic documents and generates load against
Analytics. The commands are:

- `gen` builds a standalone collection (loaded with `COPY INTO` from MinIO, or
  with batched `UPSERT`s), an external S3 collection, or a remote link
  collection fed from a Couchbase Server container that it creates.
- `query-load` runs a weighted mix of SQL++ queries (lookups, scans, GROUP BY,
  window functions, UNNEST and joins), either with a fixed number of workers or
  at a target queries per second. The load can be steady, ramp up, go up in
  steps or spike, and a share of the queries can be cancelled mid-flight.
- `ingest-load` writes upserts, inserts, deletes and expiries to the KV source
  (through the Python SDK or pillowfight), and measures the ingestion lag and
  the item count difference between KV and Analytics.
- `mixed` runs query and ingestion load together.
- `--chaos` schedules disruptions during a run, such as pausing or killing the
  source, disconnecting the link, restarting an EA node, rebalancing,
  recreating the bucket or cancelling all requests.

Each run writes `runs/<id>/run.json` and a `report.html` with throughput,
latency percentiles, ingestion lag, the `cbas_*` server metrics, per query
stats, errors and the chaos events. By default it uses the running EA_repro
environment, or any other cluster with `--host`/`-u`/`-p`.

To use it, from `cbasLoad/`:

```bash
./cbasload.py ping                                  # which cluster am I talking to?

# data
./cbasload.py gen --template orders --docs 1000000 --doc-size 512B-2KB \
    --index customerId:bigint --index created_at:string

# query load
./cbasload.py query-load --duration 5m --concurrency 16
./cbasload.py query-load --qps 100 --profile spike --duration 10m

# ingestion load (creates the source container + remote link on first use)
./cbasload.py ingest-load --rate 3000 --duration 5m

# both, with disruptions
./cbasload.py mixed --duration 5m --concurrency 8 --rate 1500 \
    --chaos 60s:pause-source --chaos 180s:unpause-source

./cbasload.py status                                # what cbasLoad created + live counts
./cbasload.py drop -c cbasingest --with-source      # clean up
```

Some examples:

- Queries failing on a collection after earlier queries were cancelled
  (SUPP-121/129):
  ```bash
  ./cbasload.py preset run cancel-storm
  ```
  30% of queries are cancelled mid-flight, then every query is run again
  without cancelling (`--post-check`), and any failures are listed in the
  report.
- Finding the point where latency starts to climb:
  `./cbasload.py preset run query-ramp` goes from 1 to 32 workers over 5
  minutes. Look at where p99 starts to rise in the report.
- Ingestion during a KV outage (SUPP-42): `./cbasload.py preset run kv-outage`.
  Check that queries keep working, and how long the lag takes to come back
  down after the source is unpaused.
- DCP rollback after unpersisted mutations are lost (SUPP-83/86):
  `./cbasload.py preset run rollback`.
- Running against a lab cluster rather than EA_repro:
  ```bash
  ./cbasload.py gen --host 10.0.0.12 -u Administrator -p '…' --kind server --docs 200000
  ./cbasload.py query-load --host 10.0.0.12 -u Administrator -p '…' --duration 10m
  ```
- A custom query mix: `./cbasload.py query-load --mix my_mix.json`, where each
  entry has a `name`, `weight` and `sql`, with placeholders such as `${coll}`,
  `${key}`, `${customer}` and `${daterange:7}` (see the cbasLoad README).

`./cbasload.py preset list` shows all twelve presets. Anything given after the
preset name overrides it, e.g. `preset run cancel-storm --duration 30s`.

It has been tested against EA 2.1.1. Server with the Analytics Service
(`--kind server`, `--source local`) and multi-node rebalances haven't been
tried yet. The source container it creates (`<project>-cbsource`) is left
running between runs, and can be removed with `./cbasload.py source down`.

Full details are in [`cbasLoad/README.md`](cbasLoad/README.md).

---

## cbKnownIssueAnalyzer

The analyser scans a `cbcollect_info` dump (a single node, or a folder of node
folders) for the issues in `SupportabilityTool.xlsx`. For each MB and node it
does two things:

- Firstly, it checks if the node's version is affected. Each MB is reported as
  `⚠ AFFECTED`, `✓ fixed`, `· not listed` or `? unknown`, based on the affected
  versions in the sheet and the fix version taken from the workaround text.
- Then it searches the relevant log files for that MB's signatures, and reports
  the matching lines as evidence with a confidence of `🔴 HIGH`, `🟠 MEDIUM` or
  `🟡 LOW`, depending on how strong the evidence is. For MBs that can't be seen
  in the logs (e.g. SUPP-69, a Workbench display issue) it prints the manual
  check to carry out.

The detectors only report an MB when its defining signal is present. As an
example, SUPP-32 is only reported when a TLS/cert refresh comes before the
driver's `exit status 33`.

To use it, from `namTooling/`:

```bash
pip install -r cbKnownIssueAnalyzer/requirements.txt

# whole cluster dump, with JSON + Markdown outputs for the ticket
python3 cbKnownIssueAnalyzer/analyse.py --logs /path/to/ClusterLogs \
    --xlsx SupportabilityTool.xlsx --json findings.json --report findings.md

# just a couple of MBs, faster
python3 cbKnownIssueAnalyzer/analyse.py --logs /path/to/node_dump --only SUPP-32,SUPP-30

# skip MBs the sheet says are fixed / not listed for that version
python3 cbKnownIssueAnalyzer/analyse.py --logs /path/to/ClusterLogs --strict-version
```

`--xlsx` can be left out if the workbook is in or above the `--logs` folder.

As an example, this is the bundled Capella node (7.6.3), which takes about 11
seconds:

```bash
python3 cbKnownIssueAnalyzer/analyse.py --logs cbasSupportability/RedactedClusterLogs_66552 \
    --xlsx SupportabilityTool.xlsx --only SUPP-32,SUPP-30
```

```
  ► SUPP-32 (MB-64968)   confidence: 🔴 HIGH
      Version : ⚠ AFFECTED — within an affected 7.6.x line (no fix patch documented for this series)
      • [verification] analytics_driver_exit_33: 66 match(es)
      • [trigger] tls_refresh_driver_restart: 99 match(es)
            redacted-ns_server.analytics_info.log:24: 2025-01-03T00:55:33.207+00:00 WARN CBAS.cbas tls config has been refreshed by ns server
            redacted-ns_server.analytics_info.log:211: 2025-01-03T00:55:49.377+00:00 WARN CBAS.cbas restarting driver due to encryption setting change
  …
  SUPP-30 / MB-64441: …
      not detected on any node
```

To cover a new issue, add a row to the workbook and then a matching
`Detector` in `cbKnownIssueAnalyzer/detectors.py`, built from real log lines.
Rows in the workbook without a detector are listed as "catalogued, no detector
implemented yet".

Full details are in [`cbKnownIssueAnalyzer/README.md`](cbKnownIssueAnalyzer/README.md).

---

## CBAS_Logs_Timeline.html

The timeline merges a node's five Analytics log files
(`ns_server.analytics_trace / debug / info / warn / error.log`, including the
redacted names) into one view ordered by time. The files are parsed in the
browser and aren't uploaded anywhere. It shows:

- a density track for each source file, and a volume chart where a range can be
  selected by dragging, which narrows down the list;
- severity toggles, a From/To time range and a search box;
- a breakdown of recurring messages, where any message can be plotted over
  time;
- a virtualised list, so very large logs stay quick to scroll.

To use it, open the file in a browser (double click, or
`open CBAS_Logs_Timeline.html`), and drag the node's `ns_server.analytics_*.log`
files onto the drop zone, or click the drop zone to pick them.

Some examples:

- Driver restarts: search for `restarting driver` or `exit status`, and compare
  the WARN/ERROR lines around the same timestamp in the other files.
- Ingestion problems: search for `rollback` or `DCP`, and use the message
  breakdown to see if a message happens once or keeps repeating (which is what
  SUPP-86 looks like).
- After a cbasLoad run: search for the run's `cbasload-<run-id>` client context
  IDs, or set the From/To range to the times of the chaos events in the cbasLoad
  report.

It loads one node's files at a time. `General_Logs_Timeline.html`
(`ns_server.debug/info/error.log` and `diag.log`) and `FTS_Logs_Timeline.html`
(`fts.log`) work the same way, and the FTS version can also take a whole
cbcollect folder and split the timeline by node. The charts need internet
access for Chart.js.

---

## SupportabilityTool.xlsx and the test bundles

`SupportabilityTool.xlsx` is where the known Analytics issues are kept, with the
Supportability ID, MB, summary, affected versions, symptoms, triggers,
verification steps and workarounds for each. It currently covers SUPP-30, 32,
33, 42, 69, 83, 86, 121, 129 and 170. The analyser reads it directly (columns
are found by their header name, so the layout can change), and the cbasLoad
presets refer to its IDs.

The bundles in `cbasSupportability/` are used to check the analyser still gives
the right answers:

- `RedactedClusterLogs_65973` (11 nodes, 7.2.5) should show SUPP-30 as MEDIUM
  on nodes 4 to 7 only, and SUPP-32 as not classified, as the driver crashes
  there without a cert refresh before them.
- `RedactedClusterLogs_66552` (1 Capella node, 7.6.3) should show SUPP-32 as
  HIGH and SUPP-30 as not detected.
