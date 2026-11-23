# EA_repro - Enterprise Analytics + MinIO (S3) repro builder

A single-file Python tool for Couchbase Technical Support Engineers. With one
command it stands up a disposable Couchbase Enterprise Analytics (EA)
environment, backed by MinIO (S3-compatible object storage).

It is there to reproduce customer scenarios quickly and in a repeatable way.
You can adjust the cluster sizing and provisioning, and the number of documents
in the S3 bucket.

You can drive it from the command line or from the local web UI.

```
# Web UI (recommended)
./ea_repro_ui.py                     # opens http://localhost:8500 in your browser

# or the CLI
./ea_repro.py up                                  # 1 node, 10k docs, defaults
./ea_repro.py up --nodes 3 --analytics-ramsize 4096 --documents 1000000
./ea_repro.py status
./ea_repro.py query 'SELECT VALUE COUNT(*) FROM ext_data;'
./ea_repro.py load --documents 250000             # reshape the dataset in place
./ea_repro.py down --purge                         # delete everything
```

## Web UI

`ea_repro_ui.py` is a small local web server that wraps the CLI. It only uses
the Python standard library, so there is nothing extra to install. Run it and a
control panel opens in your browser with the following:

- The Create tab, where you set the nodes, analytics quota, partitions and
  document count, then click *Create & provision*. The logs show up live as it
  builds.
- An Advanced section in the Create tab. Here you can pick the EA image or
  version (a dropdown of the tags you have pulled, e.g. `…:2.1.0`, or type any
  tag) and override the project name and host ports. This is useful if you
  already run Couchbase or MinIO on 8091/8095/9000.
- The Dataset tab, which resizes the S3 dataset (`load`) without rebuilding the
  cluster.
- Lifecycle buttons for *Status* and *Tear down*, with an optional purge
  checkbox.
- An Endpoints panel, once an environment is up. It has *Open MinIO Console*
  and *Open EA Console* buttons, plus links and credentials.
- A Run SQL++ box with a *Run query* button.
- Logs & diagnostics, where you can fetch the EA or MinIO container logs, or
  download a support bundle (a zip of the logs, config and cluster state) to
  attach to a ticket.

The UI uses a soft, low-contrast dark theme.

```
./ea_repro_ui.py                 # http://localhost:8500
./ea_repro_ui.py --port 9500     # different port
./ea_repro_ui.py --no-open       # don't auto-open a browser
```

It is a local control panel for your own machine's Docker, so do not expose it
to a network. Everything it does is one of the `ea_repro.py` commands shown in
its log, and the CLI remains the source of truth.

## Standalone app (macOS)

If you would rather have an app you can double-click, without setting up
Python or pip, you can build a single binary that bundles everything:

```
./build_macos.sh            # produces dist/EA-Repro
```

Then run it, either by double-clicking it in Finder or from a terminal:

```
./dist/EA-Repro             # launches the web UI at http://localhost:8500
./dist/EA-Repro up --documents 50000   # the same binary is also the CLI
```

The binary bundles Python, boto3, the UI and the CLI. The UI calls the binary
again for each command. It writes its state next to itself
(`.ea_repro_state.json`, `docker-compose.generated.yml`).

Notes:
- Docker Desktop is still needed on the machine. The binary bundles the app,
  but not Docker.
- The binary is specific to the architecture of the machine you run
  `build_macos.sh` on (e.g. Apple Silicon `arm64`). Rebuild on an Intel Mac to
  get an `x86_64` binary.
- It is unsigned. If macOS Gatekeeper blocks it (e.g. after copying it to
  another Mac), right-click it and choose *Open* once, or clear the quarantine
  flag with `xattr -dr com.apple.quarantine dist/EA-Repro`.
- `.exe` is Windows-only and cannot be produced from macOS. To get one, build on
  Windows with the same PyInstaller command
  (`pyinstaller --onefile --name EA-Repro … ea_repro_ui.py`).

## What it builds

```
                 docker compose project "ea-repro"
   ┌───────────────────────────────────────────────────────────┐
   │                                                             │
   │   ┌─────────────┐        S3 API (:9000)                    │
   │   │   MinIO      │◄──────────────────────────┐             │
   │   │             │   bucket: ea-storage  ◄──── EA columnar   │
   │   │  (S3 store) │     (EA's OWN blob store)   storage       │
   │   │             │                             │             │
   │   │             │   bucket: ea-data     ◄──── external      │
   │   └─────────────┘     (N generated JSON docs) collection    │
   │        ▲                                       │            │
   │        │ external S3 link (s3link)             │            │
   │   ┌────┴─────────────────────────────────────┴────────┐   │
   │   │  Enterprise Analytics cluster (ea-node1 … ea-nodeN)│   │
   │   │  UI :8091   Query/SQL++ :8095                       │   │
   │   └────────────────────────────────────────────────────┘   │
   └───────────────────────────────────────────────────────────┘
```

MinIO has two roles here, and this is by design.

- Firstly, it is EA's own storage backend. Enterprise Analytics is a columnar
  engine and needs an S3-compatible blob store. The `ea-storage` bucket is
  used for that.
- Secondly, it is an external data source. The `ea-data` bucket holds `N`
  generated JSON documents, which EA reads through an S3 external link and
  external collection (`s3link` to `ext_data`). The `--documents` flag sets the
  size of this set.

Every `up` finishes by running `SELECT VALUE COUNT(*) FROM ext_data;` and
checking that the count matches what you asked for.

## Requirements

- Docker Desktop (Docker Engine + Compose v2). Start it before running the tool.
- Python 3.9+
- boto3, installed with `pip install -r requirements.txt`

The EA image (`couchbase/enterprise-analytics`) is ~2.6 GB, and the first `up`
pulls it. A single EA node wants ~4 GB of container RAM at the default 2048 MiB
analytics quota, so size Docker Desktop accordingly, especially for multi-node
runs.

## Commands

- `up` renders the compose file, starts the containers, creates the buckets and
  points EA's blob storage at MinIO. It then initialises the cluster (and for
  `--nodes > 1`, joins the nodes and rebalances), generates and uploads the
  dataset, creates the link and external collection, and finally verifies the
  count.
- `status` shows the containers, the cluster nodes and quotas, and the live
  `ext_data` document count.
- `query` runs any SQL++ statement against the analytics service, taken as an
  argument or from stdin. It prints the raw JSON response.
- `load` regenerates or resizes the S3 dataset (`--documents`,
  `--docs-per-file`, `--payload-bytes`) without rebuilding the cluster.
  `--append` adds to the dataset instead of wiping it.
- `info` prints the endpoints and credentials for the current environment.
- `logs` prints the container logs (`--service all|minio|ea-nodeN`, `--tail N`).
- `bundle` zips the logs, config and cluster state into a support bundle you
  can share.
- `down` stops the containers. `--purge` also deletes the volumes (all data)
  and the local state.

Run `./ea_repro.py <command> -h` for the full list of flags.

## Sizing and provisioning flags (`up`)

- `--nodes` (default `1`) is the number of EA nodes. With more than one, the
  nodes are joined and rebalanced automatically.
- `--analytics-ramsize` (default `2048`) is the Analytics (cbas) memory quota
  in MiB, with a minimum of 1024. This is the setting that matters for EA
  sizing.
- `--partitions` (default `2`) is the number of EA storage partitions in the
  blob store.
- `--ea-cpus` (default unlimited) is the CPU limit per EA container.
- `--ea-memory` (default unlimited) is the RAM limit per EA container, e.g.
  `6g`.

> The KV/data service on EA only holds metadata and its quota is capped very
> low (~100 MB). As such, there is deliberately no data quota flag, and
> Analytics is the service you size.

## Dataset flags (`up` and `load`)

- `--documents` (default `10000`) is the total number of JSON documents in the
  `ea-data` bucket.
- `--docs-per-file` (default `1000`) is the number of documents per S3 object.
  The files are newline-delimited JSON, so the object count is
  ⌈documents ÷ docs-per-file⌉.
- `--payload-bytes` (default `0`) adds padding bytes to each document, to make
  the objects bigger on disk for repros that depend on size.

Each generated document looks like this:

```json
{"id":42,"type":"record","name":"user_42","email":"user_42@example.com",
 "age":56,"country":"IN","score":99.46,"active":true,"tags":["t2","g2"],
 "created_at":1700000042}
```

## Naming, credentials and ports (`up`)

The defaults, which can all be overridden, are project `ea-repro`; EA
`Administrator` / `password123`; MinIO `minioadmin` / `minioadmin123`; node 1
on host ports `8091` (UI) / `8095` (query); MinIO on `9000` (S3) / `9001`
(console). Each additional node is offset by `+10` (node 2 gets 8101/8105, and
so on). You can change the bases with `--ea-ui-port`, `--ea-query-port`,
`--minio-api-port` and `--minio-console-port`, and the credentials with
`--ea-username/--ea-password/--minio-user/--minio-password`.

To run several separate environments at once, give each one a different
`--project` and ports that do not overlap.

## Examples

```bash
# Big external dataset, larger analytics quota
./ea_repro.py up --documents 2000000 --docs-per-file 5000 --analytics-ramsize 6144

# 3-node cluster with per-container limits
./ea_repro.py up --nodes 3 --ea-memory 4g --ea-cpus 2

# Fat documents (each ~2 KB) to stress object size
./ea_repro.py up --documents 100000 --payload-bytes 1800

# Ad-hoc analysis
./ea_repro.py query 'SELECT country, COUNT(*) n FROM ext_data GROUP BY country ORDER BY n DESC;'
echo 'SELECT AVG(age) FROM ext_data;' | ./ea_repro.py query -

# Resize the dataset without touching the cluster
./ea_repro.py load --documents 50000
```

## How it works, and the gotchas it handles

This was tested against `couchbase/enterprise-analytics` 2.1.1 and
`minio/minio` (RELEASE.2025-09-07). The tool follows the provisioning order
and workarounds below.

- Hostname without underscores. MinIO/Go reject an S3 request whose `Host`
  header contains `_` with HTTP 400. As such, the MinIO service is named
  `minio` (hyphens only), and EA talks to `http://minio:9000`.
- Blob store credentials through the environment. The blob storage CLI has no
  key or secret flags, so EA reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  from the container environment. The compose file sets these to the MinIO root
  credentials.
- The order matters. For each node, `setting-enterprise-analytics --set` (which
  points EA at MinIO and runs a live PutObject connectivity test) has to run
  before `cluster-init`. Note however, `cluster-init` rejects `--services` on EA
  and sizes cbas with `--cluster-analytics-ramsize`.
- Multi-node joins. EA needs FQDN hostnames (short names are rejected) and
  HTTPS joins (plain HTTP is prohibited). Each node gets a
  `<name>.<project>.local` alias, `node-init` sets each hostname, and
  `server-add` targets `https://…:18091 --no-ssl-verify`. Then we run
  `rebalance`.
- External data. The S3 link is created with
  `couchbase-cli enterprise-analytics-link-setup`, because the SQL++
  `CREATE LINK` form with a dotted name does not parse. The external collection
  is created with SQL++ `CREATE EXTERNAL COLLECTION … WITH {"format":"json"}`
  (newline-delimited JSON, several docs per object).

External collections are schema-on-read, so `COUNT(*)` scans the bucket again
at query time and `load` changes show up straight away.

## Files

- `ea_repro.py` is the CLI tool (standard library and boto3 only).
- `ea_repro_ui.py` is the local web UI that wraps the CLI (standard library
  only). It is also the entrypoint for the frozen app.
- `build_macos.sh` builds `dist/EA-Repro`, the standalone macOS binary.
- `EA-Repro.spec` is the PyInstaller build spec (generated).
- `requirements.txt` lists `boto3`.
- `docker-compose.generated.yml` is written by `up` and deleted by
  `down --purge`.
- `.ea_repro_state.json` holds the saved config for the running environment
  and is deleted by `down --purge`.

## Troubleshooting

- `Docker daemon is not reachable`: start Docker Desktop and try again.
- The EA container exits or will not initialise: Docker Desktop most likely has
  too little RAM for the analytics quota. Lower `--analytics-ramsize` or give
  Docker more memory.
- Port already in use: `up` checks the host ports first and moves to the next
  free ones automatically (e.g. 8091 becomes 8098 when Couchbase Server is
  already running on 8091-8097), and prints a notice. The ports actually used
  are shown in the summary, the endpoints panel and `status`. Pass
  `--strict-ports` to fail instead, or set explicit `--*-port` bases or a
  different `--project`.
- The count does not match what you asked for after `load`: S3 listing can lag
  behind, so run `status` again.
