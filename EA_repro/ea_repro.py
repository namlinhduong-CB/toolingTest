#!/usr/bin/env python3
"""
ea_repro.py - builds a Couchbase Enterprise Analytics + MinIO repro environment.

Written for Couchbase Technical Support Engineers. It stands up a disposable
Enterprise Analytics (EA) repro, backed by MinIO (S3-compatible) object storage:

  * MinIO provides two buckets:
      - the EA storage bucket   (EA's own columnar blob store, which EA requires)
      - the EA data bucket      (N generated JSON docs that EA reads through an
                                 S3 external link and external collection)
  * An EA cluster of 1 to N nodes, provisioned end to end, with an external
    collection over the data bucket and a COUNT(*) at the end to verify it.

The cluster sizing (nodes, memory quotas, storage partitions, per-container
CPU/RAM limits) and the S3 dataset (total documents, docs per object, per-doc
padding) can all be adjusted with flags.

Dependencies are Docker with Docker Compose v2, Python 3.9+ and boto3 (see
requirements.txt). No other third-party packages are used.

Typical use:

    ./ea_repro.py up                     # default sizing, 10k docs
    ./ea_repro.py up --nodes 2 --analytics-ramsize 4096 --documents 500000
    ./ea_repro.py status
    ./ea_repro.py query 'SELECT VALUE COUNT(*) FROM ext_data;'
    ./ea_repro.py load --documents 250000        # reshape the dataset
    ./ea_repro.py down --purge                   # tear down and delete volumes

Run `./ea_repro.py <command> -h` for per-command options.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict, field
from pathlib import Path

try:
    import boto3
    from botocore.client import Config as BotoConfig
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "ERROR: boto3 is required. Install with:\n    pip install -r requirements.txt\n"
    )
    sys.exit(2)


# --------------------------------------------------------------------------- #
# Constants / paths
# --------------------------------------------------------------------------- #

# When frozen by PyInstaller, __file__ sits in a temporary extraction folder, so
# we write the state and compose files next to the executable to keep them
# between runs.
if getattr(sys, "frozen", False):
    HERE = Path(sys.executable).resolve().parent
else:
    HERE = Path(__file__).resolve().parent
STATE_FILE = HERE / ".ea_repro_state.json"
COMPOSE_FILE = HERE / "docker-compose.generated.yml"

EA_IMAGE_DEFAULT = "couchbase/enterprise-analytics:latest"
MINIO_IMAGE_DEFAULT = "minio/minio:latest"

# Ports inside the containers. These are the product ports, so do not change them.
EA_UI_PORT = 8091
EA_QUERY_PORT = 8095
MINIO_API_PORT = 9000
MINIO_CONSOLE_PORT = 9001

CBAS_CLI = "/opt/enterprise-analytics/bin/couchbase-cli"


# --------------------------------------------------------------------------- #
# Small terminal helpers
# --------------------------------------------------------------------------- #

def _c(code: str, s: str) -> str:
    if not sys.stdout.isatty():
        return s
    return f"\033[{code}m{s}\033[0m"


def info(msg: str) -> None:
    print(f"{_c('36', '›')} {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"{_c('32', '✓')} {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"{_c('33', '!')} {msg}", flush=True)


def die(msg: str, code: int = 1) -> "None":
    print(f"{_c('31', '✗')} {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def step(msg: str) -> None:
    print(f"\n{_c('1;35', '■')} {_c('1', msg)}", flush=True)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

@dataclass
class Config:
    # project name, which keeps environments apart
    project: str = "ea-repro"

    # images
    ea_image: str = EA_IMAGE_DEFAULT
    minio_image: str = MINIO_IMAGE_DEFAULT

    # cluster sizing / provisioning
    nodes: int = 1
    analytics_ramsize: int = 2048      # cbas (analytics) memory quota, MiB
    data_ramsize: int = 1024           # kv (data) memory quota, MiB
    partitions: int = 2                # EA storage partitions
    ea_cpus: float = 0.0               # per-EA-container CPU limit (0 = unlimited)
    ea_memory: str = ""                # per-EA-container RAM limit, e.g. "6g" ("" = unlimited)

    # host port bases (node i is exposed on base + (i-1)*10)
    ea_ui_host_base: int = 8091
    ea_query_host_base: int = 8095
    minio_api_host: int = 9000
    minio_console_host: int = 9001

    # credentials
    ea_username: str = "Administrator"
    ea_password: str = "password123"
    minio_user: str = "minioadmin"
    minio_password: str = "minioadmin123"

    # S3 layout
    region: str = "us-east-1"
    storage_bucket: str = "ea-storage"   # EA's own blob store
    storage_prefix: str = "ea/"
    data_bucket: str = "ea-data"         # external dataset EA reads
    data_prefix: str = "data"            # object key prefix (no trailing slash)

    # EA logical objects
    link_name: str = "s3link"
    dataverse: str = "Default"           # database.scope == Default.Default
    collection: str = "ext_data"

    # dataset shape
    documents: int = 10000               # total docs in the data bucket
    docs_per_file: int = 1000            # docs per S3 object (NDJSON)
    payload_bytes: int = 0               # extra padding per doc (0 = none)

    # minio container limits (optional)
    minio_cpus: float = 0.0
    minio_memory: str = ""

    def save(self) -> None:
        STATE_FILE.write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls) -> "Config":
        if not STATE_FILE.exists():
            die("No repro environment found. Run `ea_repro.py up` first.")
        return cls(**json.loads(STATE_FILE.read_text()))

    # derived ----------------------------------------------------------------
    def node_names(self) -> list[str]:
        return [f"ea-node{i}" for i in range(1, self.nodes + 1)]

    def container(self, service: str) -> str:
        return f"{self.project}-{service}"

    def ui_host_port(self, node_idx: int) -> int:
        return self.ea_ui_host_base + (node_idx - 1) * 10

    def query_host_port(self, node_idx: int) -> int:
        return self.ea_query_host_base + (node_idx - 1) * 10

    def node_fqdn(self, name: str) -> str:
        # EA rejects short hostnames when joining a multi-node cluster, so a
        # dotted name is needed. This alias resolves on the compose network.
        return f"{name}.{self.project}.local"

    def minio_endpoint_incluster(self) -> str:
        # The service name 'minio' has no underscores on purpose. MinIO/Go
        # reject a Host header containing '_' with HTTP 400.
        return f"http://minio:{MINIO_API_PORT}"

    def minio_endpoint_host(self) -> str:
        return f"http://localhost:{self.minio_api_host}"


# --------------------------------------------------------------------------- #
# Shell / docker helpers
# --------------------------------------------------------------------------- #

def run(cmd: list[str], *, check: bool = True, capture: bool = False,
        quiet: bool = False) -> subprocess.CompletedProcess:
    if not quiet:
        info("$ " + " ".join(cmd))
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def _port_free(port: int) -> bool:
    """True if we can bind the host port, i.e. nothing else is holding it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def preflight_ports(cfg: Config, strict: bool) -> None:
    """Check the host ports are free, and move the bases on to the next free set.

    EA node i uses <base> + (i-1)*10, so we move the base up until every node's
    port in the range is free, which keeps the offset scheme intact. MinIO only
    needs single ports. Ports already picked are reserved, so the four ranges
    never overlap.
    """
    reserved: set[int] = set()

    def take_base(preferred: int, count: int, step: int, label: str) -> int:
        base = preferred
        while True:
            ports = [base + i * step for i in range(count)]
            if all(p not in reserved and _port_free(p) for p in ports):
                reserved.update(ports)
                if base != preferred:
                    warn(f"host port {preferred} for {label} is in use → "
                         f"using {base} instead")
                return base
            if strict:
                busy = next(p for p in ports if p in reserved or not _port_free(p))
                die(f"host port {busy} for {label} is already in use. Free it, "
                    f"pass a different --*-port, or use --project — or drop "
                    f"--strict-ports to auto-pick.")
            base += 1

    cfg.ea_ui_host_base = take_base(cfg.ea_ui_host_base, cfg.nodes, 10, "EA UI")
    cfg.ea_query_host_base = take_base(cfg.ea_query_host_base, cfg.nodes, 10, "EA query")
    cfg.minio_api_host = take_base(cfg.minio_api_host, 1, 1, "MinIO S3 API")
    cfg.minio_console_host = take_base(cfg.minio_console_host, 1, 1, "MinIO console")


def require_docker() -> None:
    if shutil.which("docker") is None:
        die("`docker` not found on PATH. Install Docker Desktop.")
    r = run(["docker", "info"], check=False, capture=True, quiet=True)
    if r.returncode != 0:
        die("Docker daemon is not reachable. Start Docker Desktop and retry.")
    r = run(["docker", "compose", "version"], check=False, capture=True, quiet=True)
    if r.returncode != 0:
        die("`docker compose` (v2) not available. Update Docker Desktop.")


def compose(cfg: Config, args: list[str], *, check: bool = True,
            capture: bool = False, quiet: bool = False) -> subprocess.CompletedProcess:
    base = ["docker", "compose", "-f", str(COMPOSE_FILE), "-p", cfg.project]
    return run(base + args, check=check, capture=capture, quiet=quiet)


def docker_exec(cfg: Config, service: str, argv: list[str], *,
                check: bool = True, capture: bool = True,
                quiet: bool = False) -> subprocess.CompletedProcess:
    return run(
        ["docker", "exec", cfg.container(service)] + argv,
        check=check, capture=capture, quiet=quiet,
    )


def cli(cfg: Config, service: str, argv: list[str], *,
        check: bool = True, quiet: bool = False) -> subprocess.CompletedProcess:
    """Run couchbase-cli inside an EA node container."""
    full = [CBAS_CLI] + argv + [
        "-c", f"localhost:{EA_UI_PORT}",
        "-u", cfg.ea_username, "-p", cfg.ea_password,
    ]
    return docker_exec(cfg, service, full, check=check, quiet=quiet)


# --------------------------------------------------------------------------- #
# Compose file generation
# --------------------------------------------------------------------------- #

def _limits_block(cpus: float, memory: str, indent: str) -> str:
    lines = []
    if cpus and cpus > 0:
        lines.append(f"{indent}cpus: {cpus}")
    if memory:
        lines.append(f"{indent}mem_limit: {memory}")
    return ("\n" + "\n".join(lines)) if lines else ""


def render_compose(cfg: Config) -> str:
    """Render the docker-compose YAML. Built by hand to avoid a PyYAML dependency."""
    parts: list[str] = []
    parts.append("# Generated by ea_repro.py — do not edit by hand.")
    parts.append(f"# project: {cfg.project}")
    parts.append("name: " + cfg.project)
    parts.append("services:")

    # --- MinIO -------------------------------------------------------------
    parts.append("  minio:")
    parts.append(f"    image: {cfg.minio_image}")
    parts.append(f"    container_name: {cfg.container('minio')}")
    parts.append('    command: server /data --console-address ":9001"')
    parts.append("    environment:")
    parts.append(f"      MINIO_ROOT_USER: {cfg.minio_user}")
    parts.append(f"      MINIO_ROOT_PASSWORD: {cfg.minio_password}")
    parts.append("    ports:")
    parts.append(f'      - "{cfg.minio_api_host}:{MINIO_API_PORT}"')
    parts.append(f'      - "{cfg.minio_console_host}:{MINIO_CONSOLE_PORT}"')
    parts.append("    volumes:")
    parts.append("      - minio-data:/data")
    parts.append("    healthcheck:")
    parts.append('      test: ["CMD", "mc", "ready", "local"]')
    parts.append("      interval: 5s")
    parts.append("      timeout: 3s")
    parts.append("      retries: 30")
    lim = _limits_block(cfg.minio_cpus, cfg.minio_memory, "    ")
    if lim:
        parts.append(lim.lstrip("\n"))

    # --- EA nodes ----------------------------------------------------------
    for i, name in enumerate(cfg.node_names(), start=1):
        parts.append(f"  {name}:")
        parts.append(f"    image: {cfg.ea_image}")
        parts.append(f"    container_name: {cfg.container(name)}")
        # FQDN hostname and a matching network alias, so multi-node joins work.
        parts.append(f"    hostname: {cfg.node_fqdn(name)}")
        if cfg.nodes > 1:
            parts.append("    networks:")
            parts.append("      default:")
            parts.append("        aliases:")
            parts.append(f"          - {cfg.node_fqdn(name)}")
        parts.append("    depends_on:")
        parts.append("      minio:")
        parts.append("        condition: service_healthy")
        parts.append("    environment:")
        # Blob store credentials for EA's own columnar storage. There is no CLI
        # flag for these, so they go in the environment.
        parts.append(f"      AWS_ACCESS_KEY_ID: {cfg.minio_user}")
        parts.append(f"      AWS_SECRET_ACCESS_KEY: {cfg.minio_password}")
        parts.append(f"      AWS_REGION: {cfg.region}")
        parts.append("    ports:")
        parts.append(f'      - "{cfg.ui_host_port(i)}:{EA_UI_PORT}"')
        parts.append(f'      - "{cfg.query_host_port(i)}:{EA_QUERY_PORT}"')
        parts.append("    volumes:")
        parts.append(f"      - {name}-data:/opt/enterprise-analytics/var")
        parts.append("    ulimits:")
        parts.append("      nofile:")
        parts.append("        soft: 40960")
        parts.append("        hard: 40960")
        lim = _limits_block(cfg.ea_cpus, cfg.ea_memory, "    ")
        if lim:
            parts.append(lim.lstrip("\n"))

    # --- volumes -----------------------------------------------------------
    parts.append("volumes:")
    parts.append("  minio-data:")
    for name in cfg.node_names():
        parts.append(f"  {name}-data:")

    return "\n".join(parts) + "\n"


# --------------------------------------------------------------------------- #
# REST helpers (EA management and analytics query)
# --------------------------------------------------------------------------- #

def _basic_auth_header(cfg: Config) -> dict:
    import base64
    token = base64.b64encode(
        f"{cfg.ea_username}:{cfg.ea_password}".encode()
    ).decode()
    return {"Authorization": f"Basic {token}"}


def rest_get(cfg: Config, node_idx: int, path: str, timeout: float = 10.0):
    url = f"http://localhost:{cfg.ui_host_port(node_idx)}{path}"
    req = urllib.request.Request(url, headers=_basic_auth_header(cfg))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode()


def analytics_query(cfg: Config, statement: str, node_idx: int = 1,
                    timeout: float = 600.0) -> dict:
    """Run a SQL++ analytics statement and return the parsed JSON response."""
    url = f"http://localhost:{cfg.query_host_port(node_idx)}/analytics/service"
    data = urllib.parse.urlencode({"statement": statement}).encode()
    headers = _basic_auth_header(cfg)
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"status": "http-error", "errors": [{"msg": f"HTTP {e.code}: {body[:300]}"}]}


def query_or_die(cfg: Config, statement: str, node_idx: int = 1) -> list:
    resp = analytics_query(cfg, statement, node_idx=node_idx)
    if resp.get("status") != "success":
        errs = "; ".join(e.get("msg", str(e)) for e in resp.get("errors", []))
        die(f"Query failed ({resp.get('status')}): {errs}\n  statement: {statement}")
    return resp.get("results", [])


# --------------------------------------------------------------------------- #
# Wait helpers
# --------------------------------------------------------------------------- #

def wait_for(desc: str, fn, *, timeout: float, interval: float = 3.0) -> None:
    info(f"waiting for {desc} (up to {int(timeout)}s)…")
    start = time.time()
    last_err = ""
    while time.time() - start < timeout:
        try:
            if fn():
                ok(f"{desc} ready ({int(time.time() - start)}s)")
                return
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
        time.sleep(interval)
    die(f"timed out waiting for {desc}. Last error: {last_err or 'n/a'}")


def wait_ea_ui(cfg: Config, node_idx: int, timeout: float = 180.0) -> None:
    def check():
        try:
            status, _ = rest_get(cfg, node_idx, "/ui/index.html", timeout=4)
            return status == 200
        except urllib.error.HTTPError as e:
            return e.code in (200, 401)  # up, but may need auth
        except Exception:
            return False
    wait_for(f"EA node{node_idx} web console", check, timeout=timeout)


def wait_cluster_healthy(cfg: Config, timeout: float = 180.0) -> None:
    def check():
        status, body = rest_get(cfg, 1, "/pools/default", timeout=5)
        d = json.loads(body)
        nodes = d.get("nodes", [])
        return bool(nodes) and all(n.get("status") == "healthy" for n in nodes) \
            and len(nodes) == cfg.nodes
    wait_for("cluster healthy", check, timeout=timeout)


def wait_analytics_ready(cfg: Config, timeout: float = 240.0) -> None:
    def check():
        resp = analytics_query(cfg, "SELECT 1 AS ok;", timeout=8)
        return resp.get("status") == "success"
    wait_for("analytics query service", check, timeout=timeout)


# --------------------------------------------------------------------------- #
# S3 / MinIO helpers
# --------------------------------------------------------------------------- #

def s3_client(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint_host(),
        aws_access_key_id=cfg.minio_user,
        aws_secret_access_key=cfg.minio_password,
        region_name=cfg.region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def wait_minio(cfg: Config, timeout: float = 120.0) -> None:
    def check():
        req = urllib.request.Request(
            f"{cfg.minio_endpoint_host()}/minio/health/live"
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            return resp.status == 200
    wait_for("MinIO", check, timeout=timeout)


def ensure_buckets(cfg: Config) -> None:
    s3 = s3_client(cfg)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for b in (cfg.storage_bucket, cfg.data_bucket):
        if b in existing:
            info(f"bucket already exists: {b}")
        else:
            s3.create_bucket(Bucket=b)
            ok(f"created bucket: {b}")


def _make_doc(n: int, payload_bytes: int) -> dict:
    countries = ["US", "UK", "DE", "FR", "JP", "IN", "BR", "AU"]
    doc = {
        "id": n,
        "type": "record",
        "name": f"user_{n}",
        "email": f"user_{n}@example.com",
        "age": 18 + (n % 62),
        "country": countries[n % len(countries)],
        "score": round((n * 7.13) % 100, 2),
        "active": (n % 3 != 0),
        "tags": [f"t{n % 10}", f"g{n % 5}"],
        "created_at": 1_700_000_000 + n,
    }
    if payload_bytes > 0:
        doc["payload"] = "x" * payload_bytes
    return doc


def generate_and_upload(cfg: Config, *, wipe: bool = True) -> int:
    """Generate `cfg.documents` NDJSON docs into the data bucket and return the count."""
    s3 = s3_client(cfg)
    prefix = cfg.data_prefix.strip("/")

    if wipe:
        info(f"clearing s3://{cfg.data_bucket}/{prefix}/ …")
        _delete_prefix(s3, cfg.data_bucket, prefix + "/")

    total = cfg.documents
    per = max(1, cfg.docs_per_file)
    n_files = math.ceil(total / per)
    info(f"generating {total:,} docs across {n_files:,} object(s) "
         f"({per:,}/object, payload={cfg.payload_bytes}B)…")

    written = 0
    t0 = time.time()
    for fi in range(n_files):
        lines = []
        start = fi * per
        end = min(start + per, total)
        for n in range(start, end):
            lines.append(json.dumps(_make_doc(n, cfg.payload_bytes),
                                    separators=(",", ":")))
        body = ("\n".join(lines) + "\n").encode()
        key = f"{prefix}/part-{fi:06d}.json"
        s3.put_object(Bucket=cfg.data_bucket, Key=key, Body=body)
        written += (end - start)
        if n_files >= 10 and (fi + 1) % max(1, n_files // 10) == 0:
            pct = int((fi + 1) / n_files * 100)
            info(f"  upload {pct}%  ({written:,}/{total:,} docs)")
    ok(f"uploaded {written:,} docs to s3://{cfg.data_bucket}/{prefix}/ "
       f"in {time.time() - t0:.1f}s")
    return written


def _delete_prefix(s3, bucket: str, prefix: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})


# --------------------------------------------------------------------------- #
# Provisioning steps
# --------------------------------------------------------------------------- #

def configure_blob_storage(cfg: Config, service: str) -> None:
    """Point an EA node at MinIO as its columnar blob store. This has to run before init."""
    argv = [
        "setting-enterprise-analytics", "--set",
        "--scheme", "s3",
        "--bucket", cfg.storage_bucket,
        "--prefix", cfg.storage_prefix,
        "--region", cfg.region,
        "--endpoint", cfg.minio_endpoint_incluster(),
        "--path-style-addressing", "1",
        "--partitions", str(cfg.partitions),
    ]
    r = cli(cfg, service, argv, check=False)
    out = (r.stdout or "") + (r.stderr or "")
    if "SUCCESS" not in out:
        die(f"blob storage configuration failed on {service}:\n{out.strip()}")
    ok(f"blob storage → s3://{cfg.storage_bucket}/{cfg.storage_prefix} "
       f"({cfg.partitions} partitions)")


def node_init_hostname(cfg: Config, service: str) -> None:
    """Set a node's FQDN hostname, which is needed before it joins a multi-node cluster."""
    r = run([
        "docker", "exec", cfg.container(service), CBAS_CLI, "node-init",
        "--node-init-hostname", cfg.node_fqdn(service),
        "-c", f"localhost:{EA_UI_PORT}",
        "-u", cfg.ea_username, "-p", cfg.ea_password,
    ], check=False, capture=True)
    out = (r.stdout or "") + (r.stderr or "")
    if "SUCCESS" not in out:
        die(f"node-init (hostname) failed on {service}:\n{out.strip()}")
    ok(f"{service} hostname → {cfg.node_fqdn(service)}")


def cluster_init(cfg: Config, service: str) -> None:
    # On Enterprise Analytics the KV/data service only holds metadata and its
    # quota is capped very low (~100MB), so we do not pass --cluster-ramsize.
    # The analytics (cbas) quota is the one that matters for sizing.
    argv = [
        "cluster-init",
        "--cluster-username", cfg.ea_username,
        "--cluster-password", cfg.ea_password,
        "--cluster-analytics-ramsize", str(cfg.analytics_ramsize),
        "--cluster-name", cfg.project,
    ]
    # cluster-init establishes credentials, so -u/-p are not needed here.
    r = run([
        "docker", "exec", cfg.container(service), CBAS_CLI, *argv,
        "-c", f"localhost:{EA_UI_PORT}",
    ], check=False, capture=True)
    out = (r.stdout or "") + (r.stderr or "")
    if "SUCCESS" not in out:
        die(f"cluster-init failed:\n{out.strip()}")
    ok(f"cluster initialized (analytics quota={cfg.analytics_ramsize}MiB)")


def add_nodes_and_rebalance(cfg: Config) -> None:
    """For multi-node clusters, join nodes 2 to N and then rebalance."""
    if cfg.nodes <= 1:
        return
    for name in cfg.node_names()[1:]:
        # Joins have to use HTTPS against the secure admin port (plain HTTP is
        # prohibited), with an FQDN and --no-ssl-verify for the self-signed cert.
        add_url = f"https://{cfg.node_fqdn(name)}:18091"
        argv = [
            "server-add",
            "--server-add", add_url, "--no-ssl-verify",
            "--server-add-username", cfg.ea_username,
            "--server-add-password", cfg.ea_password,
        ]
        r = cli(cfg, "ea-node1", argv, check=False)
        out = (r.stdout or "") + (r.stderr or "")
        if "SUCCESS" not in out and "already" not in out.lower():
            die(f"server-add failed for {name}:\n{out.strip()}")
        ok(f"added {name} to cluster")

    info("rebalancing cluster…")
    r = cli(cfg, "ea-node1", ["rebalance", "--no-progress-bar"], check=False)
    out = (r.stdout or "") + (r.stderr or "")
    if "SUCCESS" not in out:
        die(f"rebalance failed:\n{out.strip()}")
    ok("rebalance complete")


def create_link(cfg: Config) -> None:
    details = {
        "type": "s3",
        "accessKeyId": cfg.minio_user,
        "secretAccessKey": cfg.minio_password,
        "region": cfg.region,
        "serviceEndpoint": cfg.minio_endpoint_incluster(),
        "dataverse": cfg.dataverse,
    }
    r = cli(cfg, "ea-node1", [
        "enterprise-analytics-link-setup", "--create",
        "--name", cfg.link_name,
        "--link-details", json.dumps(details),
    ], check=False, quiet=True)
    out = (r.stdout or "") + (r.stderr or "")
    if "SUCCESS" in out:
        ok(f"S3 link '{cfg.link_name}' → {cfg.minio_endpoint_incluster()}")
    elif "already exists" in out.lower():
        ok(f"S3 link '{cfg.link_name}' already present")
    else:
        die(f"link creation failed:\n{out.strip()}")


def create_external_collection(cfg: Config) -> None:
    fq = f"{cfg.dataverse}.`{cfg.dataverse}`.{cfg.collection}"
    analytics_query(cfg, f"DROP COLLECTION {fq} IF EXISTS;")
    stmt = (
        f"CREATE EXTERNAL COLLECTION {fq} "
        f"ON `{cfg.data_bucket}` AT {cfg.link_name} "
        f'PATH "{cfg.data_prefix.strip("/")}" '
        f'WITH {{"format":"json"}};'
    )
    resp = analytics_query(cfg, stmt)
    if resp.get("status") != "success":
        errs = "; ".join(e.get("msg", str(e)) for e in resp.get("errors", []))
        die(f"external collection creation failed: {errs}")
    ok(f"external collection '{cfg.collection}' → "
       f"s3://{cfg.data_bucket}/{cfg.data_prefix.strip('/')}")


def verify_count(cfg: Config) -> int:
    results = query_or_die(cfg, f"SELECT VALUE COUNT(*) FROM {cfg.collection};")
    return int(results[0]) if results else 0


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_up(cfg: Config, args) -> None:
    require_docker()

    if STATE_FILE.exists() and not args.force:
        warn("An environment state already exists. Existing containers will be "
             "reused/updated. Use `down` first for a clean slate, or `--force`.")

    _validate_sizing(cfg)
    preflight_ports(cfg, strict=getattr(args, "strict_ports", False))

    step("1/8  Rendering docker-compose")
    COMPOSE_FILE.write_text(render_compose(cfg))
    ok(f"wrote {COMPOSE_FILE.name}")
    cfg.save()

    step("2/8  Pulling images & starting containers")
    compose(cfg, ["up", "-d", "--remove-orphans"])

    step("3/8  Waiting for MinIO & creating buckets")
    wait_minio(cfg)
    ensure_buckets(cfg)

    step("4/8  Waiting for EA node(s)")
    for i in range(1, cfg.nodes + 1):
        wait_ea_ui(cfg, i)

    step("5/8  Configuring blob storage & initializing cluster")
    # Blob storage has to be set on every node before that node is initialised.
    for name in cfg.node_names():
        configure_blob_storage(cfg, name)
    # Multi-node clusters need FQDN hostnames on all nodes before they join.
    if cfg.nodes > 1:
        for name in cfg.node_names():
            node_init_hostname(cfg, name)
    cluster_init(cfg, "ea-node1")
    add_nodes_and_rebalance(cfg)
    wait_cluster_healthy(cfg)

    step("6/8  Waiting for analytics service")
    wait_analytics_ready(cfg)

    step("7/8  Generating dataset & uploading to S3")
    uploaded = generate_and_upload(cfg)

    step("8/8  Creating S3 link, external collection & verifying")
    create_link(cfg)
    create_external_collection(cfg)
    counted = verify_count(cfg)

    _print_summary(cfg, uploaded, counted)


def cmd_down(cfg: Config, args) -> None:
    require_docker()
    if not COMPOSE_FILE.exists():
        die("No generated compose file found; nothing to tear down.")
    extra = ["-v"] if args.purge else []
    compose(cfg, ["down", "--remove-orphans"] + extra)
    if args.purge:
        STATE_FILE.unlink(missing_ok=True)
        COMPOSE_FILE.unlink(missing_ok=True)
        ok("environment removed (containers + volumes + state).")
    else:
        ok("containers stopped (volumes kept — use `--purge` to delete data).")


def cmd_status(cfg: Config, args) -> None:
    require_docker()
    step("Containers")
    compose(cfg, ["ps"], check=False)

    step("Cluster")
    try:
        _, body = rest_get(cfg, 1, "/pools/default", timeout=6)
        d = json.loads(body)
        print(f"  name:            {d.get('clusterName')}")
        print(f"  data quota:      {d.get('memoryQuota')} MiB")
        print(f"  analytics quota: {d.get('cbasMemoryQuota')} MiB")
        for n in d.get("nodes", []):
            print(f"  node {n.get('hostname'):<22} "
                  f"services={','.join(n.get('services', []))} "
                  f"status={n.get('status')}")
    except Exception as e:  # noqa: BLE001
        warn(f"could not read cluster state: {e}")

    step("Analytics dataset")
    try:
        resp = analytics_query(cfg, f"SELECT VALUE COUNT(*) FROM {cfg.collection};",
                               timeout=30)
        if resp.get("status") == "success":
            cnt = resp["results"][0] if resp.get("results") else 0
            print(f"  {cfg.collection}: {int(cnt):,} docs "
                  f"(s3://{cfg.data_bucket}/{cfg.data_prefix.strip('/')})")
        else:
            warn(f"collection not queryable yet: "
                 f"{resp.get('errors', [{}])[0].get('msg', resp.get('status'))}")
    except Exception as e:  # noqa: BLE001
        warn(f"could not query collection: {e}")


def cmd_load(cfg: Config, args) -> None:
    """Change the S3 dataset (doc count or size) without rebuilding the cluster."""
    require_docker()
    if args.documents is not None:
        cfg.documents = args.documents
    if args.docs_per_file is not None:
        cfg.docs_per_file = args.docs_per_file
    if args.payload_bytes is not None:
        cfg.payload_bytes = args.payload_bytes
    _validate_dataset(cfg)
    cfg.save()

    wait_minio(cfg)
    uploaded = generate_and_upload(cfg, wipe=not args.append)

    # Make sure the link and collection exist, then count again.
    info("refreshing link & external collection…")
    create_link(cfg)
    create_external_collection(cfg)
    counted = verify_count(cfg)
    ok(f"uploaded {uploaded:,} docs; external collection reports "
       f"{counted:,} docs.")
    if not args.append and counted != cfg.documents:
        warn(f"count ({counted:,}) != requested ({cfg.documents:,}). "
             f"S3 listing may be eventually consistent; re-run `status`.")


def cmd_query(cfg: Config, args) -> None:
    statement = args.statement
    if statement == "-" or statement is None:
        statement = sys.stdin.read()
    resp = analytics_query(cfg, statement, timeout=args.timeout)
    print(json.dumps(resp, indent=2))
    if resp.get("status") != "success":
        sys.exit(1)


def cmd_info(cfg: Config, args) -> None:
    _print_summary(cfg, cfg.documents, None, brief=True)


def _log_targets(cfg: Config, service: str | None) -> list[str]:
    valid = ["minio"] + cfg.node_names()
    if service in (None, "", "all"):
        return valid
    if service in valid:
        return [service]
    die(f"unknown service '{service}'. Choose from: {', '.join(['all'] + valid)}")
    return []  # unreachable


def cmd_logs(cfg: Config, args) -> None:
    require_docker()
    for name in _log_targets(cfg, args.service):
        cname = cfg.container(name)
        r = run(["docker", "logs", "--tail", str(args.tail), cname],
                check=False, capture=True, quiet=True)
        body = ((r.stdout or "") + (r.stderr or "")).rstrip()
        print(f"===== {name}  ({cname}) — last {args.tail} lines =====")
        print(body if body else "(no output / container not found)")
        print()


def cmd_bundle(cfg: Config, args) -> None:
    """Zip the logs, config and cluster state into a support bundle to share."""
    require_docker()
    import zipfile
    import datetime

    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = HERE / f"ea-repro-bundle-{cfg.project}-{ts}.zip"

    def cap(cmd: list[str]) -> str:
        r = run(cmd, check=False, capture=True, quiet=True)
        return ((r.stdout or "") + (r.stderr or "")).rstrip() + "\n"

    info(f"collecting support bundle for project '{cfg.project}'…")
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        # Generated compose file and the state with passwords redacted
        if COMPOSE_FILE.exists():
            z.write(COMPOSE_FILE, "docker-compose.generated.yml")
        if STATE_FILE.exists():
            try:
                st = json.loads(STATE_FILE.read_text())
                for k in ("ea_password", "minio_password"):
                    if k in st:
                        st[k] = "***redacted***"
                z.writestr("state.redacted.json", json.dumps(st, indent=2))
            except Exception:  # noqa: BLE001
                pass
        # Host and Docker overview
        z.writestr("host/docker-version.txt", cap(["docker", "version"]))
        z.writestr("host/docker-ps.txt",
                   cap(["docker", "ps", "-a", "--filter", f"name={cfg.project}-"]))
        z.writestr("host/docker-images.txt",
                   cap(["docker", "images", "couchbase/enterprise-analytics"]))
        z.writestr("host/compose-ps.txt",
                   cap(["docker", "compose", "-f", str(COMPOSE_FILE),
                        "-p", cfg.project, "ps"]) if COMPOSE_FILE.exists() else "")
        # Logs and docker inspect output for each container
        for name in ["minio"] + cfg.node_names():
            cname = cfg.container(name)
            z.writestr(f"logs/{name}.log",
                       cap(["docker", "logs", "--tail", str(args.tail), cname]))
            z.writestr(f"inspect/{name}.json", cap(["docker", "inspect", cname]))
        # EA cluster state (best effort)
        for label, path in [("pools", "/pools"),
                            ("pools-default", "/pools/default"),
                            ("analytics-node-settings",
                             "/settings/analytics")]:
            try:
                _, body = rest_get(cfg, 1, path, timeout=8)
                z.writestr(f"cluster/{label}.json", body)
            except Exception as e:  # noqa: BLE001
                z.writestr(f"cluster/{label}.error.txt", str(e))
        # Analytics dataset count (best effort)
        try:
            resp = analytics_query(
                cfg, f"SELECT VALUE COUNT(*) FROM {cfg.collection};", timeout=20)
            z.writestr("cluster/collection-count.json", json.dumps(resp, indent=2))
        except Exception as e:  # noqa: BLE001
            z.writestr("cluster/collection-count.error.txt", str(e))

    size_kb = out_path.stat().st_size / 1024
    ok(f"bundle written: {out_path}  ({size_kb:.0f} KB)")
    print(f"BUNDLE_PATH={out_path}")


# --------------------------------------------------------------------------- #
# Validation & summary
# --------------------------------------------------------------------------- #

def _validate_dataset(cfg: Config) -> None:
    if cfg.documents < 0:
        die("--documents must be >= 0")
    if cfg.docs_per_file < 1:
        die("--docs-per-file must be >= 1")
    if cfg.payload_bytes < 0:
        die("--payload-bytes must be >= 0")


def _validate_sizing(cfg: Config) -> None:
    _validate_dataset(cfg)
    if cfg.nodes < 1:
        die("--nodes must be >= 1")
    if cfg.analytics_ramsize < 1024:
        die("--analytics-ramsize must be >= 1024 (MiB)")
    if cfg.partitions < 1:
        die("--partitions must be >= 1")
    # Rough check that the container RAM is above the analytics quota plus overhead.
    if cfg.ea_memory:
        gib = _parse_mem_to_mib(cfg.ea_memory)
        need = cfg.analytics_ramsize + 1024
        if gib and gib < need:
            warn(f"--ea-memory {cfg.ea_memory} (~{gib}MiB) is below the sum of "
                 f"quotas + overhead (~{need}MiB); EA may fail to start.")


def _parse_mem_to_mib(s: str) -> int:
    s = s.strip().lower()
    try:
        if s.endswith("g"):
            return int(float(s[:-1]) * 1024)
        if s.endswith("m"):
            return int(float(s[:-1]))
        return int(int(s) / (1024 * 1024))
    except ValueError:
        return 0


def _print_summary(cfg: Config, uploaded, counted, brief: bool = False) -> None:
    print()
    print(_c("1;32", "═" * 66))
    print(_c("1;32", "  Enterprise Analytics repro environment"))
    print(_c("1;32", "═" * 66))
    print(f"  Project name        : {cfg.project}")
    print(f"  EA nodes            : {cfg.nodes}")
    print(f"  Analytics quota     : {cfg.analytics_ramsize} MiB")
    print(f"  Storage partitions  : {cfg.partitions}")
    print()
    print(_c("1", "  Endpoints"))
    print(f"    EA Web Console    : http://localhost:{cfg.ui_host_port(1)}   "
          f"({cfg.ea_username} / {cfg.ea_password})")
    print(f"    EA Query (SQL++)  : http://localhost:{cfg.query_host_port(1)}/analytics/service")
    print(f"    MinIO Console     : http://localhost:{cfg.minio_console_host}   "
          f"({cfg.minio_user} / {cfg.minio_password})")
    print(f"    MinIO S3 API      : {cfg.minio_endpoint_host()}")
    print()
    print(_c("1", "  S3 layout"))
    print(f"    EA blob store     : s3://{cfg.storage_bucket}/{cfg.storage_prefix}")
    print(f"    External data     : s3://{cfg.data_bucket}/{cfg.data_prefix.strip('/')}/")
    print(f"    Link / collection : {cfg.link_name}  →  {cfg.collection}")
    if not brief:
        print()
        print(_c("1", "  Dataset"))
        print(f"    Requested docs    : {cfg.documents:,}")
        if uploaded is not None:
            print(f"    Uploaded docs     : {uploaded:,}")
        if counted is not None:
            mark = "✓" if counted == cfg.documents else "!"
            print(f"    Collection COUNT  : {counted:,}  {mark}")
    print()
    print(_c("1", "  Try it"))
    print(f"    ./ea_repro.py query 'SELECT VALUE COUNT(*) FROM {cfg.collection};'")
    print(f"    ./ea_repro.py query 'SELECT * FROM {cfg.collection} LIMIT 5;'")
    print(f"    ./ea_repro.py status")
    print(_c("1;32", "═" * 66))


# --------------------------------------------------------------------------- #
# Argument parsing
# --------------------------------------------------------------------------- #

def _add_sizing_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("cluster sizing / provisioning")
    g.add_argument("--nodes", type=int, default=1,
                   help="number of EA nodes (default: 1)")
    g.add_argument("--analytics-ramsize", type=int, default=2048,
                   help="analytics (cbas) memory quota MiB, min 1024 (default: 2048)")
    g.add_argument("--partitions", type=int, default=2,
                   help="EA storage partitions (default: 2)")
    g.add_argument("--ea-cpus", type=float, default=0.0,
                   help="CPU limit per EA container (0 = unlimited)")
    g.add_argument("--ea-memory", default="",
                   help="RAM limit per EA container, e.g. 6g (default: unlimited)")

    d = p.add_argument_group("S3 dataset")
    d.add_argument("--documents", type=int, default=10000,
                   help="total documents in the S3 data bucket (default: 10000)")
    d.add_argument("--docs-per-file", type=int, default=1000,
                   help="documents per S3 object / NDJSON file (default: 1000)")
    d.add_argument("--payload-bytes", type=int, default=0,
                   help="extra padding bytes per doc, to inflate size (default: 0)")

    m = p.add_argument_group("images / naming / credentials")
    m.add_argument("--project", default="ea-repro",
                   help="compose project name / container prefix (default: ea-repro)")
    m.add_argument("--ea-image", default=EA_IMAGE_DEFAULT)
    m.add_argument("--minio-image", default=MINIO_IMAGE_DEFAULT)
    m.add_argument("--ea-username", default="Administrator")
    m.add_argument("--ea-password", default="password123")
    m.add_argument("--minio-user", default="minioadmin")
    m.add_argument("--minio-password", default="minioadmin123")
    m.add_argument("--ea-ui-port", type=int, default=8091,
                   help="host port base for node1 web console (default: 8091)")
    m.add_argument("--ea-query-port", type=int, default=8095,
                   help="host port base for node1 query svc (default: 8095)")
    m.add_argument("--minio-api-port", type=int, default=9000)
    m.add_argument("--minio-console-port", type=int, default=9001)


def _cfg_from_up_args(args) -> Config:
    return Config(
        project=args.project,
        ea_image=args.ea_image,
        minio_image=args.minio_image,
        nodes=args.nodes,
        analytics_ramsize=args.analytics_ramsize,
        partitions=args.partitions,
        ea_cpus=args.ea_cpus,
        ea_memory=args.ea_memory,
        ea_ui_host_base=args.ea_ui_port,
        ea_query_host_base=args.ea_query_port,
        minio_api_host=args.minio_api_port,
        minio_console_host=args.minio_console_port,
        ea_username=args.ea_username,
        ea_password=args.ea_password,
        minio_user=args.minio_user,
        minio_password=args.minio_password,
        documents=args.documents,
        docs_per_file=args.docs_per_file,
        payload_bytes=args.payload_bytes,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ea_repro.py",
        description="Stand up a disposable Couchbase Enterprise Analytics + MinIO "
                    "(S3) repro environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)

    up = sub.add_parser("up", help="create and provision the full environment",
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    _add_sizing_args(up)
    up.add_argument("--force", action="store_true",
                    help="carry on even if a previous state exists")
    up.add_argument("--strict-ports", action="store_true",
                    help="fail on a busy host port instead of auto-picking a free one")
    up.set_defaults(func=lambda a: cmd_up(_cfg_from_up_args(a), a))

    down = sub.add_parser("down", help="stop the environment")
    down.add_argument("--purge", action="store_true",
                      help="also delete volumes (all data) and state")
    down.set_defaults(func=lambda a: cmd_down(Config.load(), a))

    st = sub.add_parser("status", help="show the containers, cluster and dataset")
    st.set_defaults(func=lambda a: cmd_status(Config.load(), a))

    ld = sub.add_parser("load", help="regenerate or resize the S3 dataset",
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ld.add_argument("--documents", type=int, default=None)
    ld.add_argument("--docs-per-file", type=int, default=None)
    ld.add_argument("--payload-bytes", type=int, default=None)
    ld.add_argument("--append", action="store_true",
                    help="add to existing objects instead of wiping first")
    ld.set_defaults(func=lambda a: cmd_load(Config.load(), a))

    q = sub.add_parser("query", help="run a SQL++ analytics statement")
    q.add_argument("statement", nargs="?", default="-",
                   help="SQL++ statement (pass '-' or leave it out to read from stdin)")
    q.add_argument("--timeout", type=float, default=600.0)
    q.set_defaults(func=lambda a: cmd_query(Config.load(), a))

    inf = sub.add_parser("info", help="print the endpoints and credentials")
    inf.set_defaults(func=lambda a: cmd_info(Config.load(), a))

    lg = sub.add_parser("logs", help="show container logs (EA nodes / MinIO)")
    lg.add_argument("--service", default="all",
                    help="'all' (default), 'minio', or 'ea-nodeN'")
    lg.add_argument("--tail", type=int, default=200,
                    help="lines per container (default: 200)")
    lg.set_defaults(func=lambda a: cmd_logs(Config.load(), a))

    bd = sub.add_parser("bundle",
                        help="zip the logs, config and cluster state for a ticket")
    bd.add_argument("--tail", type=int, default=2000,
                    help="log lines per container to include (default: 2000)")
    bd.set_defaults(func=lambda a: cmd_bundle(Config.load(), a))

    return p


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except KeyboardInterrupt:
        die("interrupted.", code=130)
    except (ClientError, BotoCoreError) as e:
        die(f"S3/MinIO error: {e}")
    except subprocess.CalledProcessError as e:
        die(f"command failed ({e.returncode}): {' '.join(e.cmd)}")


if __name__ == "__main__":
    main()
