# Couchbase Server Known Issue Log Analyser

A Python 3 tool that scans a `cbcollect_info` dump and reports which known issues
(from a support Excel catalogue such as `SupportabilityTool.xlsx`) show up in the
logs. For each MB it checks the version first, then looks for log evidence, and
reports per node with the evidence and a confidence level.

It covers every MB currently in `SupportabilityTool.xlsx`:

- SUPP-30 / MB-64441: Analytics can't read compressed values over DCP, which leads to KV memory pressure or OOM.
- SUPP-32 / MB-64968: A certificate/TLS refresh restarts the Analytics driver (exit status 33).
- SUPP-33 / MB-63876: Bucket recreated with a different vbucket count, causing a rollback crash or the connect link to fail.
- SUPP-42 / MB-61581: A KV/Data outage leaves the Analytics Service unusable for a prolonged period.
- SUPP-69 / MB-65767: The Analytics Workbench can't display integers > 2^53 (UI only, manual check).
- SUPP-83 / MB-62633: A rollback disconnect race leaves ingestion stuck (thread stuck in connector.stop).
- SUPP-86 / MB-67367: Perpetual rollback cycle between the Analytics and Data Services.
- SUPP-121 / MB-67050: Invalid pinCount −1 from an interrupted query (read path), leading to query failures.
- SUPP-129 / MB-62713: Invalid pinCount −1 via a double unpin on the B-Tree search path.
- SUPP-170 / MB-72604: Enterprise Analytics, misaligned `.dic` cloud writes halt the JVM (exit 3).

To extend coverage, add a new row to the workbook and a matching detector. The
engine itself doesn't need to change.

## Two phases: version first, then verification

For every MB and every node the tool reports two steps.

Firstly, STEP 1 checks version applicability, i.e. whether the node's Couchbase
Server version is one where this MB applies. Each MB is marked `⚠ AFFECTED`,
`✓ fixed`, `· not listed` or `? unknown`, along with the reason. AFFECTED is
decided from the sheet's Affected Versions and also a best-effort fix version
parsed from the Workarounds text. As such, a version older than the documented
fix in its release series is flagged AFFECTED even if it isn't explicitly
listed (e.g. 7.2.5 against a fix in 7.2.7).

Then STEP 2 does the verification. It scans the logs for that MB's documented
verification signatures and reports the evidence and confidence. When an MB is
AFFECTED by version but has no log fingerprint (e.g. SUPP-69, which is a
workbench-only bug), the tool prints the manual verification step to run.

Use `--strict-version` to skip the log scan for MBs documented as fixed or not
listed on a node. This is faster, but relies on the sheet's versions being
complete. By default every MB with a detector is verified regardless, because
affected-version lists are often incomplete.

## Quick start

```bash
python3 -m pip install -r requirements.txt      # just openpyxl

# Single-node dump:
python3 analyse.py --logs /path/to/cbcollect_dir --xlsx /path/to/SupportabilityTool.xlsx

# Whole-cluster dump (a directory containing per-node folders):
python3 analyse.py --logs /path/to/ClusterLogs --xlsx /path/to/SupportabilityTool.xlsx \
    --json out.json --report out.md

# Only verify MBs whose version could apply to each node:
python3 analyse.py --logs /path/to/ClusterLogs --strict-version
```

If `SupportabilityTool.xlsx` (or `Book1.xlsx` / `known_issues.xlsx`) sits in or
above the `--logs` directory, you can leave out `--xlsx` and it will be picked
up automatically.

### Options

- `--logs, -l`: the cbcollect dump, either one node or a folder of node folders (required).
- `--xlsx, -x`: the known-issues workbook. If omitted, it is looked for near `--logs`.
- `--only`: comma-separated Supportability IDs to check (e.g. `SUPP-32`).
- `--strict-version`: skip the log scan for MBs documented as fixed or not listed on each node.
- `--json FILE`: write the full results as JSON.
- `--report FILE`: write a Markdown report.
- `--max-file-mb N`: skip files larger than N MB (0 means no limit, the default is 0).
- `--max-samples N`: evidence lines kept per signal (default 4).
- `--verbose, -v`: print each file as it is scanned.

## What it does

- Firstly, it loads the catalogue from the Excel workbook (`known_issues.py`).
  Columns are matched by header name (Supportability ID, MB ID, Summary,
  Affected Versions, Symptoms, Triggers, Verification, Workarounds), so the
  layout can change without breaking the loader.
- Secondly, it finds the nodes under `--logs` (a single dump, a folder of
  `cbcollect_info_ns_1@host_…` folders, or dumps nested one level down in a
  `redacted/` subfolder) and detects each node's version.
- Then it scans only the relevant log files for each issue's signals
  (`detectors.py`) and collects the matching lines as evidence. Large logs are
  still quick to scan, because a lowercase substring check runs before each
  regex.
- Finally, it reports to the console, and optionally to JSON or Markdown, with
  per-node findings and evidence plus a cluster summary.

## How matching works

The workbook's Symptoms and Verification text is prose, and the sheet itself
says it "will not be exact, but similar", so we can't grep it as-is. Instead the
log signatures we match on live in `detectors.py`, keyed by Supportability ID.
Each detector is a set of signals, and each signal has the following.

- `file_globs`: which log files to look in (basename globs).
- `patterns`: regexes, each paired with a `hint` substring that is checked first
  to rule lines out quickly, and an optional `exclude` regex to drop lines that
  look similar but aren't relevant.
- `kind`: how strong a match is. This is one of:
  - `verification`, i.e. specific, near-definitive proof (e.g. the Analytics
    driver's `exit status 33`).
  - `trigger`, the event that provokes the issue (e.g. `tls config has been refreshed`).
  - `symptom`, an effect further down the line (e.g. Eventing `getOpCallback …
    Bucket fetch failed … unambiguous timeout`).
  - `hint`, which is weak and informational (e.g. a `kv_dcp_*` metric name is
    present, but someone still needs to compare the values).

A detector can also declare `requires`, i.e. signal names that must fire for the
issue to be classified at all. SUPP-32 requires its cert-refresh trigger. The
Analytics driver's `exit status 33` is only the SUPP-32 signature when a cert
refresh caused it, so if we see exit 33 without the trigger it is reported as an
`ⓘ … not classified` note (with a pointer to the more likely cause) rather than
as SUPP-32.

### Confidence

Confidence is based on the strength of the log evidence only.

- Any `verification` signal fired gives `🔴 HIGH`.
- A `trigger` and a `symptom` together give `🔴 HIGH`.
- A `symptom` or a `trigger` on its own gives `🟠 MEDIUM`.
- Only `hint` signals give `🟡 LOW`.
- Nothing fired gives `— none`.

Whether the node's version is in the documented "Affected Versions" list is
reported separately, and we leave it out of the confidence level on purpose.
Those lists are often incomplete. For example, the validation cluster below runs
7.2.5, which is listed in neither ticket, yet it clearly hits both issues.

## Validated results

Validated against the datasets under `cbasSupportability/`.

### `RedactedClusterLogs_65973`, an 11-node on-prem cluster on 7.2.5

This holds `Book1.xlsx` and the eleven `cbcollect_info_ns_1@…` node folders.

- SUPP-30 comes out MEDIUM on nodes 4 to 7 only, from Eventing `getOpCallback …
  unambiguous timeout` and `KV_TEMPORARY_FAILURE` in `eventing.log`. Nodes 1 to
  3 and 8 to 11 do not fire SUPP-30, which shows the tool picks out the
  affected nodes rather than flagging all of them.
- SUPP-32 is not classified. The Analytics driver is crashing here (around 770
  `exit_code:1` events across the cluster, plus 11 stray `exit_code:33`), but
  there is no TLS/cert-refresh trigger anywhere in the logs, and SUPP-32 is
  defined by a cert refresh causing the restart. The tool prints a note rather
  than silently dropping the exit-33 events, and points the analyst at SUPP-30,
  since the exit-status-1 crash loop is consistent with memory pressure.

This dataset is the reason the SUPP-32 detector requires the cert-refresh
trigger. An earlier build keyed on the generic `event_id:5121` ("Analytics
Process Crashed") event and wrongly flagged SUPP-32 HIGH on all 11 nodes.

The scan takes about 90 s for the full 11-node cluster.

### `RedactedClusterLogs_66552`, a single Capella node on 7.6.3

This has no workbook of its own, so run it with
`--xlsx ../RedactedClusterLogs_65973/Book1.xlsx`.

- SUPP-32 comes out HIGH. The trigger appears as the raw driver log lines in
  `analytics_info.log` (`tls config has been refreshed by ns server`,
  `restarting driver due to encryption setting change`, `immediate restart
  driver request rec'd`), and every crash in `diag.log` is `exit_code:33` (33
  events).
- SUPP-30 is not detected.

The scan takes about 12 s.

## Adding a new known issue

- Firstly, add the row to the workbook. It must have a Supportability ID.
- Then add a `Detector(supp_id="SUPP-NN", signals=[...])` to `DETECTORS` in
  `detectors.py`. Pick concrete regexes and `hint`s from real log lines, and tag
  each signal as `verification`, `trigger`, `symptom` or `hint`.
- Finally, run the tool. Rows in the workbook without a detector are listed as
  "catalogued, no detector implemented yet", so nothing is silently ignored.

## Files

- `analyse.py`: the CLI, node discovery, version detection, scan engine and report.
- `detectors.py`: the detection rules (signals per Supportability ID).
- `known_issues.py`: the Excel catalogue loader, plus version parsing and matching.
- `requirements.txt`: `openpyxl`.

## Notes and limitations

- The `kv_dcp_*` metrics for SUPP-30 verification are treated as hints. A
  cbcollect's text logs usually only contain Prometheus scrape-debug lines,
  which only name the series, and those are filtered out. Confirm the real
  metric values from the stats UI or a metrics snapshot, i.e.
  `kv_dcp_total_data_size_bytes` against
  `kv_dcp_total_uncompressed_data_size_bytes`, and an elevated
  `kv_dcp_ready_queue_size_bytes` for the `cbas` connection.
- Evidence lines are trimmed to about 240 characters. Raw user data stays wrapped
  in the cbcollect `<ud>…</ud>` redaction tags.
