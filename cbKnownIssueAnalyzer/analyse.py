#!/usr/bin/env python3
"""
Couchbase Server Known-Issue Log Analyser
=========================================

Scans a cbcollect_info dump (a single node, or a directory of per-node folders)
for the known issues catalogued in a support Excel workbook (e.g. Book1.xlsx),
then reports which issues show up on which nodes, along with the evidence.

    python3 analyse.py --logs /path/to/cbcollect_dir --xlsx /path/to/Book1.xlsx

The workbook holds the catalogue itself (summary, affected versions, symptoms,
triggers, verification steps and workarounds). The log signatures we match on
live in detectors.py, keyed by Supportability ID.

Output:
    - a console report (default)
    - optionally a JSON file      (--json out.json)
    - optionally a Markdown report (--report out.md)
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import detectors as det
from known_issues import KnownIssue, Version, load_known_issues, parse_version


# ---------------------------------------------------------------------------
# Data structures for results
# ---------------------------------------------------------------------------

@dataclass
class SignalHit:
    name: str
    kind: str
    count: int = 0
    samples: List[Tuple[str, int, str]] = field(default_factory=list)  # (file, lineno, text)
    _seen: set = field(default_factory=set, repr=False)  # (file, lineno) pairs already counted


@dataclass
class IssueResult:
    supp_id: str
    fired_signals: Dict[str, SignalHit] = field(default_factory=dict)
    confidence: str = det.CONF_NONE
    version_affected: Optional[bool] = None
    version_status: str = "UNKNOWN"   # AFFECTED | FIXED | NOT_LISTED | UNKNOWN
    version_reason: str = ""
    scanned: bool = True              # False if --strict-version skipped the scan
    gated_note: Optional[str] = None  # set when a `requires` gate dropped a partial match

    @property
    def fired(self) -> bool:
        return bool(self.fired_signals)


@dataclass
class NodeResult:
    label: str
    path: str
    version: Optional[Version] = None
    issues: Dict[str, IssueResult] = field(default_factory=dict)
    scanned_files: int = 0
    skipped_files: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Node discovery
# ---------------------------------------------------------------------------

_NODE_MARKERS = ("*diag.log", "*couchbase.log", "*ns_server.info.log", "*ns_server.debug.log")


def _is_node_dir(d: Path) -> bool:
    for marker in _NODE_MARKERS:
        if any(d.glob(marker)):
            return True
    return False


def _node_label(d: Path) -> str:
    """Work out a readable node label from the folder path.

    cbcollect folders look like:
        cbcollect_info_ns_1@HOST.domain_20241218-191539
    The dump can also sit in a 'redacted' subfolder, in which case we check the
    parent folder name as well.
    """
    import re

    for candidate in (d.name, d.parent.name):
        m = re.search(r"cbcollect_info[^@]*@([^_/]+)", candidate)
        if m:
            return m.group(1)
    # otherwise fall back to whichever folder name says the most
    if d.name.lower() in ("redacted", "."):
        return d.parent.name or d.name
    return d.name


def discover_nodes(root: Path) -> List[Path]:
    """Return the list of node directories under `root`.

    This covers root being a node itself, root containing node folders, and node
    dumps nested one level down in a 'redacted' (or similar) subfolder.
    """
    root = root.resolve()
    nodes: List[Path] = []

    if _is_node_dir(root):
        nodes.append(root)

    for child in sorted(p for p in root.iterdir() if p.is_dir()):
        if _is_node_dir(child):
            nodes.append(child)
        else:
            # look one level deeper (e.g. folder-XXX/redacted/*.log)
            for grand in sorted(p for p in child.iterdir() if p.is_dir()):
                if _is_node_dir(grand):
                    nodes.append(grand)

    # de-duplicate, keeping the original order
    seen = set()
    unique = []
    for n in nodes:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


# ---------------------------------------------------------------------------
# Version detection
# ---------------------------------------------------------------------------

_VERSION_SCAN_BYTES = 8 * 1024 * 1024  # the version is near the top of the file


def detect_version(node_dir: Path) -> Optional[Version]:
    # Prefer the explicit ns_server line, i.e. "web start ok ... Version: "X.Y.Z-..."".
    for glob in ("*diag.log", "*couchbase.log", "*ns_server.info.log"):
        for f in node_dir.glob(glob):
            v = _scan_version_in_file(f)
            if v:
                return v
    return None


def _scan_version_in_file(path: Path) -> Optional[Version]:
    import re

    explicit = re.compile(rb'Version:\s*"(\d+\.\d+\.\d+-\d+-\w+)"')
    generic = re.compile(rb"(\d+\.\d+\.\d+-\d+-enterprise)")
    try:
        with path.open("rb") as fh:
            chunk = fh.read(_VERSION_SCAN_BYTES)
    except OSError:
        return None
    m = explicit.search(chunk) or generic.search(chunk)
    if m:
        return parse_version(m.group(1).decode("ascii", "ignore"))
    return None


# ---------------------------------------------------------------------------
# Scan engine
# ---------------------------------------------------------------------------

def _basename_matches(basename: str, globs: List[str]) -> bool:
    low = basename.lower()
    return any(fnmatch.fnmatch(low, g.lower()) for g in globs)


@dataclass
class _FilePattern:
    supp_id: str
    signal: det.Signal
    pattern: det.Pattern
    hint_bytes: bytes


def _build_file_plan(node_dir: Path, active: Dict[str, det.Detector]
                     ) -> Dict[Path, List[_FilePattern]]:
    """Map each file that needs scanning to the patterns we apply to it."""
    plan: Dict[Path, List[_FilePattern]] = {}
    files = [p for p in node_dir.iterdir() if p.is_file()]
    for supp_id, detector in active.items():
        for signal in detector.signals:
            for f in files:
                if not _basename_matches(f.name, signal.file_globs):
                    continue
                for pat in signal.patterns:
                    plan.setdefault(f, []).append(
                        _FilePattern(
                            supp_id=supp_id,
                            signal=signal,
                            pattern=pat,
                            hint_bytes=pat.hint.lower().encode("utf-8", "ignore"),
                        )
                    )
    return plan


def _trim(text: str, limit: int = 240) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[:limit] + " …"


def scan_node(node_dir: Path,
              issues: Dict[str, KnownIssue],
              active: Dict[str, det.Detector],
              max_file_mb: int = 0,
              max_samples: int = 4,
              verbose: bool = False,
              strict_version: bool = False) -> NodeResult:
    label = _node_label(node_dir)
    version = detect_version(node_dir)
    result = NodeResult(label=label, path=str(node_dir), version=version)

    # Phase 1, version first. For every active detector, decide whether the
    # node's version is affected before we look at any log evidence.
    for supp_id in active:
        ir = IssueResult(supp_id=supp_id)
        issue = issues.get(supp_id)
        if issue:
            ir.version_affected = issue.is_version_affected(version)
            ir.version_status, ir.version_reason = issue.version_status(version)
        result.issues[supp_id] = ir

    # Phase 2, verification. Scan the logs. With --strict-version we only run
    # the verification steps for MBs whose version could be affected (AFFECTED
    # or UNKNOWN), and skip those documented as FIXED or NOT_LISTED.
    scan_active = active
    if strict_version:
        scan_active = {
            sid: d for sid, d in active.items()
            if result.issues[sid].version_status in ("AFFECTED", "UNKNOWN")
        }
        for sid, ir in result.issues.items():
            if sid not in scan_active:
                ir.scanned = False

    plan = _build_file_plan(node_dir, scan_active)

    for f, fpats in plan.items():
        if max_file_mb:
            try:
                size_mb = f.stat().st_size / (1024 * 1024)
            except OSError:
                size_mb = 0
            if size_mb > max_file_mb:
                result.skipped_files.append(f"{f.name} ({size_mb:.0f} MB > {max_file_mb} MB)")
                continue

        if verbose:
            print(f"    scanning {f.name} ...", file=sys.stderr)
        result.scanned_files += 1
        _scan_file(f, fpats, result, max_samples)

    # Finalise per issue.
    for supp_id, ir in result.issues.items():
        detector = active[supp_id]
        # A detector can require certain signals, i.e. the condition that
        # defines the issue. If some signals fired but a required one did not,
        # this is not the issue in question, so we record a note and drop the
        # partial match as it would be misleading.
        missing = [s for s in detector.requires if s not in ir.fired_signals]
        if missing and ir.fired_signals:
            seen = ", ".join(f"{n}×{h.count}" for n, h in ir.fired_signals.items())
            ir.gated_note = (detector.requires_note
                             or f"required signal(s) {missing} not found") + f"  (saw: {seen})"
            ir.fired_signals = {}
        # Confidence is based on the log evidence only. The version verdict is
        # set separately above.
        fired_kinds = {hit.kind for hit in ir.fired_signals.values()}
        ir.confidence = det.confidence_for(fired_kinds)

    return result


def _scan_file(path: Path, fpats: List[_FilePattern],
               result: NodeResult, max_samples: int) -> None:
    basename = path.name
    try:
        fh = path.open("rb")
    except OSError:
        return
    with fh:
        for lineno, raw in enumerate(fh, 1):
            low = raw.lower()
            matched: List[_FilePattern] = [
                fp for fp in fpats if fp.hint_bytes and fp.hint_bytes in low
            ]
            if not matched:
                continue
            text = None
            for fp in matched:
                if text is None:
                    text = raw.decode("utf-8", "replace")
                if not fp.pattern.compiled().search(text):
                    continue
                excl = fp.pattern.compiled_exclude()
                if excl is not None and excl.search(text):
                    continue
                _record(result, fp, basename, lineno, text, max_samples)


def _record(result: NodeResult, fp: _FilePattern, basename: str,
            lineno: int, text: str, max_samples: int) -> None:
    ir = result.issues[fp.supp_id]
    hit = ir.fired_signals.get(fp.signal.name)
    if hit is None:
        hit = SignalHit(name=fp.signal.name, kind=fp.signal.kind)
        ir.fired_signals[fp.signal.name] = hit
    # Count each log line once, even if several of a signal's patterns match it.
    key = (basename, lineno)
    if key in hit._seen:
        return
    hit._seen.add(key)
    hit.count += 1
    if len(hit.samples) < max_samples:
        hit.samples.append((basename, lineno, _trim(text)))


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

_CONF_ICON = {
    det.CONF_HIGH: "🔴 HIGH",
    det.CONF_MEDIUM: "🟠 MEDIUM",
    det.CONF_LOW: "🟡 LOW",
    det.CONF_NONE: "— none",
}

# Display order for signal kinds, strongest first.
_KIND_ORDER = {"verification": 0, "trigger": 1, "symptom": 2, "hint": 3}


def _short(text: str, limit: int = 300) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[:limit] + " …"


def print_console_report(nodes: List[NodeResult],
                         issues: Dict[str, KnownIssue],
                         active: Dict[str, det.Detector],
                         uncovered: List[str]) -> None:
    line = "=" * 78
    print(line)
    print("  COUCHBASE SERVER — KNOWN ISSUE LOG ANALYSIS")
    print(line)
    print(f"  Nodes analysed : {len(nodes)}")
    print(f"  Issues checked : {', '.join(sorted(active))}")
    if uncovered:
        print(f"  Catalogued w/o detector : {', '.join(uncovered)}")
    print()

    # Per-node detail
    for node in nodes:
        ver = str(node.version) if node.version else "unknown"
        print("-" * 78)
        print(f"  NODE: {node.label}    (version {ver})")
        print(f"        {node.path}")
        if node.skipped_files:
            print(f"        skipped (size cap): {', '.join(node.skipped_files)}")
        print("-" * 78)

        ordered = sorted(node.issues, key=_supp_key)

        # ---- Phase 1: version first ---------------------------------------
        print(f"  STEP 1 — Version applicability (node version: {ver})")
        for supp_id in ordered:
            ir = node.issues[supp_id]
            issue = issues.get(supp_id)
            mb = issue.mb_id if issue else ""
            specs = ", ".join(s.raw for s in issue.affected_specs()) if issue else ""
            status = _VERSION_STATUS_ICON.get(ir.version_status, ir.version_status)
            skipped = "" if ir.scanned else "  (verification skipped)"
            print(f"      {status:<12} {supp_id:<9} {mb:<10} affected: [{specs}]{skipped}")
            if ir.version_status in ("AFFECTED", "UNKNOWN"):
                print(f"                   └ {ir.version_reason}")

        # ---- Phase 2: verification ----------------------------------------
        print(f"\n  STEP 2 — Verification (log evidence)")
        any_fired = False
        for supp_id in ordered:
            ir = node.issues[supp_id]
            if not ir.fired:
                continue
            any_fired = True
            issue = issues.get(supp_id)
            _print_issue_block(ir, issue)

        # Version is AFFECTED but there's no log evidence, so print the manual
        # check if the detector has one.
        for supp_id in ordered:
            ir = node.issues[supp_id]
            if ir.fired or ir.version_status != "AFFECTED" or not ir.scanned:
                continue
            detector = det.get_detector(supp_id)
            issue = issues.get(supp_id)
            mb = f" ({issue.mb_id})" if issue and issue.mb_id else ""
            if detector and detector.manual_check:
                print(f"\n  → {supp_id}{mb}: version is affected but no log signature exists.")
                print(f"      Manual check: {detector.manual_check}")

        # Also report partial matches that a `requires` gate dropped, so the
        # analyst can see them.
        gated = [(sid, node.issues[sid]) for sid in ordered
                 if node.issues[sid].gated_note]
        for sid, ir in gated:
            print(f"\n  ⓘ {sid} not classified — {ir.gated_note}")

        if not any_fired and not gated:
            print("      No log indicators for any checked known issue on this node.")
        print()

    # Cluster summary matrix
    print("=" * 78)
    print("  CLUSTER SUMMARY  (confidence per node)")
    print("=" * 78)
    for supp_id in sorted(active, key=_supp_key):
        issue = issues.get(supp_id)
        title = _short(issue.summary, 70) if issue else ""
        mb = f" / {issue.mb_id}" if issue and issue.mb_id else ""
        print(f"\n  {supp_id}{mb}: {title}")
        hits = [n for n in nodes if n.issues[supp_id].fired]
        if not hits:
            print("      not detected on any node")
            continue
        for n in hits:
            ir = n.issues[supp_id]
            vs = _VERSION_STATUS_ICON.get(ir.version_status, ir.version_status)
            print(f"      {n.label:<28} {_CONF_ICON[ir.confidence]:<12} [version: {vs}]")
    print()


def _version_note(affected: Optional[bool]) -> str:
    if affected is True:
        return "[version in documented affected list]"
    if affected is False:
        return "[version NOT in documented list — verify manually]"
    return "[version match unknown]"


_VERSION_STATUS_ICON = {
    "AFFECTED": "⚠ AFFECTED",
    "FIXED": "✓ fixed",
    "NOT_LISTED": "· not listed",
    "UNKNOWN": "? unknown",
}


def _supp_key(supp_id: str):
    """Sort SUPP-NN ids by their numeric suffix (SUPP-30 before SUPP-121)."""
    m = re.search(r"(\d+)", supp_id)
    return (int(m.group(1)) if m else 0, supp_id)


def _print_issue_block(ir: IssueResult, issue: Optional[KnownIssue]) -> None:
    mb = f" ({issue.mb_id})" if issue and issue.mb_id else ""
    print(f"\n  ► {ir.supp_id}{mb}   confidence: {_CONF_ICON[ir.confidence]}")
    if issue:
        print(f"      Summary : {_short(issue.summary, 260)}")
    vstatus = _VERSION_STATUS_ICON.get(ir.version_status, ir.version_status)
    print(f"      Version : {vstatus} — {ir.version_reason}")

    # Group the fired signals by kind.
    for name, hit in sorted(ir.fired_signals.items(),
                            key=lambda kv: _KIND_ORDER.get(kv[1].kind, 9)):
        print(f"      • [{hit.kind}] {name}: {hit.count} match(es)")
        for (fname, lineno, text) in hit.samples[:3]:
            print(f"            {fname}:{lineno}: {text}")

    detector = det.get_detector(ir.supp_id)
    if detector and detector.note:
        print(f"      Analyst note : {_short(detector.note, 260)}")
    if issue and issue.workarounds:
        print(f"      Workaround   : {_short(issue.workarounds, 260)}")


# ---------------------------------------------------------------------------
# JSON / Markdown export
# ---------------------------------------------------------------------------

def build_json(nodes: List[NodeResult], issues: Dict[str, KnownIssue]) -> dict:
    out = {"nodes": []}
    for node in nodes:
        node_obj = {
            "label": node.label,
            "path": node.path,
            "version": str(node.version) if node.version else None,
            "scanned_files": node.scanned_files,
            "skipped_files": node.skipped_files,
            # Phase 1: the version verdict for every MB we checked.
            "version_applicability": {
                supp_id: {
                    "status": ir.version_status,
                    "reason": ir.version_reason,
                    "affected_listed": ir.version_affected,
                    "scanned": ir.scanned,
                }
                for supp_id, ir in node.issues.items()
            },
            "issues": {},
        }
        for supp_id, ir in node.issues.items():
            if not ir.fired:
                continue
            node_obj["issues"][supp_id] = {
                "confidence": ir.confidence,
                "version_status": ir.version_status,
                "version_reason": ir.version_reason,
                "version_affected": ir.version_affected,
                "signals": {
                    name: {
                        "kind": hit.kind,
                        "count": hit.count,
                        "samples": [
                            {"file": f, "line": ln, "text": tx}
                            for (f, ln, tx) in hit.samples
                        ],
                    }
                    for name, hit in ir.fired_signals.items()
                },
            }
        out["nodes"].append(node_obj)
    return out


def write_markdown(path: str, nodes: List[NodeResult],
                   issues: Dict[str, KnownIssue],
                   active: Dict[str, det.Detector]) -> None:
    lines: List[str] = []
    lines.append("# Couchbase Server — Known Issue Log Analysis\n")
    lines.append(f"- Nodes analysed: **{len(nodes)}**")
    lines.append(f"- Issues checked: {', '.join(sorted(active))}\n")

    lines.append("## Summary\n")
    lines.append("| Issue | MB | Node | Version | Version status | Confidence |")
    lines.append("|---|---|---|---|---|---|")
    for supp_id in sorted(active, key=_supp_key):
        issue = issues.get(supp_id)
        mb = issue.mb_id if issue else ""
        for n in nodes:
            ir = n.issues[supp_id]
            if not ir.fired:
                continue
            ver = str(n.version) if n.version else "unknown"
            lines.append(
                f"| {supp_id} | {mb} | {n.label} | {ver} | {ir.version_status} "
                f"| {ir.confidence} |"
            )

    for node in nodes:
        lines.append(f"\n## Node: {node.label} (version "
                     f"{node.version or 'unknown'})\n")
        fired = [ir for ir in node.issues.values() if ir.fired]
        if not fired:
            lines.append("_No indicators for any checked known issue._\n")
            continue
        for ir in fired:
            issue = issues.get(ir.supp_id)
            mb = f" ({issue.mb_id})" if issue and issue.mb_id else ""
            lines.append(f"### {ir.supp_id}{mb} — confidence {ir.confidence}\n")
            if issue:
                lines.append(f"{_short(issue.summary, 400)}\n")
            for name, hit in ir.fired_signals.items():
                lines.append(f"- **[{hit.kind}] {name}** — {hit.count} match(es)")
                for (fname, lineno, text) in hit.samples[:3]:
                    lines.append(f"  - `{fname}:{lineno}`: {text}")
            if issue and issue.workarounds:
                lines.append(f"\n**Workaround:** {_short(issue.workarounds, 500)}\n")

    Path(path).write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _default_xlsx_near(logs: Path) -> Optional[Path]:
    for cand in (logs, logs.parent, *logs.parents):
        for name in ("SupportabilityTool.xlsx", "Book1.xlsx", "known_issues.xlsx"):
            p = cand / name
            if p.exists():
                return p
    return None


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Analyse Couchbase Server logs for known issues from an Excel catalogue.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--logs", "-l", required=True,
                    help="Path to a cbcollect_info dump (single node), or a directory of node folders.")
    ap.add_argument("--xlsx", "-x", default=None,
                    help="Path to the known-issues workbook. If omitted, we look for it in or above --logs.")
    ap.add_argument("--only", default=None,
                    help="Comma-separated Supportability IDs to check (default: every ID with a detector).")
    ap.add_argument("--json", default=None, help="Write the full results to this JSON file.")
    ap.add_argument("--report", default=None, help="Write a Markdown report to this file.")
    ap.add_argument("--max-file-mb", type=int, default=0,
                    help="Skip files larger than this many MB (0 = no limit).")
    ap.add_argument("--max-samples", type=int, default=4,
                    help="Maximum number of evidence lines kept per signal.")
    ap.add_argument("--strict-version", action="store_true",
                    help="Only run verification for MBs whose version is AFFECTED or UNKNOWN, "
                         "and skip those documented as FIXED or NOT_LISTED. This is faster, but "
                         "relies on the affected and fix versions in the sheet being complete.")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print files as they are scanned.")
    args = ap.parse_args(argv)

    logs = Path(args.logs).expanduser()
    if not logs.exists():
        print(f"error: --logs path does not exist: {logs}", file=sys.stderr)
        return 2

    xlsx = Path(args.xlsx).expanduser() if args.xlsx else _default_xlsx_near(logs)
    if not xlsx or not xlsx.exists():
        print("error: could not find the known-issues workbook; pass --xlsx explicitly.",
              file=sys.stderr)
        return 2

    issues = load_known_issues(str(xlsx))
    if not issues:
        print(f"error: no known issues loaded from {xlsx}", file=sys.stderr)
        return 2

    # Check the issues that have a detector, filtered by --only if given.
    wanted = None
    if args.only:
        wanted = {s.strip().upper() for s in args.only.split(",") if s.strip()}
    active: Dict[str, det.Detector] = {}
    for supp_id in issues:
        if supp_id not in det.DETECTORS:
            continue
        if wanted and supp_id.upper() not in wanted:
            continue
        active[supp_id] = det.DETECTORS[supp_id]

    uncovered = [sid for sid in issues if sid not in det.DETECTORS]

    if not active:
        print("error: none of the catalogued issues have a detector "
              f"(catalogued: {', '.join(issues)}).", file=sys.stderr)
        return 2

    nodes = discover_nodes(logs)
    if not nodes:
        print(f"error: no cbcollect node directories found under {logs}", file=sys.stderr)
        return 2

    print(f"Found {len(nodes)} node(s); loaded {len(issues)} known issue(s) "
          f"from {xlsx.name}; {len(active)} detector(s) active.\n", file=sys.stderr)

    results: List[NodeResult] = []
    t0 = time.time()
    for node_dir in nodes:
        print(f"[+] {node_dir}", file=sys.stderr)
        results.append(scan_node(
            node_dir, issues, active,
            max_file_mb=args.max_file_mb,
            max_samples=args.max_samples,
            verbose=args.verbose,
            strict_version=args.strict_version,
        ))
    elapsed = time.time() - t0
    print(f"\nScanned {len(nodes)} node(s) in {elapsed:.1f}s.\n", file=sys.stderr)

    print_console_report(results, issues, active, uncovered)

    if args.json:
        Path(args.json).write_text(json.dumps(build_json(results, issues), indent=2),
                                   encoding="utf-8")
        print(f"[wrote JSON: {args.json}]", file=sys.stderr)
    if args.report:
        write_markdown(args.report, results, issues, active)
        print(f"[wrote Markdown: {args.report}]", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
