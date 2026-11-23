"""
detectors.py
============
The detection rules. Each entry pairs a Supportability ID from the Excel
catalogue with a set of concrete log signals. The spreadsheet's symptom and
verification text is prose ("Log Messages will not be exact, but similar"), so
we can't grep the workbook as-is. Instead the log signatures we can match on
are written up here, keyed by Supportability ID.

To cover a new row in the workbook, add a Detector with the same `supp_id`.
Rows in the workbook without a Detector are still reported by the tool as
"catalogued, no detector implemented yet".

Signal model
------------
A Signal is matched against the log files whose basename matches any glob in
`file_globs`. For every line we check a lowercase substring `hint` first, and
only run the regex if that is present. This keeps scans of logs that are
several hundred MB in size quick.

`kind` classifies what a firing signal tells us:
    "symptom"      - a downstream effect consistent with the issue
    "trigger"      - the event that provokes the issue
    "verification" - a strong, specific confirmation of the issue
    "hint"         - weak and informational, needs a human to confirm

Confidence comes from which kinds fired (see confidence_for()). A detector can
also set `requires`, i.e. signal names that must fire before the issue is
classified at all (e.g. SUPP-32 requires its cert-refresh trigger).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


@dataclass
class Pattern:
    """A regex plus a lowercase substring used to pre-filter lines.

    `exclude`, if set, is a regex that rules a line out even when the main
    regex matches. We use it to filter out lines that look similar but aren't
    relevant (e.g. Prometheus scrape-debug lines that only name a metric rather
    than report its value).
    """

    hint: str                        # literal that must be in the (lowercased) line
    regex: str                       # full regex confirming the match
    exclude: Optional[str] = None    # regex that, if it matches, rejects the line
    _compiled: Optional[re.Pattern] = field(default=None, repr=False)
    _compiled_excl: Optional[re.Pattern] = field(default=None, repr=False)

    def compiled(self) -> re.Pattern:
        if self._compiled is None:
            self._compiled = re.compile(self.regex, re.IGNORECASE)
        return self._compiled

    def compiled_exclude(self) -> Optional[re.Pattern]:
        if self.exclude and self._compiled_excl is None:
            self._compiled_excl = re.compile(self.exclude, re.IGNORECASE)
        return self._compiled_excl


@dataclass
class Signal:
    name: str
    kind: str                       # symptom | trigger | verification | hint
    file_globs: List[str]           # basename globs, case-insensitive
    patterns: List[Pattern]
    description: str = ""
    min_matches: int = 1            # lines needed before the signal "fires"


@dataclass
class Detector:
    supp_id: str
    signals: List[Signal]
    note: str = ""                  # extra guidance for the analyst
    requires: List[str] = field(default_factory=list)  # signal names that must fire
    requires_note: str = ""         # shown when `requires` is unmet but other signals fired
    product: str = "Couchbase Server"  # or "Enterprise Analytics"
    manual_check: str = ""          # step to run by hand when the logs can't confirm
                                    # (printed when the version matches but no
                                    #  signal fired, e.g. a workbench-only test)


CONF_HIGH = "HIGH"
CONF_MEDIUM = "MEDIUM"
CONF_LOW = "LOW"
CONF_NONE = "NONE"

_CONF_RANK = {CONF_NONE: 0, CONF_LOW: 1, CONF_MEDIUM: 2, CONF_HIGH: 3}


def confidence_for(fired_kinds: set) -> str:
    """Map the set of fired signal kinds to a confidence level.

    Confidence is based on the strength of the log evidence only. Whether the
    node's version is in the documented affected list is reported separately,
    and we leave it out of confidence on purpose. The affected lists are often
    incomplete, and a specific signature such as the Analytics driver's exit
    status 33 is proof whatever the version.

    - verification fired            gives HIGH   (specific, near-definitive proof)
    - trigger and symptom fired     gives HIGH   (cause and effect together)
    - symptom or trigger alone      gives MEDIUM
    - only weak hints fired         gives LOW
    - nothing fired                 gives NONE
    """
    if not fired_kinds:
        return CONF_NONE
    if "verification" in fired_kinds:
        return CONF_HIGH
    if "trigger" in fired_kinds and "symptom" in fired_kinds:
        return CONF_HIGH
    if "trigger" in fired_kinds or "symptom" in fired_kinds:
        return CONF_MEDIUM
    return CONF_LOW  # only hints


# ===========================================================================
# Detector definitions
# ===========================================================================

# ---- SUPP-30 : Analytics compressed DCP causes KV memory pressure / OOM -----
SUPP_30 = Detector(
    supp_id="SUPP-30",
    note=(
        "Verification is strongest when KV metrics kv_dcp_total_data_size_bytes "
        "and kv_dcp_total_uncompressed_data_size_bytes DIFFER for a cbas DCP "
        "connection while kv_dcp_ready_queue_size_bytes is elevated. Confirm "
        "cbas is the (only) DCP client before concluding."
    ),
    signals=[
        Signal(
            name="eventing_bucket_op_timeout",
            kind="symptom",
            file_globs=["*eventing*.log", "*eventing.log"],
            description="Eventing get/fetch ops time out because KV is overloaded.",
            min_matches=1,
            patterns=[
                Pattern(
                    hint="getopcallback",
                    regex=r"(Consumer|Producer)::getOpCallback.*Bucket (fetch|get) failed",
                ),
            ],
        ),
        Signal(
            name="kv_temporary_failure_timeouts",
            kind="symptom",
            file_globs=[
                "*eventing*.log", "*goxdcr*.log", "*memcached.log",
                "*reports.log", "*diag.log", "*error.log",
            ],
            description="Clients hit KV temp-failures / ambiguous timeouts (KV under memory pressure).",
            min_matches=1,
            patterns=[
                # SDK-level signatures, specific to client ops timing out under
                # KV pressure. We don't match the bare memcached `etmpfail` atom on
                # purpose, as it also shows up for harmless warmup errors when a
                # bucket is created.
                Pattern(hint="ambiguoustimeoutexception", regex=r"AmbiguousTimeoutException"),
                Pattern(hint="kv_temporary_failure", regex=r"KV_TEMPORARY_FAILURE"),
            ],
        ),
        Signal(
            name="kv_dcp_ready_queue_elevated",
            kind="hint",
            file_globs=["*stats.log", "*prometheus*.log", "*diag.log", "*analytics*.log"],
            description="kv_dcp_ready_queue_size_bytes sample present — inspect the value for the cbas connection.",
            min_matches=1,
            patterns=[
                Pattern(
                    hint="kv_dcp_ready_queue_size_bytes",
                    regex=r"kv_dcp_ready_queue_size_bytes",
                    # ignore Prometheus scrape-debug lines that only name the series
                    exclude=r"caller=scrape\.go|Duplicate sample for timestamp",
                ),
            ],
        ),
        Signal(
            name="kv_dcp_compression_divergence",
            kind="hint",
            file_globs=["*stats.log", "*prometheus*.log", "*diag.log"],
            description=(
                "kv_dcp_total_uncompressed_data_size_bytes sample present — compare "
                "against kv_dcp_total_data_size_bytes (divergence => compressed docs "
                "being duplicated for cbas)."
            ),
            min_matches=1,
            patterns=[
                Pattern(
                    hint="kv_dcp_total_uncompressed_data_size_bytes",
                    regex=r"kv_dcp_total_uncompressed_data_size_bytes",
                    exclude=r"caller=scrape\.go|Duplicate sample for timestamp",
                ),
            ],
        ),
    ],
)

# ---- SUPP-32 : Certificate refresh restarts the Analytics driver ------------
#
# The condition that defines this issue is a TLS/cert refresh causing the
# restart. Exit status 33 is the exit code that restart uses, but on its own it
# isn't enough, since the Analytics driver can also exit for unrelated reasons
# (e.g. exit status 1 under memory pressure, which points at SUPP-30 instead).
# As such, the cert-refresh trigger is required before we classify a node as
# SUPP-32. We don't use the generic "Analytics Process Crashed" event
# (event_id 5121) as evidence, because it fires for every crash whatever the
# cause.
SUPP_32 = Detector(
    supp_id="SUPP-32",
    note=(
        "Signature = a TLS/cert refresh immediately followed by the Analytics "
        "driver exiting with status 33. Exit status 33 WITHOUT a cert refresh is "
        "not this issue (a plain 'exit status 1' crash loop points at memory "
        "pressure / SUPP-30 instead)."
    ),
    requires=["tls_refresh_driver_restart"],
    requires_note=(
        "Analytics driver exit-status-33 events were seen, but NO TLS/cert-refresh "
        "trigger was found in the logs — not classified as SUPP-32 (investigate the "
        "crash cause; a plain exit-status-1 crash loop suggests SUPP-30 memory pressure)."
    ),
    signals=[
        Signal(
            name="tls_refresh_driver_restart",
            kind="trigger",
            file_globs=["*diag.log", "*analytics*.log", "*babysitter*.log", "*info.log"],
            description="TLS/cert config refresh triggered an Analytics driver restart.",
            min_matches=1,
            patterns=[
                Pattern(hint="tls config has been refreshed", regex=r"tls config has been refreshed"),
                Pattern(hint="restarting driver due to encryption", regex=r"restarting driver due to encryption"),
                Pattern(hint="immediate restart driver request", regex=r"immediate restart driver request"),
            ],
        ),
        Signal(
            name="analytics_driver_exit_33",
            kind="verification",
            file_globs=["*diag.log", "*analytics*.log", "*babysitter*.log", "*error.log"],
            description="Analytics driver exited specifically with status 33 (the cert-refresh restart code).",
            min_matches=1,
            patterns=[
                Pattern(
                    hint="exit status 33",
                    regex=r"analytics driver has exited w/?\s*exit status 33",
                ),
                # Structured crash event in diag.log, only when exit_code is 33.
                Pattern(hint='"exit_code":33', regex=r'"exit_code"\s*:\s*33'),
            ],
        ),
    ],
)


# ---- SUPP-33 : Bucket recreated with a higher vbucket count, rollback crash --
#
# When a bucket backing an Analytics collection is dropped and recreated (or a
# remote link is repointed) with a different vbucket count, the rollback-to-0
# logic hits an ArrayIndexOutOfBounds and the connect link fails. The signature
# that defines it is the "number of bucket partitions has changed" log line
# (and possibly the "reallocating dcp bucket state ... mismatch" line).
SUPP_33 = Detector(
    supp_id="SUPP-33",
    note=(
        "Signature = a bucket's vbucket (partition) count changing between the "
        "old and new bucket, forcing a rollback to 0 that then fails. Confirm the "
        "bucket was dropped/recreated (or a remote link repointed) with a "
        "different vbucket count."
    ),
    signals=[
        Signal(
            name="bucket_partition_count_changed",
            kind="verification",
            file_globs=["*analytics*.log", "*diag.log"],
            description="CBAS observed the bucket's vbucket count change and rolled back to 0.",
            patterns=[
                Pattern(
                    hint="number of bucket partitions has changed",
                    regex=r"number of bucket partitions has changed \(was \d+ now \d+\)",
                ),
                Pattern(
                    hint="reallocating dcp bucket state",
                    regex=r"reallocating dcp bucket state due to bucket partitions mismatch",
                ),
            ],
        ),
        Signal(
            name="connect_link_failed_aioobe",
            kind="symptom",
            file_globs=["*analytics*.log", "*diag.log"],
            description="Connect link fails with an internal error / ArrayIndexOutOfBounds during rollback.",
            patterns=[
                Pattern(hint="cbas0029", regex=r"CBAS0029: Connect link failed"),
                Pattern(
                    hint="out of bounds for length",
                    regex=r"ArrayIndexOutOfBoundsException: Index \d+ out of bounds for length \d+",
                ),
            ],
        ),
    ],
)

# ---- SUPP-42 : KV/Data outage leaves the Analytics Service prolonged-unusable
#
# This follows on from a Data Service outage. CBAS DCP channels fail repeatedly
# and the service reports itself temporarily unavailable. There is no single
# definitive line. Instead we look for repeated DCP connection failures together
# with "temporarily unavailable" or "Failure handling HTTP Request". The root
# cause is on KV, so check Data Service health for the same window.
SUPP_42 = Detector(
    supp_id="SUPP-42",
    note=(
        "This is a DOWNSTREAM effect of a KV/Data Service outage — confirm the KV "
        "side (timeouts, OOM, slow ops) for the same window. CBAS recovers once KV "
        "does. Repeated DcpChannel 'Connection FAILED N times' plus 'temporarily "
        "unavailable' is the fingerprint."
    ),
    requires=["dcp_channel_connection_failed"],
    requires_note=(
        "Generic 'Failure handling HTTP Request' warnings were seen but the "
        "SUPP-42 fingerprint — repeated DcpChannel 'Connection FAILED N times' "
        "(KV unreachable) — was not. 'Failure handling HTTP Request' alone is "
        "too generic to classify; corroborate with Data Service health."
    ),
    signals=[
        Signal(
            name="dcp_channel_connection_failed",
            kind="trigger",
            file_globs=["*analytics*warn*.log", "*analytics*.log"],
            description="CBAS DcpChannel repeatedly fails to connect (KV unreachable).",
            patterns=[
                Pattern(hint="connection failed", regex=r"DcpChannel.*Connection FAILED \d+ times"),
            ],
        ),
        Signal(
            name="analytics_temporarily_unavailable",
            kind="symptom",
            file_globs=["*analytics*.log", "*query.log", "*diag.log", "*error.log"],
            description="Analytics Service reports itself temporarily unavailable (HTTP 500 / code 23000).",
            patterns=[
                Pattern(hint="temporarily unavailable", regex=r"Analytics Service is temporarily unavailable"),
                Pattern(hint='"code":23000', regex=r'"code"\s*:\s*23000'),
            ],
        ),
        Signal(
            name="failure_handling_http_request",
            kind="hint",   # generic warning, only supporting evidence and never classifies on its own
            file_globs=["*analytics*error*.log", "*analytics*warn*.log", "*analytics*.log"],
            description="Repeated 'Failure handling HTTP Request' in the Analytics error/warn logs (generic).",
            patterns=[
                Pattern(hint="failure handling http request", regex=r"Failure handling HTTP Request"),
            ],
        ),
    ],
)

# ---- SUPP-69 : Analytics Workbench cannot display integers > 2^53 ------------
#
# This one has next to no footprint in a cbcollect, as it is reproduced in the
# Analytics Workbench UI. So we go by version and give a manual test. If the
# message ever does turn up in a captured log, we treat it as proof.
SUPP_69 = Detector(
    supp_id="SUPP-69",
    note=(
        "UI-only precision limit; not visible in cbcollect logs. Verify by hand "
        "in the on-prem Analytics Workbench."
    ),
    manual_check=(
        "In the on-prem Analytics Workbench run:  select 2503110226000002;\n"
        "        If the result shows  \"status\": \"Result is returned but cannot be "
        "displayed\"  the node is affected (the value returns fine in cbq / "
        "Columnar / Query Workbench)."
    ),
    signals=[
        Signal(
            name="result_cannot_be_displayed",
            kind="verification",
            file_globs=["*analytics*.log", "*query.log"],
            description="The workbench 'cannot be displayed' status (rarely captured in logs).",
            patterns=[
                Pattern(
                    hint="cannot be displayed",
                    regex=r"Result is returned but cannot be displayed",
                ),
            ],
        ),
    ],
)

# ---- SUPP-83 : Rollback disconnect race leaves ingestion stuck --------------
#
# On a full DCP rollback, CBAS disconnects and then reconnects the bucket, and a
# race in the disconnect path can leave it stuck. The trigger is the sequence of
# required rollbacks followed by "Disconnecting the bucket ... to perform full
# rollback". The verification is the stuck state itself, i.e. a thread BLOCKED
# in CouchbaseConnector.stop() in an analytics thread dump.
SUPP_83 = Detector(
    supp_id="SUPP-83",
    requires=["connector_stop_blocked"],
    requires_note=(
        "The rollback/disconnect sequence (and/or generic CouchbaseConnector.stop "
        "frames) were seen, but NOT the defining 'BLOCKED on ...CouchbaseConnector' "
        "stuck-thread line — so this is not confirmed as the SUPP-83 stuck race. "
        "Rollbacks alone are normal; capture an analytics thread dump during the "
        "hang to confirm."
    ),
    note=(
        "Trigger = a full DCP rollback during ingestion. Verification = a thread "
        "wedged in CouchbaseConnector.stop() (seen in an analytics thread dump / "
        "debug log). If only the rollback/disconnect lines appear without the "
        "blocked thread, it is the right scenario but not yet proven stuck. Also "
        "see SUPP-86 (perpetual rollback cycle) which shares the rollback lines."
    ),
    signals=[
        Signal(
            name="rollback_disconnect_sequence",
            kind="trigger",
            file_globs=["*analytics*.log"],
            description="Required rollback then bucket disconnect to perform a full rollback.",
            patterns=[
                Pattern(hint="notifying cc of the following required rollbacks",
                        regex=r"notifying CC of the following required rollbacks"),
                Pattern(hint="to perform full rollback",
                        regex=r"Disconnecting the bucket .* to perform full rollback"),
            ],
        ),
        Signal(
            name="connector_stop_blocked",
            kind="verification",
            file_globs=["*analytics*debug*.log", "*analytics*.log", "*threads*.log"],
            # Only the BLOCKED line proves the stuck race. A bare
            # CouchbaseConnector.stop() frame shows up in normal thread dumps
            # during any healthy disconnect, so we don't classify on it.
            description="A thread is BLOCKED/stuck waiting on CouchbaseConnector (the disconnect race).",
            patterns=[
                Pattern(hint="blocked on com.couchbase.analytics.adapter.couchbaseconnector",
                        regex=r"BLOCKED on com\.couchbase\.analytics\.adapter\.CouchbaseConnector"),
            ],
        ),
        Signal(
            name="connector_stop_frames_present",
            kind="hint",
            file_globs=["*analytics*debug*.log", "*analytics*.log", "*threads*.log"],
            description="CouchbaseConnector.stop() frames present in a thread dump (generic; needs the BLOCKED line to matter).",
            patterns=[
                Pattern(hint="couchbaseconnector.stop",
                        regex=r"CouchbaseConnector\.stop\(CouchbaseConnector\.java"),
            ],
        ),
    ],
)

# ---- SUPP-86 : Perpetual rollback cycle ------------------------------------
#
# A cluster-level race causes rollbacks to repeat on the same bucket, sometimes
# indefinitely. The sheet notes there is no definitive statistic. What we look
# for is the required-rollback and "Performing full rollback of datasets" lines
# recurring on the same bucket over a long period. This is reported at MEDIUM at
# most, and the analyst needs to confirm the repetition over time.
SUPP_86 = Detector(
    supp_id="SUPP-86",
    note=(
        "There is no definitive single log line. Confirm by observing the "
        "'required rollbacks' and 'Performing full rollback of datasets' messages "
        "RECURRING on the SAME bucket over an extended period. Shares the rollback "
        "lines with SUPP-83 — SUPP-83 is distinguished by a wedged connector thread."
    ),
    signals=[
        Signal(
            name="full_rollback_of_datasets",
            kind="trigger",
            file_globs=["*analytics*.log"],
            description="'Performing full rollback of datasets ... for the bucket' (a rollback cycle step).",
            patterns=[
                Pattern(hint="performing full rollback of datasets",
                        regex=r"Performing full rollback of datasets .* for the bucket"),
            ],
        ),
        Signal(
            name="required_rollbacks_recurring",
            kind="symptom",
            file_globs=["*analytics*.log"],
            description="Repeated 'notifying CC of the following required rollbacks' (>=3 distinct lines).",
            min_matches=3,   # a one-off rollback is normal, the cycle is what repeats
            patterns=[
                Pattern(hint="notifying cc of the following required rollbacks",
                        regex=r"notifying CC of the following required rollbacks"),
            ],
        ),
    ],
)

# ---- SUPP-121 : Invalid pinCount -1 from an interrupted query (read path) ----
#
# An interrupted or cancelled query closes the pipeline with a buffer-cache page
# at pinCount -1, which breaks later access to that collection. We tell it apart
# from SUPP-129 (same -1 symptom) by the read/interrupt path, i.e. 'Error while
# reading a page', InterruptedException and ResourceReleaseUtils 'Failure
# closing a closeable resource'.
SUPP_121 = Detector(
    supp_id="SUPP-121",
    requires=["invalid_pincount_negative_one"],
    requires_note=(
        "The read/interrupt path evidence was seen, but NOT the defining "
        "'Invalid pinCount: -1' line — not classified as SUPP-121 (the pinCount "
        "corruption is the defining symptom; 'Failure closing a closeable "
        "resource' and interrupted reads occur for other reasons too)."
    ),
    note=(
        "The -1 pinCount here comes from the READ/interrupt path (query cancelled "
        "while fetching). Confirmed by 'Failure closing a closeable resource' "
        "(ResourceReleaseUtils) around an interrupted page read. SUPP-129 is the "
        "same -1 symptom via the B-Tree SEARCH path — check the stack to tell them "
        "apart. Workaround for both: POST /analytics/cluster/restart."
    ),
    signals=[
        Signal(
            name="reading_page_interrupted",
            kind="trigger",
            file_globs=["*analytics*.log"],
            description="Buffer-cache page read interrupted (query cancellation).",
            patterns=[
                Pattern(hint="error while reading a page", regex=r"Error while reading a page"),
            ],
        ),
        Signal(
            name="resource_release_failure",
            kind="verification",
            file_globs=["*analytics*.log"],
            description="ResourceReleaseUtils fails to close a resource (pipeline closed at invalid pinCount).",
            patterns=[
                Pattern(hint="failure closing a closeable resource",
                        regex=r"Failure closing a closeable resource"),
            ],
        ),
        Signal(
            name="invalid_pincount_negative_one",
            kind="symptom",
            file_globs=["*analytics*.log"],
            description="decrementAndGet: Invalid pinCount: -1 (shared with SUPP-129).",
            patterns=[
                Pattern(hint="invalid pincount: -1", regex=r"Invalid pinCount:\s*-1"),
            ],
        ),
    ],
)

# ---- SUPP-129 : Double unpin gives Invalid pinCount -1 during a B-Tree search
#
# Same invalid -1 pinCount symptom as SUPP-121, but via the B-Tree search path
# (DiskBTree.searchDown / IndexSearch). We require the B-Tree search evidence so
# that a bare pinCount -1 (which could be SUPP-121's read path) isn't
# misclassified as SUPP-129.
SUPP_129 = Detector(
    supp_id="SUPP-129",
    note=(
        "The -1 pinCount here occurs during a B-Tree SEARCH (double unpin) — stack "
        "shows DiskBTree.searchDown / IndexSearchOperatorNodePushable. Distinct "
        "from SUPP-121 (read/interrupt path). Workaround: POST "
        "/analytics/cluster/restart; fixed in 7.2.9 / 7.6.10 / 8.0.1."
    ),
    requires=["invalid_pincount_negative_one", "btree_search_unpin"],
    requires_note=(
        "Requires BOTH 'Invalid pinCount: -1' AND the B-Tree search stack "
        "(DiskBTree.searchDown / IndexSearchOperatorNodePushable). Generic "
        "IndexSearch frames alone are normal query execution — without the "
        "pinCount -1 this is not SUPP-129. A pinCount -1 off the search path is "
        "likely SUPP-121 instead."
    ),
    signals=[
        Signal(
            name="btree_search_unpin",
            kind="verification",
            file_globs=["*analytics*.log"],
            description="pinCount failure on the DiskBTree search path (the SUPP-129 signature).",
            patterns=[
                Pattern(hint="diskbtree.searchdown", regex=r"DiskBTree\.searchDown"),
                Pattern(hint="indexsearchoperatornodepushable",
                        regex=r"IndexSearchOperatorNodePushable"),
            ],
        ),
        Signal(
            name="invalid_pincount_negative_one",
            kind="symptom",
            file_globs=["*analytics*.log"],
            description="decrementAndGet: Invalid pinCount: -1 (shared with SUPP-121).",
            patterns=[
                Pattern(hint="invalid pincount: -1", regex=r"Invalid pinCount:\s*-1"),
            ],
        ),
    ],
)

# ---- SUPP-170 : Enterprise Analytics .dic misaligned positions halt the JVM --
#
# Enterprise Analytics only (columnar format on cloud/object storage). A merge
# writes look-aside (.dic) pages out of order to the append-only cloud copy, so
# the writer rejects it and halts the JVM (exit 3). The definitive line is
# "Misaligned positions in ....dic" on a [MERGE:] thread. The .btree files are
# never affected.
SUPP_170 = Detector(
    supp_id="SUPP-170",
    product="Enterprise Analytics",
    note=(
        "Enterprise Analytics only, columnar (COLUMN) collections on cloud/object "
        "storage. Confirm all three: the error is on a [MERGE:] thread; the file "
        "ends in .dic (never .btree); and the offset gap == pageSize+8 (131080 B "
        "at the 128KB default). Fixed in EA 2.2.0 (build 1316) / 2.2.0-MP1; or use "
        "row storage format."
    ),
    signals=[
        Signal(
            name="misaligned_positions_dic",
            kind="verification",
            file_globs=["*analytics*.log", "*cbas*.log", "*diag.log"],
            description="FIFOLocalWriter fatal: misaligned positions writing a .dic look-aside file.",
            patterns=[
                Pattern(hint="misaligned positions in", regex=r"Misaligned positions in .*\.dic"),
                Pattern(hint="fifolocalwriter has encountered a fatal error",
                        regex=r"FIFOLocalWriter has encountered a fatal error"),
            ],
        ),
        Signal(
            name="jvm_halt_nc_timeout",
            kind="symptom",
            file_globs=["*analytics*.log", "*diag.log"],
            description="Global recovery times out and the JVM exits (status 3).",
            patterns=[
                Pattern(hint="passed before getting back the responses from ncs",
                        regex=r"passed before getting back the responses from NCs"),
            ],
        ),
    ],
)


DETECTORS: Dict[str, Detector] = {
    d.supp_id: d
    for d in (
        SUPP_30, SUPP_32, SUPP_33, SUPP_42, SUPP_69,
        SUPP_83, SUPP_86, SUPP_121, SUPP_129, SUPP_170,
    )
}


def get_detector(supp_id: str) -> Optional[Detector]:
    return DETECTORS.get(supp_id)
