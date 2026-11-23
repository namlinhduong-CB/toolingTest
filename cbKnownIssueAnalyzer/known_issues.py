"""
known_issues.py
===============
Loads the "known issues" catalogue from the support Excel workbook (e.g. Book1.xlsx)
and has the helpers for matching versions.

The workbook should contain a table whose header row includes a "Supportability
ID" column. Column headers are matched by name (ignoring case and whitespace), so
the columns can move around without breaking the loader. The headers we
recognise, and the attribute each one maps to, are:

    Supportability ID   to supp_id      (e.g. "SUPP-30")   [required key]
    MB ID               to mb_id        (e.g. "MB-64441")
    Summary             to summary
    Affected Versions   to affected_versions_raw
    Symptoms            to symptoms
    Triggers            to triggers
    Verification        to verification
    Workarounds         to workarounds

Any other columns are kept in KnownIssue.extra.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Version handling
# ---------------------------------------------------------------------------

# A Couchbase build version looks like "7.6.5-5704-enterprise".
_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)(?:-(\d+))?")


@dataclass(frozen=True)
class Version:
    """A parsed Couchbase Server version."""

    major: int
    minor: int
    patch: int
    build: Optional[int] = None
    raw: str = ""

    @property
    def short(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __str__(self) -> str:
        return self.raw or self.short


def parse_version(text: str) -> Optional[Version]:
    """Extract the first X.Y.Z[-build] version found in `text`."""
    if not text:
        return None
    m = _VERSION_RE.search(text)
    if not m:
        return None
    major, minor, patch, build = m.groups()
    return Version(
        major=int(major),
        minor=int(minor),
        patch=int(patch),
        build=int(build) if build else None,
        raw=m.group(0),
    )


# A spec token from the spreadsheet: "7.2.x", "7.6.5", "6.5.x", "7.x" ...
_SPEC_RE = re.compile(r"(\d+)\.(\d+|x)(?:\.(\d+|x))?", re.IGNORECASE)


@dataclass(frozen=True)
class VersionSpec:
    """One affected-version token, possibly with `x` wildcards."""

    major: int
    minor: Optional[int]  # None means wildcard
    patch: Optional[int]  # None means wildcard
    raw: str = ""

    def matches(self, v: Version) -> bool:
        if v is None:
            return False
        if self.major != v.major:
            return False
        if self.minor is not None and self.minor != v.minor:
            return False
        if self.patch is not None and self.patch != v.patch:
            return False
        return True


def parse_affected_versions(raw: str) -> List[VersionSpec]:
    """Parse a free-form 'Affected Versions' cell into a list of VersionSpec.

    Handles comma / newline / semicolon separated tokens and `x` wildcards,
    e.g. "6.5.x,\n7.2.6, 7.6.5".
    """
    specs: List[VersionSpec] = []
    if not raw:
        return specs
    for m in _SPEC_RE.finditer(raw):
        major = int(m.group(1))

        def _part(p: Optional[str]) -> Optional[int]:
            if p is None or p.lower() == "x":
                return None
            return int(p)

        specs.append(
            VersionSpec(
                major=major,
                minor=_part(m.group(2)),
                patch=_part(m.group(3)),
                raw=m.group(0),
            )
        )
    return specs


# A concrete fix version like "7.2.7" mentioned in Workarounds/Summary text.
_FIX_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


def parse_fix_versions(text: str,
                       affected: Optional[List[VersionSpec]] = None
                       ) -> Dict[Tuple[int, int], Tuple[int, str]]:
    """Best-effort extraction of fix versions from free text (e.g. Workarounds).

    Returns {(major, minor): (first_fix_patch, raw)}, i.e. the earliest patch
    in each release series where the issue is documented as fixed. A token only
    counts as a fix if it is newer than the newest affected patch in the same
    series, so we don't mistake an affected version for a fix.

    This is a heuristic, since the spreadsheet has no 'fixed in' column of its
    own, and it is labelled as such wherever we print it.
    """
    fixes: Dict[Tuple[int, int], Tuple[int, str]] = {}
    if not text:
        return fixes

    # newest affected patch per (major, minor) series, used to filter fix candidates
    max_affected: Dict[Tuple[int, int], int] = {}
    for s in (affected or []):
        if s.minor is None or s.patch is None:
            continue
        key = (s.major, s.minor)
        max_affected[key] = max(max_affected.get(key, -1), s.patch)

    for m in _FIX_RE.finditer(text):
        major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
        key = (major, minor)
        gate = max_affected.get(key)
        if gate is not None and patch <= gate:
            continue  # not newer than a known affected patch, so not a fix
        cur = fixes.get(key)
        if cur is None or patch < cur[0]:
            fixes[key] = (patch, m.group(0))
    return fixes


# ---------------------------------------------------------------------------
# Known issue record
# ---------------------------------------------------------------------------

@dataclass
class KnownIssue:
    supp_id: str
    mb_id: str = ""
    summary: str = ""
    affected_versions_raw: str = ""
    symptoms: str = ""
    triggers: str = ""
    verification: str = ""
    workarounds: str = ""
    extra: Dict[str, str] = field(default_factory=dict)

    def affected_specs(self) -> List[VersionSpec]:
        return parse_affected_versions(self.affected_versions_raw)

    def fix_versions(self) -> Dict[Tuple[int, int], Tuple[int, str]]:
        """Best-effort fix versions parsed from Workarounds (and Summary)."""
        return parse_fix_versions(
            f"{self.workarounds}\n{self.summary}", self.affected_specs())

    @property
    def product(self) -> str:
        blob = f"{self.affected_versions_raw}\n{self.summary}".lower()
        return "Enterprise Analytics" if "enterprise analytics" in blob \
            else "Couchbase Server"

    def is_version_affected(self, v: Optional[Version]) -> Optional[bool]:
        """Return True/False if the version matches the documented affected
        list, or None if we can't tell (no version or no specs)."""
        specs = self.affected_specs()
        if v is None or not specs:
            return None
        return any(s.matches(v) for s in specs)

    def version_status(self, v: Optional[Version]) -> Tuple[str, str]:
        """Version verdict, used for the version-first step.

        Returns (status, reason) where status is one of:
            AFFECTED    - version matches the affected list, or is older than
                          the documented fix in its release series
            FIXED       - version is at or after the documented fix for its series
            NOT_LISTED  - this MB does not reference the version's release line
            UNKNOWN     - no version detected, or the MB lists no versions
        """
        specs = self.affected_specs()
        if v is None:
            return "UNKNOWN", "server version could not be detected"
        if not specs:
            return "UNKNOWN", "MB lists no affected versions"

        if any(s.matches(v) for s in specs):
            return "AFFECTED", "version is in the documented affected list"

        fixes = self.fix_versions()
        fix = fixes.get((v.major, v.minor))
        if fix is not None:
            fix_patch, fix_raw = fix
            if v.patch < fix_patch:
                return ("AFFECTED",
                        f"older than the documented fix {fix_raw} in the "
                        f"{v.major}.{v.minor}.x series (fix parsed from Workarounds)")
            return ("FIXED",
                    f"at/after the documented fix {fix_raw} "
                    f"(fix parsed from Workarounds)")

        # Check whether any affected version refers to this release line at all.
        same_series = any(s.major == v.major and (s.minor in (None, v.minor))
                          for s in specs)
        if same_series:
            return ("AFFECTED",
                    f"within an affected {v.major}.{v.minor}.x line "
                    "(no fix patch documented for this series)")
        return ("NOT_LISTED",
                f"no affected {v.major}.{v.minor}.x version listed for this MB")


# ---------------------------------------------------------------------------
# Workbook loading
# ---------------------------------------------------------------------------

# Normalised header text, mapped to the KnownIssue attribute name.
_HEADER_MAP = {
    "supportability id": "supp_id",
    "supp id": "supp_id",
    "mb id": "mb_id",
    "mb": "mb_id",
    "summary": "summary",
    "affected versions": "affected_versions_raw",
    "affected version": "affected_versions_raw",
    "symptoms": "symptoms",
    "triggers": "triggers",
    "trigger": "triggers",
    "verification": "verification",
    "workarounds": "workarounds",
    "workaround": "workarounds",
}


def _norm_header(value: object) -> str:
    """Normalise a header cell. Lowercase it, collapse whitespace and drop the
    notes in brackets the spreadsheet uses, e.g. 'Symptoms (Log ...)'."""
    if value is None:
        return ""
    text = str(value)
    text = re.split(r"[\(\n]", text, maxsplit=1)[0]  # drop "(...)" notes
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def load_known_issues(xlsx_path: str, sheet: Optional[str] = None) -> Dict[str, KnownIssue]:
    """Load known issues from an .xlsx workbook, keyed by Supportability ID."""
    try:
        import openpyxl  # noqa: WPS433 (importing here keeps the dependency optional)
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "openpyxl is required to read the Excel workbook. "
            "Install it with:  python3 -m pip install openpyxl"
        ) from exc

    wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)
    ws = wb[sheet] if sheet else wb[wb.sheetnames[0]]

    rows = list(ws.iter_rows(values_only=True))
    header_row_idx, col_index = _locate_header(rows)
    if header_row_idx is None:
        raise SystemExit(
            f"Could not find a 'Supportability ID' header in {xlsx_path!r}. "
            "Check the sheet layout."
        )

    issues: Dict[str, KnownIssue] = {}
    for row in rows[header_row_idx + 1:]:
        supp_col = col_index.get("supp_id")
        if supp_col is None or supp_col >= len(row):
            continue
        supp_val = row[supp_col]
        if supp_val is None or not str(supp_val).strip():
            continue  # skip blank rows

        fields: Dict[str, str] = {}
        extra: Dict[str, str] = {}
        for attr, idx in col_index.items():
            if attr == "__extra__":
                continue
            val = row[idx] if idx < len(row) else None
            text = "" if val is None else str(val).strip()
            fields[attr] = text
        # keep any unmapped columns in extra
        for header, idx in col_index.get("__extra__", {}).items():  # type: ignore
            val = row[idx] if idx < len(row) else None
            if val is not None and str(val).strip():
                extra[header] = str(val).strip()

        issue = KnownIssue(
            supp_id=str(fields.get("supp_id", "")).strip(),
            mb_id=fields.get("mb_id", ""),
            summary=fields.get("summary", ""),
            affected_versions_raw=fields.get("affected_versions_raw", ""),
            symptoms=fields.get("symptoms", ""),
            triggers=fields.get("triggers", ""),
            verification=fields.get("verification", ""),
            workarounds=fields.get("workarounds", ""),
            extra=extra,
        )
        issues[issue.supp_id] = issue

    wb.close()
    return issues


def _locate_header(rows) -> Tuple[Optional[int], Dict]:
    """Find the header row and build {attr: column index}.

    Columns that are present but not mapped are recorded under the key
    '__extra__' as {header_text: column_index}.
    """
    for r_idx, row in enumerate(rows):
        norm = [_norm_header(c) for c in row]
        if any(h in ("supportability id", "supp id") for h in norm):
            col_index: Dict = {}
            extra: Dict[str, int] = {}
            for c_idx, h in enumerate(norm):
                if not h:
                    continue
                attr = _HEADER_MAP.get(h)
                if attr and attr not in col_index:
                    col_index[attr] = c_idx
                elif not attr:
                    extra[str(row[c_idx])] = c_idx
            col_index["__extra__"] = extra
            return r_idx, col_index
    return None, {}


if __name__ == "__main__":  # quick manual check
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "Book1.xlsx"
    for sid, issue in load_known_issues(path).items():
        specs = ", ".join(s.raw for s in issue.affected_specs()) or "(none)"
        print(f"{sid}  {issue.mb_id}  versions=[{specs}]")
        print(f"    {issue.summary[:120]}")
