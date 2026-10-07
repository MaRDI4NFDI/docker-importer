"""Archive status of CRAN packages: the *end time* qualifier on *CRAN project*.

A package that has left CRAN carries, on its *CRAN project* statement, an *end
time* qualifier with the day CRAN archived it. A package on CRAN carries none.
Nothing else on the item is touched.

Both facts are read from CRAN's own repository metadata, once per run:

* ``src/contrib/PACKAGES``: the repository index, one record per package
  currently on CRAN.
* ``src/contrib/PACKAGES.in``: the CRAN team's notes on packages. For a package
  that has left CRAN, ``X-CRAN-Comment`` says when ("Archived on 2026-10-05 as
  ..."). This is free text, so the date is read defensively.

Rule, for each package with a *CRAN project* statement in the Wikibase:

* on CRAN → no end time (removed if present: the package came back);
* not on CRAN, comment with a date → end time is that day;
* not on CRAN, no dated comment (e.g. a Bioconductor or base R package recorded
  as a CRAN project) → left as it is and reported.

The current state is read from the SPARQL store to plan the changes; each
change is checked again against the live item before it is written.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import requests

log = logging.getLogger("CRANlogger")

CRAN_CONTRIB = "https://cran.r-project.org/src/contrib"
USER_AGENT = "mardi-importer (https://portal.mardi4nfdi.de; CRAN archive status)"
CRAN_PROJECT = "wdt:P5565"
END_TIME = "wdt:P582"
GREGORIAN = "http://www.wikidata.org/entity/Q1985727"
# A truncated index would make thousands of packages look archived.
MIN_PACKAGES_ON_CRAN = 10000

_ARCHIVE_DATE = re.compile(r"\b(?:archived?|removed)\b\D{0,20}?(\d{4}-\d{2}-\d{2})", re.I)
_ANY_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


# -- reading CRAN -------------------------------------------------------------------

def parse_dcf(text: str) -> list[dict[str, str]]:
    """Records of a Debian control file; continuation lines are joined with a space."""
    records, rec, key = [], {}, None
    for line in text.splitlines():
        if not line.strip():
            if rec:
                records.append(rec)
            rec, key = {}, None
        elif line[0] in " \t":
            if key:
                rec[key] += " " + line.strip()
        else:
            k, sep, v = line.partition(":")
            if sep:
                key = k.strip()
                rec[key] = v.strip()
    if rec:
        records.append(rec)
    return records


def _valid(day: str) -> bool:
    try:
        date.fromisoformat(day)
    except ValueError:
        return False
    return True


def archive_date(comment: str) -> str | None:
    """The day a package left CRAN, from its ``X-CRAN-Comment``.

    The latest date following "Archived"/"Removed" ("Archived on 2026-10-05 as
    ..."); failing that, the latest date in the comment ("Orphaned on
    2016-08-25 ... Archived as ..."). None when the comment has no date.
    """
    dates = [d for d in _ARCHIVE_DATE.findall(comment) if _valid(d)] \
        or [d for d in _ANY_DATE.findall(comment) if _valid(d)]
    return max(dates) if dates else None


def on_cran(packages_text: str) -> set[str]:
    return {r["Package"] for r in parse_dcf(packages_text) if "Package" in r}


def archived(packages_in_text: str) -> dict[str, str]:
    """Package → the day it left CRAN, for every dated ``X-CRAN-Comment``."""
    out = {}
    for r in parse_dcf(packages_in_text):
        if "Package" in r and (day := archive_date(r.get("X-CRAN-Comment", ""))):
            out[r["Package"]] = day
    return out


def fetch_cran(session: requests.Session | None = None) -> tuple[set[str], dict[str, str]]:
    """Packages on CRAN, and the archive day of packages that have left it."""
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)
    texts = {}
    for name in ("PACKAGES", "PACKAGES.in"):
        res = session.get(f"{CRAN_CONTRIB}/{name}", timeout=120)
        res.raise_for_status()
        texts[name] = res.text
    current = on_cran(texts["PACKAGES"])
    if len(current) < MIN_PACKAGES_ON_CRAN:
        raise RuntimeError(f"CRAN index lists only {len(current)} packages; refusing to use it")
    return current, archived(texts["PACKAGES.in"])


# -- deciding -----------------------------------------------------------------------

@dataclass(frozen=True)
class Listed:
    """A package recorded in the Wikibase as a CRAN project."""
    qid: str
    package: str
    end_time: str | None        # YYYY-MM-DD, or None


@dataclass(frozen=True)
class Change:
    qid: str
    package: str
    old: str | None
    new: str | None             # None → remove the end time

    @property
    def action(self) -> str:
        return "remove" if self.new is None else "add" if self.old is None else "correct"


def plan(listed: Iterable[Listed], current: set[str],
         archive: dict[str, str]) -> tuple[list[Change], list[Listed]]:
    """End-time changes, and packages whose status cannot be told (left as they are)."""
    changes, unresolved = [], []
    for item in sorted(listed, key=lambda x: (x.package.casefold(), x.qid)):
        if item.package in current:
            want = None
        elif item.package in archive:
            want = archive[item.package]
        else:
            unresolved.append(item)
            continue
        if item.end_time != want:
            changes.append(Change(item.qid, item.package, item.end_time, want))
    return changes, unresolved


# -- the Wikibase -------------------------------------------------------------------

def _pid(api, prop: str) -> str:
    pid = api.get_local_id_by_label(prop, "property")
    pid = pid[0] if isinstance(pid, list) else pid
    if not pid:
        raise RuntimeError(f"property {prop} not found in the Wikibase")
    return pid


def read_listed(api, sparql=None) -> list[Listed]:
    """Every *CRAN project* statement with its end time, from the SPARQL store.

    An item with several end times (none expected) is listed with the latest,
    so that it is planned like any other and corrected on the live item.
    """
    if sparql is None:
        from wikibaseintegrator.wbi_helpers import execute_sparql_query as sparql
    cran, end = _pid(api, CRAN_PROJECT), _pid(api, END_TIME)
    query = (f"SELECT ?item ?name ?end WHERE {{ ?item p:{cran} ?st . ?st ps:{cran} ?name . "
             f"OPTIONAL {{ ?st pq:{end} ?end }} }}")
    found: dict[tuple[str, str], str | None] = {}
    for b in sparql(query)["results"]["bindings"]:
        key = (b["item"]["value"].rsplit("/", 1)[-1], b["name"]["value"])
        day = b["end"]["value"][:10] if "end" in b else None
        found[key] = max(filter(None, (found.get(key), day)), default=None)
    return [Listed(qid, name, day) for (qid, name), day in found.items()]


def time_value(day: str) -> dict:
    return {"time": f"+{day}T00:00:00Z", "timezone": 0, "before": 0, "after": 0,
            "precision": 11, "calendarmodel": GREGORIAN}


def edits_for(claims: list[dict], package: str, end_pid: str, want: str | None) -> list[dict]:
    """API requests that bring the live statements of ``package`` to ``want``.

    One end time stays (replaced in place if its day differs) and any others are
    removed; with ``want`` None, all are removed. Statements of another value are
    not touched.
    """
    edits = []
    for c in claims:
        dv = c.get("mainsnak", {}).get("datavalue", {})
        if dv.get("value") != package:
            continue
        ends = c.get("qualifiers", {}).get(end_pid, [])
        if want is None:
            drop = ends
        else:
            keep = next((q for q in ends if q.get("datavalue", {}).get("value", {}).get("time", "")[1:11] == want), None)
            if keep is None:
                setq = {"action": "wbsetqualifier", "claim": c["id"], "property": end_pid,
                        "snaktype": "value", "value": json.dumps(time_value(want))}
                if ends:
                    keep = ends[0]
                    setq["snakhash"] = keep["hash"]
                edits.append(setq)
            drop = [q for q in ends if q is not keep]
        if drop:
            edits.append({"action": "wbremovequalifiers", "claim": c["id"],
                          "qualifiers": "|".join(q["hash"] for q in drop)})
    return edits


def summary_for(change: Change) -> str:
    if change.new is None:
        return f"CRAN archive status: {change.package} is on CRAN again"
    return f"CRAN archive status: {change.package} archived on CRAN on {change.new}"


def apply(api, change: Change, cran_pid: str, end_pid: str, call=None) -> int:
    """Write one change after checking the live item. Returns the number of edits made."""
    if call is None:
        from wikibaseintegrator.wbi_helpers import mediawiki_api_call_helper as call
    claims = api.item.get(entity_id=change.qid).get_json().get("claims", {}).get(cran_pid, [])
    edits = edits_for(claims, change.package, end_pid, change.new)
    for data in edits:
        call(data={**data, "summary": summary_for(change), "bot": "", "format": "json"},
             login=api.login, is_bot=True)
    return len(edits)


def sync(api, dry_run: bool = False, current: set[str] | None = None,
         archive: dict[str, str] | None = None, listed: list[Listed] | None = None) -> dict:
    """Bring every *CRAN project* end time in line with CRAN. Returns a report."""
    if current is None or archive is None:
        current, archive = fetch_cran()
    if listed is None:
        listed = read_listed(api)
    changes, unresolved = plan(listed, current, archive)
    log.info("CRAN archive status: %d CRAN projects, %d on CRAN, %d archived; %d changes, %d unresolved",
             len(listed), len(current), len(archive), len(changes), len(unresolved))

    results = []
    if not dry_run and changes:
        cran_pid, end_pid = _pid(api, CRAN_PROJECT), _pid(api, END_TIME)
    for ch in changes:
        row = {"qid": ch.qid, "package": ch.package, "action": ch.action, "old": ch.old, "new": ch.new}
        if not dry_run:
            try:
                n = apply(api, ch, cran_pid, end_pid)
                row["status"] = "written" if n else "already up to date"
            except Exception as exc:
                log.error("CRAN archive status of %s (%s) failed: %s", ch.package, ch.qid, exc)
                row["status"] = "error"
                row["error"] = str(exc)
        results.append(row)

    counts: dict[str, int] = {}
    for ch in changes:
        counts[ch.action] = counts.get(ch.action, 0) + 1
    return {
        "dry_run": dry_run,
        "cran_projects": len(listed),
        "with_end_time": sum(1 for x in listed if x.end_time),
        "changes": counts,
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "results": results,
        "unresolved": [{"qid": x.qid, "package": x.package, "end_time": x.end_time,
                        "note": "not on CRAN and no dated archive comment; left as it is"}
                       for x in unresolved],
    }
