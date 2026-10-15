"""CRAN Task Views: one item per view, and *part of* that view on each listed package.

Read from CRAN's ``src/contrib/Views.rds``. A view item carries its label
("CRAN Task View: <topic>"), the view's short name as alias and *CRAN task view
ID*, *instance of* CRAN task view, *part of* CRAN Task Views, its CRAN page,
GitHub source, maintainers (as *author*, or *author name string* unless one
person item of that name is linked to a package of the view) and version date as
*last update*. It is rewritten when the version changes.

A listed package gets *part of* the view; a core package with the qualifier
*subject has role* core package. Only *part of* statements pointing to view
items are changed. The current state is read from the SPARQL store to plan; each
package is checked again on the live wiki before it is written.
"""

from __future__ import annotations

import json
import logging
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .resolve import name_key

log = logging.getLogger("CRANlogger")

VIEWS_RDS = "https://cran.r-project.org/src/contrib/Views.rds"
VIEW_URL = "https://CRAN.R-project.org/view="
COLLECTION = "Q20060"                                   # CRAN Task Views
REUSE = {"ExperimentalDesign": "Q5974426"}              # CRAN Task View DoE, from swMATH
CLASS, CORE, VIEW_ID = "CRAN task view", "core package", "CRAN task view ID"
PROPS = {"instance": "wdt:P31", "part_of": "wdt:P361", "role": "wdt:P2868", "website": "wdt:P856",
         "repo": "wdt:P1324", "author": "wdt:P50", "author_string": "wdt:P2093", "updated": "wdt:P5017",
         "cran": "wdt:P5565", "maintained_by": "wdt:P126"}
# Rewritten on a view item when its version changes; anything else on it is kept.
MANAGED = ("instance", "part_of", "view_id", "website", "repo", "author", "author_string", "updated")


@dataclass(frozen=True)
class TaskView:
    name: str
    topic: str
    version: str
    source: str
    maintainers: tuple[str, ...]
    packages: dict = field(default_factory=dict, hash=False, compare=False)      # package → core

    @property
    def label(self) -> str:
        return f"CRAN Task View: {self.topic}"


# -- reading CRAN -------------------------------------------------------------------

def _str(x) -> str:
    return "" if x is None else str(x[0] if hasattr(x, "__len__") and not isinstance(x, str) else x)


def _maintainers(view: dict) -> tuple[str, ...]:
    names = []
    for c in view.get("citation") or []:
        for a in c.get("author") or []:
            name = " ".join(str(w) for part in ("given", "family") if a.get(part) is not None for w in a[part])
            if name.strip():
                names.append(" ".join(name.split()))
    if names:
        return tuple(names)
    text = _str(view.get("maintainer")).replace(" and ", ",")
    return tuple(n.strip() for n in text.split(",") if n.strip())


def _iso(day: str) -> str:
    """'2026-9-2' → '2026-09-02'."""
    y, m, d = day.split("-")
    return f"{y}-{int(m):02d}-{int(d):02d}"


def read_views(path: str | Path) -> list[TaskView]:
    import rdata
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")                 # no converters for R's ctv/person classes
        data = rdata.read_rds(str(path))
    views = []
    for v in data.values():
        pl = v["packagelist"]
        views.append(TaskView(_str(v["name"]), _str(v["topic"]), _iso(_str(v["version"])), _str(v["source"]),
                              _maintainers(v), {str(n): bool(c) for n, c in zip(pl["name"], pl["core"])}))
    return sorted(views, key=lambda x: x.name)


def fetch_views(dest: str | Path, session=None) -> Path:
    import requests
    session = session or requests.Session()
    res = session.get(VIEWS_RDS, timeout=300)
    res.raise_for_status()
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(res.content)
    return dest


# -- statements -----------------------------------------------------------------------

def _item(qid: str) -> dict:
    return {"value": {"entity-type": "item", "numeric-id": int(qid[1:]), "id": qid}, "type": "wikibase-entityid"}


def _snak(pid: str, datavalue: dict) -> dict:
    return {"snaktype": "value", "property": pid, "datavalue": datavalue}


def statement(pid: str, datavalue: dict, qualifiers: dict | None = None, guid: str | None = None) -> dict:
    s = {"mainsnak": _snak(pid, datavalue), "type": "statement", "rank": "normal"}
    if qualifiers:
        s["qualifiers"] = {p: [_snak(p, dv)] for p, dv in qualifiers.items()}
    if guid:
        s["id"] = guid
    return s


def _string(v: str) -> dict:
    return {"value": v, "type": "string"}


def _day(day: str) -> dict:
    return {"value": {"time": f"+{day}T00:00:00Z", "timezone": 0, "before": 0, "after": 0, "precision": 11,
                      "calendarmodel": "http://www.wikidata.org/entity/Q1985727"}, "type": "time"}


def view_claims(view: TaskView, ids: dict, authors: list[tuple[str, str | None]]) -> list[dict]:
    """The managed statements of a view item; ``authors`` are (name, QID or None)."""
    p = ids
    claims = [statement(p["instance"], _item(p["class"])),
              statement(p["part_of"], _item(COLLECTION)),
              statement(p["view_id"], _string(view.name)),
              statement(p["website"], _string(VIEW_URL + view.name)),
              statement(p["updated"], _day(view.version))]
    if view.source:
        claims.append(statement(p["repo"], _string(view.source)))
    for name, qid in authors:
        claims.append(statement(p["author"], _item(qid)) if qid else statement(p["author_string"], _string(name)))
    return claims


def view_entity(view: TaskView, ids: dict, authors, current: dict | None = None) -> dict:
    """``wbeditentity`` data for a new view item, or for ``current`` (its JSON) when it exists."""
    data = {"labels": {"en": {"language": "en", "value": view.label}},
            "claims": view_claims(view, ids, authors)}
    aliases = {a["value"] for a in (current or {}).get("aliases", {}).get("en", [])}
    if view.name not in aliases:
        data["aliases"] = {"en": [{"language": "en", "value": view.name, "add": ""}]}
    if current:
        managed = {ids[k] for k in MANAGED}
        data["claims"] += [{"id": c["id"], "remove": ""} for pid, cs in current.get("claims", {}).items()
                           if pid in managed for c in cs]
    return data


# -- packages -------------------------------------------------------------------------

def _value_id(c: dict) -> str | None:
    v = c.get("mainsnak", {}).get("datavalue", {}).get("value")
    return v.get("id") if isinstance(v, dict) else None


def _is_core(c: dict, role_pid: str, core_qid: str) -> bool:
    return any(_value_id({"mainsnak": q}) == core_qid for q in c.get("qualifiers", {}).get(role_pid, []))


def package_edits(claims: list[dict], want: dict[str, bool], view_qids: set[str], ids: dict) -> list[dict]:
    """*part of* statements to add, change or remove so that the package is in the views ``want`` (view QID → core)."""
    edits, seen = [], set()
    for c in claims:
        view = _value_id(c)
        if view not in view_qids:
            continue
        if view not in want or view in seen:
            edits.append({"id": c["id"], "remove": ""})
            continue
        seen.add(view)
        if _is_core(c, ids["role"], ids["core"]) != want[view]:
            q = {ids["role"]: _item(ids["core"])} if want[view] else None
            edits.append(statement(ids["part_of"], _item(view), q, guid=c["id"]))
    for view, core in sorted(want.items()):
        if view not in seen:
            q = {ids["role"]: _item(ids["core"])} if core else None
            edits.append(statement(ids["part_of"], _item(view), q))
    return edits


# -- the Wikibase ---------------------------------------------------------------------

def _local(api, label: str, kind: str) -> str:
    found = api.get_local_id_by_label(label, kind)
    found = found[0] if isinstance(found, list) and found else found
    if not found:
        raise RuntimeError(f"{kind} {label!r} not found in the Wikibase (run the CRAN setup)")
    return found


def local_ids(api) -> dict:
    ids = {k: _local(api, v, "property") for k, v in PROPS.items()}
    ids["view_id"] = _local(api, VIEW_ID, "property")
    ids["class"] = _local(api, CLASS, "item")
    ids["core"] = _local(api, CORE, "item")
    return ids


def _sparql(query: str) -> list[dict]:
    from wikibaseintegrator.wbi_helpers import execute_sparql_query
    return [{k: v["value"] for k, v in b.items()} for b in execute_sparql_query(query)["results"]["bindings"]]


def _q(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


@dataclass
class State:
    views: dict[str, tuple[str, str | None]]            # view name → (QID, version day)
    packages: dict[str, str]                            # CRAN name → QID
    members: dict[str, dict[str, bool]]                 # package QID → {view QID: core}
    people: dict[str, dict[tuple, set[str]]]            # CRAN name → {name key: person QIDs}


def read_state(ids: dict, sparql=_sparql) -> State:
    p = ids
    views = {}
    for r in sparql(f"SELECT ?item ?id ?v WHERE {{ ?item wdt:{p['instance']} wd:{p['class']} ; "
                    f"wdt:{p['view_id']} ?id . OPTIONAL {{ ?item wdt:{p['updated']} ?v }} }}"):
        views[r["id"]] = (_q(r["item"]), r.get("v", "").lstrip("+")[:10] or None)
    packages = {r["name"]: _q(r["pkg"]) for r in sparql(f"SELECT ?pkg ?name WHERE {{ ?pkg wdt:{p['cran']} ?name }}")}
    members: dict[str, dict[str, bool]] = defaultdict(dict)
    for r in sparql(f"SELECT ?pkg ?view ?role WHERE {{ ?view wdt:{p['instance']} wd:{p['class']} . "
                    f"?pkg p:{p['part_of']} ?st . ?st ps:{p['part_of']} ?view . "
                    f"OPTIONAL {{ ?st pq:{p['role']} ?role }} }}"):
        core = _q(r.get("role", "")) == p["core"]
        members[_q(r["pkg"])][_q(r["view"])] = members[_q(r["pkg"])].get(_q(r["view"]), False) or core
    people: dict[str, dict[tuple, set[str]]] = defaultdict(lambda: defaultdict(set))
    for r in sparql(f"SELECT ?name ?person ?label WHERE {{ ?pkg wdt:{p['cran']} ?name . "
                    f"?pkg wdt:{p['author']}|wdt:{p['maintained_by']} ?person . "
                    f"?person rdfs:label ?label FILTER(LANG(?label) = \"en\") }}"):
        people[r["name"]][name_key(r["label"])].add(_q(r["person"]))
    return State(views, packages, dict(members), people)


def authors_of(view: TaskView, state: State) -> list[tuple[str, str | None]]:
    """Maintainers with the one person item of that name linked to a package of the view, if there is one."""
    out = []
    for name in view.maintainers:
        found = set().union(*(state.people.get(pkg, {}).get(name_key(name), set()) for pkg in view.packages))
        out.append((name, found.pop() if len(found) == 1 else None))
    return out


@dataclass
class Plan:
    create: list[TaskView]
    update: list[tuple[TaskView, str]]                  # (view, QID)
    packages: dict[str, dict[str, bool]]                # package QID → wanted {view QID: core}, where it differs
    missing: list[str]                                  # listed packages without an item


def plan(views: list[TaskView], state: State) -> Plan:
    create, update, qids = [], [], {}
    for v in views:
        if v.name in state.views:
            qid, version = state.views[v.name]
            if version != v.version:
                update.append((v, qid))
        elif v.name in REUSE:
            qid = REUSE[v.name]
            update.append((v, qid))
        else:
            create.append(v)
            continue
        qids[v.name] = qid
    want: dict[str, dict[str, bool]] = defaultdict(dict)
    missing = []
    for v in views:
        for pkg, core in v.packages.items():
            if pkg not in state.packages:
                missing.append(pkg)
            elif v.name in qids:
                want[state.packages[pkg]][qids[v.name]] = core
    known = set(qids.values())
    changed = {q: w for q, w in want.items() if w != state.members.get(q, {})}
    for q, m in state.members.items():                  # packages to leave a view
        if q not in want and m and set(m) <= known:
            changed[q] = {}
    return Plan(create, update, changed, sorted(set(missing)))


class Writer:
    """Raw API calls as the bot, so that statement IDs and hashes are those of the live wiki."""

    def __init__(self, api, call=None):
        if call is None:
            from wikibaseintegrator.wbi_helpers import mediawiki_api_call_helper as call
        self.api, self.call = api, call

    def get(self, qid: str) -> dict:
        r = self.call(data={"action": "wbgetentities", "ids": qid, "props": "aliases|claims", "format": "json"},
                      login=self.api.login, allow_anonymous=True)
        return r["entities"][qid]

    def edit(self, data: dict, summary: str, qid: str | None = None) -> str:
        req = {"action": "wbeditentity", "data": json.dumps(data), "summary": summary, "bot": "", "format": "json"}
        req.update({"id": qid} if qid else {"new": "item"})
        return self.call(data=req, login=self.api.login, is_bot=True)["entity"]["id"]

    def find(self, view: TaskView, id_pid: str) -> str | None:
        """A view item made since the SPARQL store was last updated, looked up by label."""
        r = self.call(data={"action": "wbsearchentities", "search": view.label, "language": "en", "type": "item",
                            "limit": 10, "format": "json"}, login=self.api.login, allow_anonymous=True)
        for hit in r.get("search", []):
            if hit.get("label") == view.label:
                claims = self.get(hit["id"]).get("claims", {}).get(id_pid, [])
                if any(c["mainsnak"].get("datavalue", {}).get("value") == view.name for c in claims):
                    return hit["id"]
        return None

    def resolve(self, qids: set[str]) -> dict[str, str]:
        """Merged items → the item they redirect to."""
        out = {}
        for chunk in [sorted(qids)[i:i + 50] for i in range(0, len(qids), 50)]:
            r = self.call(data={"action": "wbgetentities", "ids": "|".join(chunk), "props": "info",
                                "redirects": "yes", "format": "json"}, login=self.api.login, allow_anonymous=True)
            for e in r.get("entities", {}).values():
                if (rd := e.get("redirects")):
                    out[rd["from"]] = rd["to"]
        return out


def sync(api, dry_run: bool = False, views: list[TaskView] | None = None, state: State | None = None,
         writer: Writer | None = None) -> dict:
    """Bring the task view items and the packages' *part of* in line with CRAN. Returns a report."""
    import tempfile
    if views is None:
        views = read_views(fetch_views(Path(tempfile.gettempdir()) / "mardi_importer" / "Views.rds"))
    ids = local_ids(api)
    if state is None:
        state = read_state(ids)
    p = plan(views, state)
    log.info("CRAN task views: %d views, %d to create, %d to update, %d packages to change, %d not on the portal",
             len(views), len(p.create), len(p.update), len(p.packages), len(p.missing))
    report = {"dry_run": dry_run, "views": len(views), "create": [v.name for v in p.create],
              "update": [v.name for v, _ in p.update], "packages_to_change": len(p.packages),
              "missing_packages": p.missing, "errors": [], "packages_written": 0}
    if dry_run:
        return report

    writer = writer or Writer(api)
    people = {q for v in views for _, q in authors_of(v, state) if q}
    redirect = writer.resolve(people) if people else {}
    authors = lambda v: [(n, redirect.get(q, q) if q else None) for n, q in authors_of(v, state)]

    qids = {}
    for v in p.create:
        try:
            if (found := writer.find(v, ids["view_id"])):
                qids[v.name] = found
                if found not in {q for q, _ in state.views.values()}:
                    p.update.append((v, found))
                continue
            qids[v.name] = writer.edit(view_entity(v, ids, authors(v)), f"CRAN task view {v.name}")
            log.info("CRAN task view %s created: %s", v.name, qids[v.name])
        except Exception as exc:
            log.error("CRAN task view %s not created: %s", v.name, exc)
            report["errors"].append(f"{v.name}: {exc}")
    for v, qid in p.update:
        try:
            writer.edit(view_entity(v, ids, authors(v), writer.get(qid)), f"CRAN task view {v.name}", qid)
            qids[v.name] = qid
        except Exception as exc:
            log.error("CRAN task view %s (%s) not updated: %s", v.name, qid, exc)
            report["errors"].append(f"{v.name}: {exc}")
    qids.update({n: q for n, (q, _) in state.views.items() if n not in qids})

    view_qids = set(qids.values())
    want: dict[str, dict[str, bool]] = defaultdict(dict)
    for v in views:
        if v.name in qids:
            for pkg, core in v.packages.items():
                if pkg in state.packages:
                    want[state.packages[pkg]][qids[v.name]] = core
    todo = set(p.packages) | {state.packages[pkg] for v in p.create if v.name in qids
                              for pkg in v.packages if pkg in state.packages}
    for n, pkg in enumerate(sorted(todo, key=lambda q: int(q[1:])), 1):
        try:
            live = writer.get(pkg).get("claims", {}).get(ids["part_of"], [])
            edits = package_edits(live, want.get(pkg, {}), view_qids, ids)
            if edits:
                writer.edit({"claims": edits}, "CRAN task views", pkg)
                report["packages_written"] += 1
            if n % 500 == 0:
                log.info("CRAN task views: %d/%d packages checked", n, len(todo))
        except Exception as exc:
            log.error("CRAN task views of %s not written: %s", pkg, exc)
            report["errors"].append(f"{pkg}: {exc}")
    log.info("CRAN task views: %d packages written, %d errors", report["packages_written"], len(report["errors"]))
    return report
