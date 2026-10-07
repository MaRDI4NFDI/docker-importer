"""Remove the *object named as* qualifier from author statements of Julia package items.

The Julia importer used to keep the author's name as stated in the package as a
qualifier next to the author item. That qualifier is no longer wanted; this
one-off script strips it from every existing Julia package item. It touches
nothing else: only the qualifier, only on *author* statements, only on items that
are an instance of *Julia package*.

Run it inside the importer pod, whose environment holds the Wikibase endpoints
and the Julia importer account::

    kubectl cp bot_scripts/julia_remove_author_name_qualifiers.py <pod>:/tmp/
    kubectl exec -it <pod> -- python3 /tmp/julia_remove_author_name_qualifiers.py           # dry run
    kubectl exec -it <pod> -- python3 /tmp/julia_remove_author_name_qualifiers.py --apply   # write

Items are found through the wiki's own search index (``haswbstatement``), not
the triple store; ``--qids`` restricts the run to the given items instead.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import requests
from mardiclient import MardiClient

log = logging.getLogger("julia-cleanup")

JULIA_PACKAGE_CLASS = "Julia package"
INSTANCE_OF = "wdt:P31"
AUTHOR = "wdt:P50"
OBJECT_NAMED_AS = "wdt:P1932"


def client() -> MardiClient:
    return MardiClient(
        user=os.environ["JULIA_USER"], password=os.environ["JULIA_PASS"],
        mediawiki_api_url=os.environ["MEDIAWIKI_API_URL"],
        sparql_endpoint_url=os.environ.get("SPARQL_ENDPOINT_URL"),
        wikibase_url=os.environ.get("WIKIBASE_URL"),
        importer_api_url=os.environ["IMPORTER_API_URL"],
        user_agent=os.environ.get("IMPORTER_MW_AGENT"),
    )


def local_id(api: MardiClient, entity: str, kind: str) -> str:
    found = api.get_local_id_by_label(entity, kind)
    found = found[0] if isinstance(found, list) else found
    if not found:
        sys.exit(f"'{entity}' not found in this wiki")
    return found


def julia_package_items(api_url: str, instance_pid: str, julia_class: str) -> list[str]:
    """QIDs of every item that is an instance of *Julia package*, from the search index."""
    qids: set[str] = set()
    offset = 0
    while True:
        r = requests.get(api_url, params={
            "action": "query", "list": "search", "srnamespace": 120, "srlimit": "max",
            "sroffset": offset, "srsearch": f"haswbstatement:{instance_pid}={julia_class}", "format": "json",
        }, timeout=60)
        r.raise_for_status()
        data = r.json()
        qids.update(s["title"].split(":", 1)[1] for s in data["query"]["search"])
        if "continue" not in data:
            break
        offset = data["continue"]["sroffset"]
    return sorted(qids, key=lambda q: int(q[1:]))


def strip_qualifiers(item, author_pid: str, named_pid: str) -> int:
    """Drop the qualifier from every author statement; returns how many statements changed."""
    changed = 0
    for claim in item.claims.get(author_pid):
        if claim.qualifiers.get(named_pid):
            claim.qualifiers.clear(named_pid)
            changed += 1
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write the changes (default: report only)")
    parser.add_argument("--qids", nargs="*", help="only these items (default: every Julia package item)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api = client()
    instance_pid = local_id(api, INSTANCE_OF, "property")
    author_pid = local_id(api, AUTHOR, "property")
    named_pid = local_id(api, OBJECT_NAMED_AS, "property")
    julia_class = local_id(api, JULIA_PACKAGE_CLASS, "item")
    qids = args.qids or julia_package_items(os.environ["MEDIAWIKI_API_URL"], instance_pid, julia_class)
    log.info("%d Julia package item(s); author=%s, qualifier=%s; %s",
             len(qids), author_pid, named_pid, "APPLYING" if args.apply else "dry run")

    items_changed = statements_changed = failed = 0
    for qid in qids:
        try:
            item = api.item.get(entity_id=qid)
            n = strip_qualifiers(item, author_pid, named_pid)
            if not n:
                continue
            items_changed += 1
            statements_changed += n
            log.info("%s %s: %d author statement(s) %s", qid, item.labels.get("en"), n,
                     "cleaned" if args.apply else "would be cleaned")
            if args.apply:
                item.write()
        except Exception as exc:  # keep going; the closing log line counts failures
            failed += 1
            log.error("%s: %s", qid, exc)
    log.info("done: %d item(s), %d statement(s) %s; %d failure(s)", items_changed, statements_changed,
             "changed" if args.apply else "to change", failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
