"""Replace the line-break and tab codes in the review text of zbMATH papers with a space.

For every item that is a *scholarly article* with a zbMATH DE Number or zbMATH
Open document ID, each *review text* statement is cleaned:

* ``\\NEWLINE``, ``NEWLINE`` (without the backslash) and ``\\TAB`` become a space.
* ``\\N``, ``\\R`` and ``\\T`` become a space (``\\NThe`` becomes `` The``), except
  when they spell a LaTeX command such as ``\\Theta``, ``\\Re`` or ``\\Rightarrow``.

Only the value of the review text statements changes; their references and
qualifiers stay.

Usage::

    python3 zbmath_clean_review_text.py --qids Q6887239 Q6908215   # dry run on these items
    python3 zbmath_clean_review_text.py                            # dry run on all items found
    python3 zbmath_clean_review_text.py --apply                    # write the changes

Without ``--qids`` the items are found through the QLever query service (the
number is checked against the main query service) and saved in ``--found``; a
later run reuses that list. With ``--apply`` the script logs in with
MARDI_BOT_USER and MARDI_BOT_PASS (a bot password from Special:BotPasswords) and
writes each changed item. Each item handled with ``--apply`` is added to
``--done`` and skipped by later runs. The old and new text of every change is
appended to ``--changes``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time

log = logging.getLogger("zbmath-review-cleanup")

INSTANCE_OF = "wdt:P31"
SCHOLARLY_ARTICLE = "wd:Q13442814"
ZBMATH_OPEN_ID = "wdt:P894"
DE_NUMBER = "zbMATH DE Number"
REVIEW_TEXT = "review text"

QLEVER = "https://qlever.portal.mardi4nfdi.de/sparql"
PREFIXES = "PREFIX wdt: <https://portal.mardi4nfdi.de/prop/direct/>"

LONG_CODE = re.compile(r"(?<!\\)\\?NEWLINE|(?<!\\)\\TAB")
SHORT_CODE = re.compile(r"(?<!\\)\\([NRT][A-Za-z]*)")
# LaTeX commands that start like a code; these are left alone.
LATEX_COMMANDS = {"Re", "Rightarrow", "Rrightarrow", "Rightarrowtail", "Rightleftharpoons", "Rsh", "Rho",
                  "Rbag", "Rvert", "RVert", "Rangle", "Rparen", "Romannumeral",
                  "Theta", "Tau", "TeX", "Tilde", "Top", "Tr", "Thorn",
                  "Nearrow", "Nwarrow"}


def clean_review(text: str) -> str:
    """The review with its line-break and tab codes replaced by single spaces."""
    spaced = LONG_CODE.sub(" ", text)  

    def replace(match):
        code = match.group(1)
        if code in LATEX_COMMANDS:
            return match.group(0)
        return " " + code[1:]  # "\NThe" -> " The"

    cleaned = SHORT_CODE.sub(replace, spaced)
    if cleaned == text:
        return text
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()


def client(apply: bool):
    """A MardiClient; logged in as the bot when ``apply`` is set."""
    from mardiclient import MardiClient

    login = {}
    if apply:
        login = dict(user=os.environ["MARDI_BOT_USER"], password=os.environ["MARDI_BOT_PASS"], login_with_bot=True)
    api = MardiClient(**login)
    if apply and api.login is None:
        sys.exit("login failed: check MARDI_BOT_USER and MARDI_BOT_PASS")
    return api


def local_id(api, entity: str, kind: str) -> str:
    """The ID on this wiki for a Wikidata ID (``wd:``/``wdt:``) or a label."""
    found = api.get_local_id_by_label(entity, kind)
    found = found[0] if isinstance(found, list) and found else found
    if not found or found == "None":
        sys.exit(f"'{entity}' not found in this wiki")
    return found


def items_with_codes(review_pid: str, found_file: str) -> list[str]:
    """QIDs of every item whose review text holds a code.

    The list comes from the QLever query service in one request. The main query
    service then counts the same items, and a warning is logged if the numbers
    differ. The list is saved in ``found_file`` with a last line "complete", and a
    later run reads it from there.
    """
    from wikibaseintegrator.wbi_config import config
    from wikibaseintegrator.wbi_helpers import execute_sparql_query

    qids = read_lines(found_file)
    if qids and qids[-1] == "complete":
        return qids[:-1]

    config["BACKOFF_MAX_TRIES"] = 3  # the retry() around the queries waits longer between tries
    where = (f"?item wdt:{review_pid} ?review . "
             f'FILTER(REGEX(?review, "NEWLINE|\\\\\\\\(TAB|N|R|T)"))')
    rows = retry(lambda: execute_sparql_query(f"SELECT DISTINCT ?item WHERE {{ {where} }}",
                                              prefix=PREFIXES, endpoint=QLEVER))["results"]["bindings"]
    qids = [row["item"]["value"].rsplit("/", 1)[1] for row in rows]
    log.info("QLever found %d item(s)", len(qids))
    try:
        rows = retry(lambda: execute_sparql_query(f"SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE {{ {where} }}",
                                                  prefix=PREFIXES), tries=3)["results"]["bindings"]
        count = int(rows[0]["count"]["value"])
        if count == len(qids):
            log.info("the main query service counts the same number")
        else:
            log.warning("the main query service counts %d item(s), QLever found %d", count, len(qids))
    except Exception as exc:
        log.warning("could not count the items on the main query service: %s", exc)

    with open(found_file, "w") as f:
        f.writelines(q + "\n" for q in qids + ["complete"])
    return qids


def is_zbmath_paper(item, instance_pid: str, article_qid: str, id_pids: list[str]) -> bool:
    """Whether the item is a scholarly article with at least one zbMATH ID."""
    classes = [(c.mainsnak.datavalue or {}).get("value") for c in item.claims.get(instance_pid)]
    is_article = any(isinstance(v, dict) and v.get("id") == article_qid for v in classes)
    return is_article and any(item.claims.get(pid) for pid in id_pids)


def retry(action, tries: int = 6):
    """Run action(); on an error wait 2, 4, 8 ... minutes (at most 1 h) and try again."""
    for attempt in range(1, tries + 1):
        try:
            return action()
        except Exception as exc:
            if attempt == tries:
                raise
            wait = min(120 * 2 ** (attempt - 1), 3600)
            log.warning("%s; trying again in %d s", exc, wait)
            time.sleep(wait)


def read_lines(path: str) -> list[str]:
    """The non-empty lines of a file, or an empty list if it does not exist."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def main() -> int:
    """Find the items, clean their reviews and, with --apply, write them."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write the changes (default: report only)")
    parser.add_argument("--qids", nargs="*", help="only these items (default: every item found by the query)")
    parser.add_argument("--found", default="review_cleanup_found.txt",
                        help="items found by the query, reused by later runs")
    parser.add_argument("--done", default="review_cleanup_done.txt", help="items already written, skipped")
    parser.add_argument("--changes", default="review_cleanup_changes.jsonl", help="old and new text of each change")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api = client(args.apply)
    instance_pid = local_id(api, INSTANCE_OF, "property")
    article_qid = local_id(api, SCHOLARLY_ARTICLE, "item")
    id_pids = [local_id(api, ZBMATH_OPEN_ID, "property"), local_id(api, DE_NUMBER, "property")]
    review_pid = local_id(api, REVIEW_TEXT, "property")
    log.info("instance of=%s, scholarly article=%s, zbMATH IDs=%s, review text=%s",
             instance_pid, article_qid, id_pids, review_pid)

    done = set(read_lines(args.done))
    found = args.qids or items_with_codes(review_pid, args.found)
    qids = [q for q in dict.fromkeys(found) if q not in done]  # dict.fromkeys drops repeats, keeps the order
    log.info("%d item(s) to check; %s", len(qids), "APPLYING" if args.apply else "dry run")

    changed = skipped = failed = 0
    for qid in qids:
        try:
            item = retry(lambda: api.item.get(entity_id=qid))
            edits = []
            claims = item.claims.get(review_pid)
            if not is_zbmath_paper(item, instance_pid, article_qid, id_pids):
                skipped += 1
                log.info("%s: not a zbMATH scholarly article, skipped", qid)
                claims = []
            for claim in claims:
                old = (claim.mainsnak.datavalue or {}).get("value")
                if not isinstance(old, str):
                    continue
                new = clean_review(old)
                if new != old:
                    claim.mainsnak.datavalue["value"] = new  # same statement: references and qualifiers stay
                    edits.append((old, new))
            if args.apply and edits:
                retry(lambda: item.write(limit_claims=[review_pid], summary="Replace line-break codes in review text"))
            for old, new in edits:  # recorded only after a successful write
                with open(args.changes, "a") as f:
                    f.write(json.dumps({"qid": qid, "old": old, "new": new, "applied": args.apply},
                                       ensure_ascii=False) + "\n")
                changed += 1
                log.info("%s: review %s", qid, "cleaned" if args.apply else "would be cleaned")
            if args.apply:
                with open(args.done, "a") as f:
                    f.write(qid + "\n")
        except Exception as exc:  # keep going; the closing log line counts failures
            failed += 1
            log.error("%s: %s", qid, exc)

    log.info("done: %d review(s) %s, %d item(s) skipped, %d failure(s)",
             changed, "changed" if args.apply else "to change", skipped, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
