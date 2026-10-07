"""CRAN archive status: reading CRAN's metadata, planning end times, live edits.

No network and no Wikibase: the MaRDI client, the SPARQL store and the API call
are stubs. Comments and claims are the real ones met in the first comparison
with CRAN (October 2026).
"""

import json
import os
import sys
import unittest
from unittest.mock import Mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.cran import archive  # noqa: E402
from mardi_importer.cran.archive import Change, Listed  # noqa: E402

PACKAGES = """Package: A3
Version: 1.0.0
Depends: R (>= 2.15.0), xtable,
        pbapply

Package: nbody
Version: 1.41
"""

PACKAGES_IN = """Package: nbody
X-CRAN-History: Archived on 2023-11-28 as requires archived package
  'magicaxis'. Unarchived on 2024-08-20.

Package: dineq
X-CRAN-Comment: Archived on 2026-10-06 at the maintainer's request.

Package: mustashe
X-CRAN-Comment: Archived on 2024-01-23 as check problems were not
  corrected in time.
X-CRAN-History: Archived on 2021-05-17 as check problems were not
  corrected in time. Unarchived on 2021-06-01.

Package: NanoStringNorm
X-CRAN-Comment: Archive on 2021-07-08 as orphaned and with no remaining dependants.

Package: rmongodb
X-CRAN-Comment: Orphaned on 2016-08-25 as requested by the maintainer.
  Archived as failed its own checks against 'rmongodb' 3.

Package: biglars
X-CRAN-Comment: Archived on 2020-04-23 as check problems were not corrected
  in time. Unarchived on 2020-05-02. Archived on 2020-09-30.

Package: undated
X-CRAN-Comment: Archived as orphaned.
"""


def end_qualifier(day, h="h1"):
    return {"snaktype": "value", "property": "P411", "hash": h,
            "datavalue": {"value": {"time": f"+{day}T00:00:00Z", "precision": 11}, "type": "time"}}


def cran_claim(package, *ends, guid="Q100116$14311B5C"):
    c = {"mainsnak": {"snaktype": "value", "property": "P229",
                      "datavalue": {"value": package, "type": "string"}},
         "type": "statement", "id": guid, "rank": "normal"}
    if ends:
        c["qualifiers"] = {"P411": list(ends)}
    return c


class TestReadingCRAN(unittest.TestCase):
    def test_dcf_joins_continuation_lines(self):
        recs = archive.parse_dcf(PACKAGES)
        self.assertEqual([r["Package"] for r in recs], ["A3", "nbody"])
        self.assertEqual(recs[0]["Depends"], "R (>= 2.15.0), xtable, pbapply")

    def test_on_cran(self):
        self.assertEqual(archive.on_cran(PACKAGES), {"A3", "nbody"})

    def test_archive_dates(self):
        got = archive.archived(PACKAGES_IN)
        self.assertEqual(got, {
            "dineq": "2026-10-06",
            "mustashe": "2024-01-23",          # the comment, not the history
            "NanoStringNorm": "2021-07-08",    # "Archive on"
            "rmongodb": "2016-08-25",          # undated archival: latest date in the comment
            "biglars": "2020-09-30",           # latest archival, not the unarchival
        })
        self.assertNotIn("nbody", got)         # history only: it is back on CRAN
        self.assertNotIn("undated", got)

    def test_archive_date_ignores_impossible_dates(self):
        self.assertIsNone(archive.archive_date("Archived on 2021-13-45."))

    def test_truncated_index_is_refused(self):
        session = Mock()
        session.headers = {}
        session.get.return_value.text = PACKAGES
        with self.assertRaises(RuntimeError):
            archive.fetch_cran(session)


class TestPlan(unittest.TestCase):
    current = {"A3", "nbody"}
    arch = {"dineq": "2026-10-06", "mustashe": "2024-01-23"}

    def test_rules(self):
        listed = [
            Listed("Q1", "A3", None),                 # on CRAN, no end time: nothing
            Listed("Q2", "nbody", "2022-08-16"),      # back on CRAN: remove
            Listed("Q3", "dineq", None),              # archived, unflagged: add
            Listed("Q4", "mustashe", "2021-05-17"),   # last release date: correct
            Listed("Q5", "limma", "2007-09-24"),      # never on CRAN: left, reported
        ]
        changes, unresolved = archive.plan(listed, self.current, self.arch)
        self.assertEqual(changes, [
            Change("Q3", "dineq", None, "2026-10-06"),
            Change("Q4", "mustashe", "2021-05-17", "2024-01-23"),
            Change("Q2", "nbody", "2022-08-16", None),
        ])
        self.assertEqual([c.action for c in changes], ["add", "correct", "remove"])
        self.assertEqual(unresolved, [Listed("Q5", "limma", "2007-09-24")])

    def test_correct_state_plans_nothing(self):
        listed = [Listed("Q1", "A3", None), Listed("Q3", "dineq", "2026-10-06")]
        self.assertEqual(archive.plan(listed, self.current, self.arch), ([], []))


class TestLiveEdits(unittest.TestCase):
    def test_add(self):
        (e,) = archive.edits_for([cran_claim("dineq")], "dineq", "P411", "2026-10-06")
        self.assertEqual(e["action"], "wbsetqualifier")
        self.assertNotIn("snakhash", e)
        self.assertEqual(json.loads(e["value"])["time"], "+2026-10-06T00:00:00Z")
        self.assertEqual(json.loads(e["value"])["precision"], 11)

    def test_correct_replaces_in_place(self):
        claim = cran_claim("mustashe", end_qualifier("2021-05-17", "old"))
        (e,) = archive.edits_for([claim], "mustashe", "P411", "2024-01-23")
        self.assertEqual((e["action"], e["snakhash"]), ("wbsetqualifier", "old"))

    def test_remove(self):
        claim = cran_claim("nbody", end_qualifier("2022-08-16", "a"), end_qualifier("2023-01-01", "b"))
        self.assertEqual(archive.edits_for([claim], "nbody", "P411", None),
                         [{"action": "wbremovequalifiers", "claim": "Q100116$14311B5C", "qualifiers": "a|b"}])

    def test_extra_end_times_are_dropped(self):
        claim = cran_claim("dineq", end_qualifier("2020-01-01", "a"), end_qualifier("2026-10-06", "b"))
        self.assertEqual(archive.edits_for([claim], "dineq", "P411", "2026-10-06"),
                         [{"action": "wbremovequalifiers", "claim": "Q100116$14311B5C", "qualifiers": "a"}])

    def test_already_right_or_other_value_untouched(self):
        self.assertEqual(archive.edits_for([cran_claim("dineq", end_qualifier("2026-10-06"))],
                                           "dineq", "P411", "2026-10-06"), [])
        self.assertEqual(archive.edits_for([cran_claim("other")], "dineq", "P411", "2026-10-06"), [])

    def test_apply_reads_live_item_and_writes_as_bot(self):
        api = Mock()
        api.item.get.return_value.get_json.return_value = {"claims": {"P229": [cran_claim("dineq")]}}
        call = Mock()
        n = archive.apply(api, Change("Q3", "dineq", None, "2026-10-06"), "P229", "P411", call=call)
        self.assertEqual(n, 1)
        api.item.get.assert_called_once_with(entity_id="Q3")
        kwargs = call.call_args.kwargs
        self.assertTrue(kwargs["is_bot"])
        self.assertIs(kwargs["login"], api.login)
        self.assertIn("archived on CRAN on 2026-10-06", kwargs["data"]["summary"])

    def test_apply_skips_when_live_item_is_already_right(self):
        api = Mock()
        api.item.get.return_value.get_json.return_value = {
            "claims": {"P229": [cran_claim("dineq", end_qualifier("2026-10-06"))]}}
        call = Mock()
        self.assertEqual(archive.apply(api, Change("Q3", "dineq", None, "2026-10-06"), "P229", "P411", call=call), 0)
        call.assert_not_called()


class TestSync(unittest.TestCase):
    def api(self):
        api = Mock()
        api.get_local_id_by_label.side_effect = lambda p, _: {"wdt:P5565": "P229", "wdt:P582": "P411"}[p]
        return api

    def test_read_listed(self):
        b = lambda q, n, end=None: {"item": {"value": f"https://portal.mardi4nfdi.de/entity/{q}"},
                                    "name": {"value": n},
                                    **({"end": {"value": f"{end}T00:00:00Z"}} if end else {})}
        sparql = Mock(return_value={"results": {"bindings": [
            b("Q1", "A3"), b("Q2", "nbody", "2022-08-16"), b("Q2", "nbody", "2023-01-01")]}})
        got = archive.read_listed(self.api(), sparql=sparql)
        self.assertEqual(sorted(got, key=lambda x: x.qid),
                         [Listed("Q1", "A3", None), Listed("Q2", "nbody", "2023-01-01")])
        self.assertIn("p:P229", sparql.call_args.args[0])
        self.assertIn("pq:P411", sparql.call_args.args[0])

    def test_dry_run_writes_nothing(self):
        api = self.api()
        report = archive.sync(api, dry_run=True, current={"A3", "nbody"}, archive={"dineq": "2026-10-06"},
                              listed=[Listed("Q2", "nbody", "2022-08-16"), Listed("Q3", "dineq", None),
                                      Listed("Q5", "limma", "2007-09-24")])
        self.assertEqual(report["changes"], {"add": 1, "remove": 1})
        self.assertEqual([r["package"] for r in report["unresolved"]], ["limma"])
        api.item.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
