"""Zenodo importer: one item per concept, found by concept DOI, version DOI or record ID.

No network and no Wikibase: Zenodo responses, the SPARQL store and the
WikibaseIntegrator datatypes are faked.
"""

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

import importlib  # noqa: E402

module = importlib.import_module("mardi_importer.zenodo.ZenodoResource")
ZenodoResource = module.ZenodoResource

PIDS = {"wdt:P4901": "P227", "wdt:P356": "P27", "wdt:P393": "P131", "wdt:P577": "P28",
        "wdt:P9934": "P1506", "community": "P1495"}


def snak(kind):
    return lambda value, prop_nr, qualifiers=(): SimpleNamespace(
        kind=kind, value=value, prop=prop_nr, qualifiers=list(qualifiers))


def record(rid, index, last, concept="100", version=None, date="2024-01-0{}", communities=()):
    meta = {"title": f"Record {rid}", "publication_date": date.format(index + 1),
            "relations": {"version": [{"index": index, "is_last": last}]},
            "communities": [{"id": c} for c in communities]}
    if version:
        meta["version"] = version
    return {"id": int(rid), "conceptrecid": concept, "conceptdoi": f"10.5281/zenodo.{concept}",
            "doi": f"10.5281/zenodo.{rid}", "metadata": meta}


def bindings(rows):
    return {"results": {"bindings": [
        {k: {"value": v} for k, v in row.items()} for row in rows]}}


class ZenodoTest(unittest.TestCase):
    def setUp(self):
        self.api = Mock()
        self.api.get_local_id_by_label.side_effect = lambda p, _: PIDS[p]
        self.api.item.get.return_value.get_value.return_value = []
        self.responses = {}
        self.sparql = Mock(return_value=bindings([]))
        for name, value in [("zenodo_get", lambda path: self.responses[path]),
                            ("execute_sparql_query", self.sparql),
                            ("ExternalID", snak("id")), ("String", snak("str")), ("Time", snak("time"))]:
            p = patch.object(module, name, value)
            p.start()
            self.addCleanup(p.stop)
        ZenodoResource._community_items = None

    def resource(self, zenodo_id):
        return ZenodoResource(zenodo_id, api=self.api, wdi=Mock())

    def versions(self, n, concept="100"):
        hits = [record(str(1000 + i), i, i == n - 1, concept) for i in reversed(range(n))]
        self.responses[f"/records/{1000 + n - 1}/versions?size=20&sort=version"] = {"hits": {"hits": hits}}
        for h in hits:
            self.responses[f"/records/{h['id']}"] = h
            self.responses[f"/records/{h['id']}/versions?size=20&sort=version"] = {"hits": {"hits": hits}}
        self.responses[f"/records/{concept}"] = hits[0]
        return hits


class TestLookup(ZenodoTest):
    def test_concept_id_resolves_to_latest_record(self):
        self.versions(3)
        r = self.resource("100")
        self.assertEqual(r.zenodo_id, "1002")
        self.assertEqual(r.concept_doi, "10.5281/ZENODO.100")

    def test_old_version_uses_latest_metadata(self):
        self.versions(3)
        r = self.resource("1000")
        self.assertEqual((r.zenodo_id, r.title), ("1002", "Record 1002"))

    def test_query_matches_dois_and_record_ids(self):
        self.versions(2)
        self.resource("1000")
        query = self.sparql.call_args[0][0]
        for value in ['"10.5281/ZENODO.100"', '"10.5281/zenodo.100"', '"10.5281/ZENODO.1000"',
                      '"1000"', '"1001"', '"100"', "wdt:P27", "wdt:P227"]:
            self.assertIn(value, query)

    def test_prefers_own_item_then_oldest(self):
        self.versions(1)
        e = "https://portal.mardi4nfdi.de/entity/"
        self.sparql.return_value = bindings([{"item": e + "Q50", "owned": "false"},
                                             {"item": e + "Q90", "owned": "true"},
                                             {"item": e + "Q70", "owned": "true"}])
        with self.assertLogs(module.log, "WARNING"):
            r = self.resource("1000")
        self.assertEqual((r.QID, r.owned), ("Q70", True))

    def test_doi_only_item_is_linked_not_rewritten(self):
        self.versions(1)
        self.sparql.return_value = bindings([{"item": "https://x/entity/Q50", "owned": "false"}])
        r = self.resource("1000")
        self.assertEqual(r.create(update=True), "Q50")
        self.api.item.get.assert_not_called()
        self.api.item.new.assert_not_called()


class TestIdentifiers(ZenodoTest):
    def test_single_version_needs_no_versions_call(self):
        self.responses["/records/7"] = record("7", 0, True, concept="6", version="1.0")
        r = self.resource("7")
        ids, doi = r.identifier_claims()
        self.assertEqual([(c.value, [(q.kind, q.value) for q in c.qualifiers]) for c in ids],
                         [("7", [("str", "1.0"), ("time", "+2024-01-01T00:00:00Z")])])
        self.assertEqual((doi.prop, doi.value), ("P27", "10.5281/ZENODO.6"))

    def test_version_label_falls_back_to_zenodo_numbering(self):
        self.versions(2)
        ids, _ = self.resource("1001").identifier_claims()
        self.assertEqual([c.qualifiers[0].value for c in ids], ["v2", "v1"])

    def test_keeps_newest_twenty(self):
        self.versions(25)
        ids, _ = self.resource("1024").identifier_claims()
        self.assertEqual([c.value for c in ids], [str(1000 + i) for i in range(24, 4, -1)])


class TestCreate(ZenodoTest):
    def test_label_clash_retries_with_dated_description(self):
        rec = record("7", 0, True)
        rec["metadata"]["resource_type"] = {"title": "Presentation"}
        self.responses["/records/7"] = rec
        holding = lambda rid: Mock(id=f"Q{rid}", **{"claims.get.return_value": [
            SimpleNamespace(mainsnak=SimpleNamespace(datavalue={"value": rid}))]})
        item = self.api.item.new.return_value
        item.write.side_effect = [holding("9"), holding("7")]
        self.assertEqual(self.resource("7").create(), "Q7")
        item.descriptions.set.assert_called_with(
            language="en", value="Presentation published at Zenodo repository on 2024-01-01.")


class TestResourceType(ZenodoTest):
    def test_articles_and_books_get_publication_profile(self):
        for title, expected in [("Journal article", "wd:Q13442814"), ("Book", "wd:Q571")]:
            rec = record("7", 0, True)
            rec["metadata"]["resource_type"] = {"title": title}
            self.responses["/records/7"] = rec
            r = self.resource("7")
            self.assertEqual((r.resource_type, r._mardi_type), (expected, "MaRDI publication profile"))


class TestCommunities(ZenodoTest):
    def test_only_communities_with_an_item(self):
        self.responses["/records/7"] = record("7", 0, True, communities=("mardigmci", "eu", "mardi"))
        self.sparql.side_effect = [
            bindings([]),
            bindings([{"item": "https://x/entity/Q6038424", "slug": "mardigmci"},
                      {"item": "https://x/entity/Q6038405", "slug": "mardi"}]),
        ]
        r = self.resource("7")
        self.assertEqual([c.QID for c in r.communities], ["Q6038424", "Q6038405"])


if __name__ == "__main__":
    unittest.main()
