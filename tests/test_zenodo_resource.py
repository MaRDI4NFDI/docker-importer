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
        "wdt:P9934": "P1506", "community": "P1495", "wdt:P50": "P16", "wdt:P2093": "P43",
        "wdt:P1343": "P286", "wdt:P2860": "P223", "wdt:P144": "P408", "wdt:P361": "P37", "wdt:P527": "P265"}


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
        for item in (self.api.item.get.return_value, self.api.item.new.return_value):
            item.claims.get.return_value = []
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

    def test_label_clash_on_update_keeps_dated_description(self):
        rec = record("7", 0, True)
        rec["metadata"]["resource_type"] = {"title": "Presentation"}
        self.responses["/records/7"] = rec
        self.sparql.return_value = bindings([{"item": "https://x/entity/Q50", "owned": "true"}])
        ZenodoResource._community_items = {}
        item = self.api.item.get.return_value
        item.write.side_effect = [Mock(id="Q9"), Mock(id="Q50")]
        self.assertEqual(self.resource("7").create(update=True), "Q50")
        item.descriptions.set.assert_called_with(
            language="en", value="Presentation published at Zenodo repository on 2024-01-01.")

    def test_unresolved_clash_fails(self):
        self.responses["/records/7"] = record("7", 0, True)
        self.sparql.return_value = bindings([{"item": "https://x/entity/Q50", "owned": "true"}])
        ZenodoResource._community_items = {}
        self.api.item.get.return_value.write.return_value = Mock(id="Q9")
        with self.assertRaises(ValueError):
            self.resource("7").create(update=True)


class FakeAuthor:
    """Name-matching stand-in for ``utils.Author``."""

    def __init__(self, api, name, orcid=None, affiliation=None, _aliases=(), _QID=None):
        last, _, first = name.partition(", ")
        self.name, self.orcid, self.QID = f"{first} {last}" if first else name, orcid, _QID

    def pull_QID(self, pool):
        self.QID = next((a.QID for a in pool if a.name == self.name), None)


class RealClaimsTest(ZenodoTest):
    """Real WikibaseIntegrator claims on the item being updated."""

    def setUp(self):
        super().setUp()
        # Other test modules may leave stubs of these in sys.modules.
        real = ("wikibaseintegrator", "requests")
        stubs = {k: sys.modules.pop(k) for k in list(sys.modules) if k.startswith(real)}
        self.addCleanup(sys.modules.update, stubs)
        datatypes = importlib.import_module("wikibaseintegrator.datatypes")
        Claims = importlib.import_module("wikibaseintegrator.models").Claims
        actions = importlib.import_module("wikibaseintegrator.wbi_enums").ActionIfExists
        self.addCleanup(lambda: [sys.modules.pop(k) for k in list(sys.modules) if k.startswith(real)])
        for name, value in [("ExternalID", datatypes.ExternalID), ("String", datatypes.String),
                            ("Time", datatypes.Time), ("ItemValue", datatypes.Item), ("ActionIfExists", actions)]:
            p = patch.object(module, name, value)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(module, "Author", FakeAuthor)
        p.start()
        self.addCleanup(p.stop)
        self.datatypes = datatypes
        self.api.get_claim.side_effect = lambda prop, value: (
            datatypes.Item(value, prop_nr=PIDS[prop]) if prop == "wdt:P50"
            else datatypes.String(value, prop_nr=PIDS[prop]))
        self.claims = Claims()
        for n, (dt, value, prop) in enumerate([(datatypes.Item, "Q100", "P16"), (datatypes.String, "Jane Roe", "P43"),
                                               (datatypes.String, "Old Name", "P43")]):
            claim = dt(value, prop_nr=prop)
            claim.id = f"Q50${n}"
            self.claims.add(claim, actions.FORCE_APPEND)
        item = self.api.item.get.return_value
        item.claims = self.claims
        item.write.return_value = Mock(id="Q50")
        item.get_value.side_effect = lambda p: ["Q100"] if p == "wdt:P50" else []
        item.labels.get.return_value = "John Doe"
        item.aliases.get.return_value = None
        self.sparql.return_value = bindings([{"item": "https://x/entity/Q50", "owned": "true"}])
        ZenodoResource._community_items = {}
        self.actions = actions

    def values(self, prop):
        return [(c.mainsnak.datavalue["value"], c.removed) for c in self.claims.get(prop)]


class TestUpdateAuthors(RealClaimsTest):
    """Unchanged authors must survive an update."""

    def test_unchanged_authors_are_kept_and_others_replaced(self):
        rec = record("7", 0, True)
        rec["metadata"]["creators"] = [{"name": "Doe, John"}, {"name": "Roe, Jane"}]
        self.responses["/records/7"] = rec
        self.resource("7").create(update=True)
        self.assertEqual([(v["id"] if isinstance(v, dict) else v, r) for v, r in self.values("P16")],
                         [("Q100", False)])
        self.assertEqual(self.values("P43"), [("Jane Roe", False), ("Old Name", True)])

    def test_no_creators_leaves_authors_alone(self):
        self.responses["/records/7"] = record("7", 0, True)
        self.resource("7").create(update=True)
        self.assertFalse(any(c.removed for p in ("P16", "P43") for c in self.claims.get(p)))


class TestResourceType(ZenodoTest):
    def test_articles_and_books_get_publication_profile(self):
        for title, expected in [("Journal article", "wd:Q13442814"), ("Book", "wd:Q571")]:
            rec = record("7", 0, True)
            rec["metadata"]["resource_type"] = {"title": title}
            self.responses["/records/7"] = rec
            r = self.resource("7")
            self.assertEqual((r.resource_type, r._mardi_type), (expected, "MaRDI publication profile"))

    def test_workflow_gets_workflow_profile(self):
        rec = record("7", 0, True)
        rec["metadata"]["resource_type"] = {"title": "Workflow"}
        self.responses["/records/7"] = rec
        r = self.resource("7")
        self.assertEqual((r.resource_type, r._mardi_type), ("wd:Q115682347", "MaRDI workflow profile"))

    def test_profile_label_is_passed_as_item_id(self):
        rec = record("7", 0, True)
        rec["metadata"]["resource_type"] = {"title": "Workflow"}
        self.responses["/records/7"] = rec
        ids = {**PIDS, "MaRDI workflow profile": ["Q6534216"]}
        self.api.get_local_id_by_label.side_effect = lambda p, _: ids[p]
        self.sparql.return_value = bindings([{"item": "https://x/entity/Q50", "owned": "true"}])
        ZenodoResource._community_items = {}
        self.api.item.get.return_value.write.return_value = Mock(id="Q50")
        self.resource("7").create(update=True)
        self.api.item.get.return_value.add_claim.assert_any_call(
            "MaRDI profile type", "Q6534216", action="replace_all")


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


class TestRelatedWorks(ZenodoTest):
    def setUp(self):
        super().setUp()
        self.responses["/records/7"] = rec = record("7", 0, True, concept="6")
        rec["metadata"]["related_identifiers"] = [
            {"relation": "isDescribedBy", "scheme": "doi", "identifier": "10.5281/zenodo.15304389"},
            {"relation": "isDocumentedBy", "scheme": "doi", "identifier": "10.1007/978-1-4757-3502-4_6."},
            {"relation": "isDocumentedBy", "scheme": "doi", "identifier": "https://doi.org/10.5281/zenodo.15304389"},
            {"relation": "cites", "scheme": "arxiv", "identifier": "arXiv:2402.17665"},
            {"relation": "isSupplementTo", "scheme": "doi", "identifier": "10.1/skipped"},
            {"relation": "isPartOf", "scheme": "url", "identifier": "https://example.org"},
            {"relation": "hasPart", "scheme": "doi", "identifier": "10.5281/zenodo.6"},
        ]
        self.arxiv, self.crossref = Mock(), Mock()
        self.arxiv.ArxivPublication.return_value.create.return_value = "Q20"
        self.crossref.CrossrefPublication.return_value.create.return_value = "Q30"
        p = patch.dict(sys.modules, {"mardi_importer.arxiv.ArxivPublication": self.arxiv,
                                     "mardi_importer.crossref.CrossrefPublication": self.crossref})
        p.start()
        self.addCleanup(p.stop)

    def test_mapped_relations_are_imported_and_grouped(self):
        r = self.resource("7")
        with patch.object(module, "ZenodoResource") as zenodo:
            zenodo.return_value.create.return_value = "Q10"
            related = r.related_works()
        self.assertEqual(related, {"P286": ["Q10", "Q30"], "P223": ["Q20"]})
        zenodo.assert_called_once_with("15304389", api=self.api, wdi=r.wdi, link_related=False)
        self.crossref.CrossrefPublication.assert_called_once_with("10.1007/978-1-4757-3502-4_6")
        self.arxiv.ArxivPublication.assert_called_once_with("2402.17665")

    def test_arxiv_doi_goes_to_arxiv_importer(self):
        self.assertEqual(self.resource("7")._related_item("doi", "10.48550/arXiv.2310.20443"), "Q20")
        self.arxiv.ArxivPublication.assert_called_once_with("2310.20443")

    def test_failed_import_is_skipped(self):
        self.crossref.CrossrefPublication.side_effect = ValueError("not in Crossref")
        r = self.resource("7")
        with patch.object(module, "ZenodoResource") as zenodo, self.assertLogs(module.log, "WARNING"):
            zenodo.return_value.create.return_value = "Q10"
            self.assertEqual(r.related_works()["P286"], ["Q10"])


class TestRelationClaims(RealClaimsTest):
    def setUp(self):
        super().setUp()
        for n, value in enumerate(["Q300", "Q301", "Q302"]):
            claim = self.datatypes.Item(value, prop_nr="P286")
            claim.id = f"Q50$r{n}"
            self.claims.add(claim, self.actions.FORCE_APPEND)
        self.responses["/records/7"] = record("7", 0, True)

    def test_update_mirrors_zenodo(self):
        item = self.api.item.get.return_value
        self.resource("7")._relation_claims(item, {"P286": ["Q302", "Q303"]}, update=True)
        self.assertEqual([(v["id"], r) for v, r in self.values("P286")],
                         [("Q300", True), ("Q301", True), ("Q302", False), ("Q303", False)])
        self.assertEqual(len(self.claims.get("P286")[-1].references), 0)

    def test_new_item_removes_nothing(self):
        item = self.api.item.get.return_value
        self.resource("7")._relation_claims(item, {}, update=False)
        self.assertFalse(any(c.removed for c in self.claims.get("P286")))

    def test_failed_lookup_keeps_statements(self):
        item = self.api.item.get.return_value
        r = self.resource("7")
        r._related_incomplete = True
        r._relation_claims(item, {"P286": ["Q303"]}, update=True)
        self.assertEqual([(v["id"], r) for v, r in self.values("P286")],
                         [("Q300", False), ("Q301", False), ("Q302", False), ("Q303", False)])
