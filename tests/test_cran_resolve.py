"""CRAN importer: resolving people to items, and updating a package's people.

No network and no Wikibase: the wiki, the ORCID registry and the package item
are stubs. Cases are the ones fixed by hand in October 2026, which a rerun of the
importer must not undo.
"""

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.cran.authors import Entry  # noqa: E402
from mardi_importer.cran.people import GraphPerson  # noqa: E402
from mardi_importer.cran.resolve import People  # noqa: E402

SCOTT, AARON = "0000-0003-1444-9135", "0000-0003-2542-2202"
JENNY, LUCY = "0000-0002-6983-2759", "0000-0001-7297-9359"


class FakeWiki:
    def __init__(self, labels, orcids=None, redirects=None):
        self.labels, self.orcids, self.redirects = dict(labels), orcids or {}, redirects or {}
        self.created = []

    def lookup(self, qid):
        qid = self.redirects.get(qid, qid)
        return (qid, self.labels[qid]) if qid in self.labels else None

    def items_with_orcid(self, orcid):
        return list(self.orcids.get(orcid, []))

    def create_person(self, name, orcid):
        qid = f"Q{900 + len(self.created)}"
        self.created.append((qid, name, orcid))
        self.labels[qid] = name
        if orcid:
            self.orcids.setdefault(orcid, []).append(qid)
        return qid


class Registry:
    def __init__(self, names):
        self.names = names

    def name(self, orcid):
        return self.names.get(orcid)


REGISTRY = Registry({SCOTT: "Scott Chamberlain", AARON: "Aaron Wolen", JENNY: "Jennifer Bryan",
                     LUCY: "Lucy D'Agostino McGowan"})


def e(pkg, name, roles=("aut",), orcid=None, email=None, source="Authors@R", family=None, kind="person"):
    return Entry(pkg, name, source, roles, family=family, orcid=orcid, email=email, kind=kind)


class TestResolve(unittest.TestCase):
    def test_hand_fixed_link_stays_and_a_borrowed_orcid_is_not_used(self):
        # hoardr gives Scott Chamberlain Aaron Wolen's ORCID; hoardr was pointed to Scott by hand.
        wiki = FakeWiki({"Q57170": "Scott Chamberlain", "Q79341": "Aaron Wolen"}, {AARON: ["Q79341"]})
        people = People(wiki, REGISTRY)
        scott = e("hoardr", "Scott Chamberlain", orcid=AARON, family="Chamberlain")
        r = people.resolve(scott, [("Q57170", "Scott Chamberlain")])
        self.assertEqual((r.qid, r.how), ("Q57170", "already linked"))
        r = people.resolve(scott, [])                         # even without the link: never Aaron's item
        self.assertNotEqual(r.qid, "Q79341")
        self.assertTrue(any("registered to 'Aaron Wolen'" in n for n in people.notes))

    def test_same_person_found_through_other_packages(self):
        # quartets gives Lucy D'Agostino McGowan Jennifer Bryan's ORCID; her address joins her
        # to pald, which links her own item.
        quartets = e("quartets", "Lucy D'Agostino McGowan", ("aut", "cre"), JENNY, "lucy@x")
        pald = e("pald", "Lucy D'Agostino McGowan", ("aut", "cre"), LUCY, "lucy@x")
        graph = {"Q92834": GraphPerson("Q92834", "Lucy D'Agostino McGowan", {LUCY}, [("pald", "author")]),
                 "Q68925": GraphPerson("Q68925", "Jennifer Bryan", {JENNY}, [("other", "author")])}
        wiki = FakeWiki({"Q92834": "Lucy D'Agostino McGowan", "Q68925": "Jennifer Bryan"},
                        {JENNY: ["Q68925"], LUCY: ["Q92834"]})
        people = People.build(wiki, [quartets, pald], graph, REGISTRY)
        r = people.resolve(quartets, [])
        self.assertEqual((r.qid, r.how), ("Q92834", "same person on CRAN"))

    def test_orcid_on_a_differently_named_item_is_not_followed(self):
        # taxonbridge: a private ORCID record, on an item of another name.
        wiki = FakeWiki({"Q103928": "Werner Veldsman"}, {"0000-0001-9837-8332": ["Q103928"]})
        people = People(wiki, Registry({}))
        r = people.resolve(e("taxonbridge", "Marc Robinson-Rechavi", orcid="0000-0001-9837-8332"), [])
        self.assertEqual((r.kind, r.qid), ("string", None))
        self.assertEqual(wiki.created, [])

    def test_verified_orcid_finds_the_item(self):
        wiki = FakeWiki({"Q57170": "Scott Chamberlain"}, {SCOTT: ["Q57170"]})
        r = People(wiki, REGISTRY).resolve(e("vcr", "Scott Chamberlain", orcid=SCOTT), [])
        self.assertEqual((r.qid, r.how), ("Q57170", "ORCID"))

    def test_new_person_created_once(self):
        wiki = FakeWiki({})
        a, b = e("p1", "Jane Roe", ("cre",), email="jane@x", source="Maintainer"), \
            e("p2", "Jane Roe", ("cre",), email="jane@x", source="Maintainer")
        people = People.build(wiki, [a, b], {}, Registry({}))
        r1, r2 = people.resolve(a, []), people.resolve(b, [])
        self.assertEqual(r1.qid, r2.qid)
        self.assertEqual(len(wiki.created), 1)

    def test_items_merged_since_count_as_one(self):
        # Hadley Wickham: packages still link items merged into Q62906 since.
        mentions = [e(p, "Hadley Wickham", email="h@x", family="Wickham") for p in ("ggplot2", "dplyr", "vcr")]
        graph = {"Q62906": GraphPerson("Q62906", "Hadley Wickham", links=[("ggplot2", "author")]),
                 "Q71344": GraphPerson("Q71344", "Hadley Wickham", links=[("dplyr", "author")])}
        wiki = FakeWiki({"Q62906": "Hadley Wickham"}, redirects={"Q71344": "Q62906"})
        r = People.build(wiki, mentions, graph, Registry({})).resolve(mentions[2], [])
        self.assertEqual((r.qid, r.how), ("Q62906", "same person on CRAN"))
        self.assertEqual(wiki.created, [])

    def test_one_person_under_two_addresses(self):
        old, new = e("dplyr", "Hadley Wickham", email="h@rstudio"), e("vcr", "Hadley Wickham", email="h@posit")
        graph = {"Q62906": GraphPerson("Q62906", "Hadley Wickham", links=[("dplyr", "author"), ("vcr", "author")])}
        wiki = FakeWiki({"Q62906": "Hadley Wickham"})
        r = People.build(wiki, [old, new], graph, Registry({})).resolve(new, [])
        self.assertEqual(r.qid, "Q62906")

    def test_who_gets_a_new_item(self):
        once = e("p1", "Ann Lee", email="ann@x")
        twice = [e("p2", "Bob Kay", email="bob@x"), e("p3", "Bob Kay", email="bob@x")]
        people = People.build(FakeWiki({}), [once, *twice], {}, Registry({}))
        self.assertEqual(people.resolve(once, []).kind, "string")       # one package, address only
        self.assertEqual(people.resolve(twice[0], []).kind, "item")     # several packages

    def test_name_only_orphaned_and_organisations(self):
        people = People(FakeWiki({}), Registry({}))
        self.assertEqual(people.resolve(e("p", "John Doe"), []).kind, "string")
        self.assertEqual(people.resolve(e("p", "ORPHANED", kind="orphaned"), []).kind, "none")
        self.assertEqual(people.resolve(e("p", "Posit, PBC", kind="organisation"), []).kind, "string")

    def test_an_item_standing_for_two_people_is_not_used_to_find_either(self):
        a = e("p1", "Ann Lee", email="ann@x")
        b = e("p2", "Bob Kay", email="bob@x")
        graph = {"Q5": GraphPerson("Q5", "Ann Lee", links=[("p1", "author"), ("p2", "author")])}
        people = People.build(FakeWiki({"Q5": "Ann Lee"}), [a, b], graph, Registry({}))
        self.assertNotIn(people.person_of[id(b)].id, people.items_of_person)


# -- RPackage.apply_people --------------------------------------------------------------

class Claim:
    def __init__(self, value):
        self.mainsnak = SimpleNamespace(datavalue={"value": value})
        self.removed = False

    def remove(self, remove=True):
        self.removed = remove


class FakeItem:
    PIDS = {"wdt:P50": "P16", "wdt:P126": "P19", "wdt:P2093": "P43"}

    def __init__(self, qid, claims):
        self.id = qid
        self.claims = SimpleNamespace(get=lambda pid: self._claims.get(pid, []))
        self._claims = {self.PIDS[p]: [Claim({"id": v} if p != "wdt:P2093" else v) for v in vs]
                        for p, vs in claims.items()}
        self.added = []

    def add_claim(self, prop, value, **kw):
        self.added.append((prop, value))

    def removed(self):
        return [(p, c.mainsnak.datavalue["value"]) for p, cs in self._claims.items() for c in cs if c.removed]


def package(entries, claims, wiki, registry=REGISTRY):
    from mardi_importer.cran.RPackage import RPackage
    api = Mock()
    api.get_local_id_by_label.side_effect = lambda p, _: FakeItem.PIDS[p]
    pkg = RPackage("2026-10-01", "pkg", "Title", entries=entries, people=People(wiki, registry),
                   api=api, wdi=Mock(), crossref=Mock(), arxiv=Mock(), zenodo=Mock())
    pkg._item = FakeItem("Q1", claims)
    return pkg


class TestApplyPeople(unittest.TestCase):
    def test_hoardr_rerun_changes_nothing(self):
        wiki = FakeWiki({"Q57170": "Scott Chamberlain", "Q99": "Tamás Stirling", "Q79341": "Aaron Wolen"},
                        {AARON: ["Q79341"]})
        entries = [e("hoardr", "Scott Chamberlain", orcid=AARON, family="Chamberlain"),
                   e("hoardr", "Tamás Stirling", ("aut", "cre"), family="Stirling"),
                   e("hoardr", "Tamás Stirling", ("cre",), email="t@x", source="Maintainer")]
        pkg = package(entries, {"wdt:P50": ["Q57170", "Q99"], "wdt:P126": ["Q99"]}, wiki)
        pkg.apply_people()
        self.assertEqual(pkg.item.added, [])
        self.assertEqual(pkg.item.removed(), [])

    def test_link_to_a_merged_item_counts_as_present(self):
        wiki = FakeWiki({"Q92834": "Lucy D'Agostino McGowan"}, {LUCY: ["Q92834"]},
                        redirects={"Q141972": "Q92834"})
        entries = [e("tidycode", "Lucy D'Agostino McGowan", ("aut", "cre"), LUCY),
                   e("tidycode", "Lucy D'Agostino McGowan", ("cre",), email="l@x", source="Maintainer")]
        pkg = package(entries, {"wdt:P50": ["Q92834"], "wdt:P126": ["Q141972"]}, wiki)
        pkg.apply_people()
        self.assertEqual(pkg.item.added, [])
        self.assertEqual(pkg.item.removed(), [])

    def test_maintainer_change_string_upgrade_and_orphaned(self):
        wiki = FakeWiki({"Q7": "Old Maintainer"})
        entries = [e("p", "Jane Roe", ("aut", "cre"), family="Roe"),
                   e("p", "Jane Roe", ("cre",), email="jane@x", source="Maintainer")]
        pkg = package(entries, {"wdt:P50": [], "wdt:P126": ["Q7"], "wdt:P2093": ["Jane Roe", "John Doe"]}, wiki)
        pkg.apply_people()
        new = wiki.created[0][0]
        self.assertEqual(sorted(pkg.item.added), [("wdt:P126", new), ("wdt:P50", new)])
        self.assertEqual(sorted(pkg.item.removed()), [("P19", {"id": "Q7"}), ("P43", "Jane Roe")])

        pkg = package([e("p", "ORPHANED", ("cre",), source="Maintainer", kind="orphaned")],
                      {"wdt:P126": ["Q7"]}, FakeWiki({"Q7": "Old Maintainer"}))
        pkg.apply_people()
        self.assertEqual(pkg.item.removed(), [("P19", {"id": "Q7"})])
        self.assertEqual(pkg.item.added, [])

    def test_nothing_known_changes_nothing(self):
        for entries in (None, [], [e("p", "Jane Roe", family="Roe")]):        # last: no Maintainer field
            pkg = package(entries, {"wdt:P50": ["Q7"], "wdt:P126": ["Q7"]}, FakeWiki({"Q7": "Old Maintainer"}))
            pkg.apply_people()
            self.assertEqual(pkg.item.removed(), [], entries)

    def test_maintainer_orcid_comes_from_the_cre_entry(self):
        wiki = FakeWiki({"Q57170": "Scott Chamberlain"}, {SCOTT: ["Q57170"]})
        entries = [e("vcr", "Scott Chamberlain", ("aut", "cre"), SCOTT),
                   e("vcr", "Scott Chamberlain", ("cre",), email="s@x", source="Maintainer")]
        pkg = package(entries, {}, wiki)
        pkg.apply_people()
        self.assertEqual(sorted(pkg.item.added), [("wdt:P126", "Q57170"), ("wdt:P50", "Q57170")])
        self.assertEqual(wiki.created, [])


if __name__ == "__main__":
    unittest.main()
