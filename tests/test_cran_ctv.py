"""CRAN task views: reading Views.rds, planning, and the edits to view items and packages. No network."""

import os
import sys
import unittest
from unittest.mock import Mock, patch

import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.cran import ctv  # noqa: E402
from mardi_importer.cran.ctv import State, TaskView  # noqa: E402

IDS = {"instance": "P31", "part_of": "P37", "role": "P635", "website": "P29", "repo": "P339", "author": "P16",
       "author_string": "P43", "updated": "P170", "cran": "P229", "maintained_by": "P19", "view_id": "P900",
       "class": "Q900", "core": "Q901"}

BAYES = TaskView("Bayesian", "Bayesian Inference", "2026-03-16", "https://github.com/cran-task-views/Bayesian/",
                 ("Jong Hee Park", "Kevin M. Quinn"), {"rstan": True, "MCMCpack": False})


def part_of(guid, view, core=False):
    c = {"id": guid, "mainsnak": {"snaktype": "value", "property": "P37", "datavalue": ctv._item(view)}}
    if core:
        c["qualifiers"] = {"P635": [{"snaktype": "value", "property": "P635", "datavalue": ctv._item("Q901")}]}
    return c


def state(**kw):
    base = dict(views={}, packages={"rstan": "Q1", "MCMCpack": "Q2", "zoo": "Q3"}, members={}, people={})
    base.update(kw)
    return State(**base)


class TestRead(unittest.TestCase):
    def test_views_rds(self):
        data = {"Bayesian": {
            "name": np.array(["Bayesian"]), "topic": np.array(["Bayesian Inference"]),
            "maintainer": np.array(["Jong Hee Park, Kevin M. Quinn"]), "version": np.array(["2026-03-16"]),
            "source": np.array(["https://github.com/cran-task-views/Bayesian/"]),
            "packagelist": {"name": ["rstan", "MCMCpack"], "core": [True, False]},       # a data frame in R
            "citation": [{"author": [{"given": np.array(["Jong", "Hee"]), "family": np.array(["Park"])},
                                     {"given": np.array(["Kevin", "M."]), "family": np.array(["Quinn"])}]}]}}
        with patch.dict(sys.modules, {"rdata": Mock(read_rds=Mock(return_value=data))}):
            (v,) = ctv.read_views("Views.rds")
        self.assertEqual((v.label, v.version, v.maintainers), ("CRAN Task View: Bayesian Inference", "2026-03-16",
                                                               ("Jong Hee Park", "Kevin M. Quinn")))
        self.assertEqual(v.packages, {"rstan": True, "MCMCpack": False})

    def test_maintainers_without_citation(self):
        self.assertEqual(ctv._maintainers({"maintainer": np.array(["Ann Lee, Bob Kay and Cy Dee"])}),
                         ("Ann Lee", "Bob Kay", "Cy Dee"))


class TestPackageEdits(unittest.TestCase):
    def test_add_core_and_plain(self):
        edits = ctv.package_edits([], {"Q10": True, "Q11": False}, {"Q10", "Q11"}, IDS)
        self.assertEqual([(e["mainsnak"]["datavalue"]["value"]["id"], "qualifiers" in e) for e in edits],
                         [("Q10", True), ("Q11", False)])
        self.assertNotIn("id", edits[0])

    def test_core_changes_drops_and_other_part_of_kept(self):
        live = [part_of("Q1$a", "Q10"), part_of("Q1$b", "Q11", core=True), part_of("Q1$c", "Q500"),
                part_of("Q1$d", "Q12"), part_of("Q1$e", "Q12")]
        edits = ctv.package_edits(live, {"Q10": True, "Q11": True, "Q12": False}, {"Q10", "Q11", "Q12", "Q13"}, IDS)
        self.assertEqual([e.get("id") for e in edits], ["Q1$a", "Q1$e"])       # Q500 is no view: untouched
        self.assertIn("qualifiers", edits[0])
        self.assertEqual(edits[1], {"id": "Q1$e", "remove": ""})               # duplicate

    def test_left_view(self):
        self.assertEqual(ctv.package_edits([part_of("Q1$a", "Q10")], {}, {"Q10"}, IDS), [{"id": "Q1$a", "remove": ""}])

    def test_up_to_date(self):
        self.assertEqual(ctv.package_edits([part_of("Q1$a", "Q10", core=True)], {"Q10": True}, {"Q10"}, IDS), [])


class TestPlan(unittest.TestCase):
    def test_new_view(self):
        p = ctv.plan([BAYES], state())
        self.assertEqual(([v.name for v in p.create], p.update, p.packages), (["Bayesian"], [], {}))

    def test_version_change_and_packages(self):
        s = state(views={"Bayesian": ("Q10", "2025-01-01")}, members={"Q1": {"Q10": False}, "Q3": {"Q10": False}})
        p = ctv.plan([BAYES], s)
        self.assertEqual(p.update, [(BAYES, "Q10")])
        self.assertEqual(p.packages, {"Q1": {"Q10": True}, "Q2": {"Q10": False}, "Q3": {}})

    def test_up_to_date(self):
        s = state(views={"Bayesian": ("Q10", "2026-03-16")}, members={"Q1": {"Q10": True}, "Q2": {"Q10": False}})
        p = ctv.plan([BAYES], s)
        self.assertEqual((p.create, p.update, p.packages), ([], [], {}))

    def test_reuse_and_missing(self):
        doe = TaskView("ExperimentalDesign", "Design of Experiments", "2026-01-01", "", (), {"nopkg": False})
        p = ctv.plan([doe], state())
        self.assertEqual((p.create, p.update, p.missing), ([], [(doe, "Q5974426")], ["nopkg"]))


class TestViewItem(unittest.TestCase):
    def test_update_replaces_managed_statements_only(self):
        current = {"aliases": {"en": [{"value": "Bayesian"}]},
                   "claims": {"P29": [{"id": "Q10$w"}], "P13": [{"id": "Q10$swmath"}], "P43": [{"id": "Q10$a"}]}}
        data = ctv.view_entity(BAYES, IDS, [("Jong Hee Park", "Q77"), ("Kevin M. Quinn", None)], current)
        self.assertNotIn("aliases", data)
        removed = [c["id"] for c in data["claims"] if "remove" in c]
        self.assertEqual(sorted(removed), ["Q10$a", "Q10$w"])
        props = [c["mainsnak"]["property"] for c in data["claims"] if "mainsnak" in c]
        self.assertEqual(props, ["P31", "P37", "P900", "P29", "P170", "P339", "P16", "P43"])

    def test_new_item_gets_alias(self):
        data = ctv.view_entity(BAYES, IDS, [])
        self.assertEqual(data["aliases"]["en"][0]["value"], "Bayesian")
        self.assertEqual(data["labels"]["en"]["value"], "CRAN Task View: Bayesian Inference")

    def test_authors_only_when_unambiguous(self):
        s = state(people={"rstan": {("hee", "park"): {"Q77"}}, "MCMCpack": {("kevin", "quinn"): {"Q78"},
                                                                           ("jong", "park"): {"Q77"}}})
        s.people["MCMCpack"][("kevin", "quinn")] = {"Q78", "Q79"}
        self.assertEqual(ctv.authors_of(BAYES, s), [("Jong Hee Park", "Q77"), ("Kevin M. Quinn", None)])


class FakeWriter:
    def __init__(self, live, existing=None):
        self.live, self.edits, self.existing = live, [], existing or {}

    def find(self, view, id_pid):
        return self.existing.get(view.name)

    def get(self, qid):
        return self.live.get(qid, {"claims": {}})

    def edit(self, data, summary, qid=None):
        self.edits.append((qid, data))
        return qid or "Q10"

    def resolve(self, qids):
        return {}


class TestSync(unittest.TestCase):
    def api(self):
        api = Mock()
        api.get_local_id_by_label.side_effect = lambda label, kind: {
            **{v: IDS[k] for k, v in ctv.PROPS.items()}, ctv.VIEW_ID: "P900", ctv.CLASS: "Q900", ctv.CORE: "Q901"}[label]
        return api

    def test_dry_run_writes_nothing(self):
        w = FakeWriter({})
        report = ctv.sync(self.api(), dry_run=True, views=[BAYES], state=state(), writer=w)
        self.assertEqual((report["create"], w.edits), (["Bayesian"], []))

    def test_new_view_then_its_packages(self):
        w = FakeWriter({"Q1": {"claims": {"P37": [part_of("Q1$a", "Q10", core=True)]}}})
        report = ctv.sync(self.api(), views=[BAYES], state=state(), writer=w)
        self.assertEqual([q for q, _ in w.edits], [None, "Q2"])                # rstan already right
        self.assertEqual((report["packages_written"], report["errors"]), (1, []))

    def test_view_made_since_the_sparql_store_was_updated_is_not_made_again(self):
        w = FakeWriter({}, existing={"Bayesian": "Q10"})
        ctv.sync(self.api(), views=[BAYES], state=state(), writer=w)
        self.assertEqual([q for q, _ in w.edits], ["Q10", "Q1", "Q2"])         # updated, not created


if __name__ == "__main__":
    unittest.main()
