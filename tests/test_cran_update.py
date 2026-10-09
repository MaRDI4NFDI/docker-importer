"""CRAN importer: updating a package writes once, and an error leaves it untouched.

A run on 2026-10-09 removed vcr's and hoardr's licences, dependencies, imports
and cited works, then failed (Wikidata answered 429) before adding the new ones.
No network and no Wikibase here. The claims follow WikibaseIntegrator's rules
(equal: same property, value and qualifiers; removed: flagged, dropped on write);
real WikibaseIntegrator classes are not used because another test module may
replace them with stubs.
"""

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.cran import RPackage as module  # noqa: E402
from mardi_importer.cran.RPackage import RPackage  # noqa: E402

PIDS = {"wdt:P275": "P163", "wdt:P1547": "P342", "imports": "P585", "wdt:P2860": "P223",
        "wdt:P50": "P16", "wdt:P126": "P19", "wdt:P2093": "P43"}


class Claim:
    def __init__(self, pid, value):
        self.pid, self.removed = pid, False
        self.mainsnak = SimpleNamespace(datavalue={"value": {"id": value}}, property_number=pid)

    def __eq__(self, other):
        return isinstance(other, Claim) and (self.pid, self.mainsnak.datavalue) == (other.pid, other.mainsnak.datavalue)

    def remove(self, remove=True):
        self.removed = remove


class Claims:
    def __init__(self):
        self.claims = {}

    def get(self, pid):
        return self.claims.get(pid, [])

    def add(self, claim, action=None):
        self.claims.setdefault(claim.pid, []).append(claim)


def claim(pid, value):
    return Claim(pid, value)


class FakeItem:
    def __init__(self, claims):
        self.id = "Q89086"
        self.claims = Claims()
        for c in claims:
            self.claims.add(c)
        self.descriptions = SimpleNamespace(values={"en": "Record HTTP Calls"}, set=Mock())
        self.write = Mock(return_value=SimpleNamespace(id="Q89086"))

    def add_claim(self, prop, value, **kw):
        pass

    def removed(self):
        return sorted((p, c.mainsnak.datavalue["value"]["id"]) for p, cs in self.claims.claims.items()
                      for c in cs if c.removed)


def vcr(claims):
    api = Mock()
    api.get_local_id_by_label.side_effect = lambda p, _: PIDS[p]
    api.get_claim.side_effect = lambda p, v, **kw: claim(PIDS.get(p, p), v)
    pkg = RPackage("2026-10-01", "vcr", "Record HTTP Calls", entries=[], people=Mock(),
                   wikidata_ids={}, api=api, wdi=Mock(), crossref=Mock(), arxiv=Mock(), zenodo=Mock())
    pkg._item = FakeItem(claims)
    pkg.pull = lambda: pkg
    pkg.license_data = [("Q57086", "")]                       # MIT, unchanged
    pkg.dependencies = [("Q27458", "")]                       # R
    pkg.imports = [("Q57482", ""), ("Q99999", "")]            # one kept, one new
    return pkg


class TestUpdate(unittest.TestCase):
    def test_statements_replaced_in_one_write(self):
        pkg = vcr([claim("P163", "Q57086"), claim("P342", "Q27458"),
                   claim("P585", "Q57482"), claim("P585", "Q73373")])
        with patch.object(module, "remove_claims", create=True) as separate_removal:
            pkg.update()
        separate_removal.assert_not_called()
        pkg.item.write.assert_called_once()
        self.assertEqual(pkg.item.removed(), [("P585", "Q73373")])          # only what CRAN dropped
        imports = [(c.mainsnak.datavalue["value"]["id"], c.removed) for c in pkg.item.claims.get("P585")]
        self.assertEqual(sorted(imports), [("Q57482", False), ("Q73373", True), ("Q99999", False)])
        self.assertEqual([c.removed for c in pkg.item.claims.get("P163")], [False])   # unchanged licence kept

    def test_an_error_before_the_write_changes_nothing(self):
        pkg = vcr([claim("P163", "Q57086"), claim("P585", "Q73373")])
        pkg.get_wikidata_QID = Mock(side_effect=RuntimeError("429 Client Error: Too Many Requests"))
        with self.assertRaises(RuntimeError):
            pkg.update()
        pkg.item.write.assert_not_called()


class TestWikidata(unittest.TestCase):
    def response(self, rows):
        r = Mock()
        r.json.return_value = {"results": {"bindings": [
            {"item": {"value": f"http://www.wikidata.org/entity/{q}"}, "name": {"value": n},
             "kind": {"value": str(k)}} for q, n, k in rows]}}
        return r

    def test_cran_project_first_and_ambiguous_names_left_out(self):
        session = Mock()
        session.get.return_value = self.response([
            ("Q326489", "ggplot2", 1), ("Q999", "ggplot2", 2),       # P5565 wins over a label
            ("Q1", "boot", 2), ("Q2", "boot", 2),                   # two R packages called boot: neither
            ("Q104854189", "dplyr", 2)])
        self.assertEqual(module.wikidata_r_packages(session), {"ggplot2": "Q326489", "dplyr": "Q104854189"})
        self.assertEqual(session.get.call_count, 1)

    def test_wikidata_down_is_not_an_error(self):
        session = Mock()
        session.get.side_effect = RuntimeError("429 Client Error: Too Many Requests")
        self.assertEqual(module.wikidata_r_packages(session), {})


if __name__ == "__main__":
    unittest.main()
