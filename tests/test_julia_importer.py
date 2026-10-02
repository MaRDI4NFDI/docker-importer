"""Julia General importer: decision rules, references, update rule, e-mail guard.

No network and no Wikibase: the registry is a throwaway git repository and the
MaRDI client is a stub. Cases are the real ones met in the first full batch.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import Mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.julia import registry, metadata  # noqa: E402
from mardi_importer.julia.JuliaPackage import (  # noqa: E402
    AUTHOR, AUTHOR_NAME_STRING, DESCRIBED_BY_SOURCE, LICENSE, OBJECT_NAMED_AS, PACKAGE_NAME,
    SOURCE_REPOSITORY, VERSION, JuliaPackage, Planned, add_planned,
)
from mardi_importer.julia.JuliaSource import JuliaSource, decide_person, same_software  # noqa: E402
from mardi_importer.julia.metadata import RepoMetadata  # noqa: E402
from mardi_importer.julia.people import (  # noqa: E402
    Mention, People, Person, assert_no_emails, same_person_possible,
)

REF = ("https://github.com/o/r/blob/abc/Project.toml", "2026-10-01")


def person(name="Jane Doe", packages=("A",), evidence=(), orcids=()):
    return Person(id=f"person-{name}", names=Counter({name: 1}), packages=set(packages),
                  orcids=set(orcids), evidence=set(evidence))


class TestRegistry(unittest.TestCase):
    def test_repo_key(self):
        for u in ("https://github.com/SciML/OrdinaryDiffEq.jl.git",
                  "git@github.com:SciML/OrdinaryDiffEq.jl.git",
                  "https://github.com/sciml/ordinarydiffeq.jl/blob/master/README.md"):
            self.assertEqual(registry.repo_key(u), "github.com/sciml/ordinarydiffeq.jl")

    def test_scope(self):
        pkg = lambda name, repo="https://github.com/SciML/X.jl.git", subdir=None: \
            {"name": name, "repo": repo, "subdir": subdir}
        self.assertTrue(registry.in_scope(pkg("X")))
        self.assertFalse(registry.in_scope(pkg("X", repo="https://github.com/someone/X.jl")))
        self.assertFalse(registry.in_scope(pkg("OpenBLAS_jll")))
        self.assertFalse(registry.in_scope(pkg("OrdinaryDiffEqTsit5", subdir="lib/OrdinaryDiffEqTsit5")))
        self.assertTrue(registry.in_scope(pkg("DelayDiffEq", subdir="lib/DelayDiffEq")))

    def test_latest_version(self):
        self.assertEqual(registry.latest_version(
            {"1.9.0": {}, "1.10.0": {"yanked": True}, "1.9.0-beta": {}}), "1.9.0")

    def test_read_registry(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "General"
            files = {
                "Registry.toml": '[packages]\na-1 = { name = "Optim", path = "O/Optim" }\n'
                                 'a-2 = { name = "Other", path = "O/Other" }\n',
                "O/Optim/Package.toml": 'repo = "https://github.com/JuliaNLSolvers/Optim.jl.git"\n',
                "O/Optim/Versions.toml": '["1.0.0"]\ngit-tree-sha1 = "x"\n["1.1.0"]\ngit-tree-sha1 = "y"\n',
                "O/Other/Package.toml": 'repo = "https://github.com/someone/Other.jl.git"\n',
            }
            for rel, text in files.items():
                (root / rel).parent.mkdir(parents=True, exist_ok=True)
                (root / rel).write_text(text)
            git = ["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@t",
                   "-c", "commit.gpgsign=false"]
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(git + ["add", "."], check=True)
            subprocess.run(git + ["commit", "-qm", "x"], check=True)
            sha, pkgs = registry.read_registry(root)
            scoped = [p for p in pkgs if registry.in_scope(p)]
            self.assertEqual([(p["name"], p["version"]) for p in scoped], [("Optim", "1.1.0")])
            self.assertEqual(len(sha), 40)


class TestLicences(unittest.TestCase):
    def test_first_licence_named_wins_over_bundled_notices(self):
        text = ("MIT License\nPermission is hereby granted, free of charge, to any person\n"
                + "x " * 300 + "Redistribution and use in source and binary forms ... Neither the name")
        self.assertEqual(metadata.classify_license(text), "MIT")

    def test_gpl_text_naming_lgpl_and_agpl_is_gpl(self):
        gpl2 = ("GNU GENERAL PUBLIC LICENSE\n Version 2, June 1991\n TERMS AND CONDITIONS " + "x " * 400
                + "use the GNU Lesser General Public License instead")
        self.assertEqual(metadata.classify_license(gpl2), "GPL-2.0")
        notice = "licensed under the GNU Public License, Version 3.0+:\n" + "x " * 400 + "GNU Affero"
        self.assertEqual(metadata.classify_license(notice), "GPL-3.0-or-later")

    def test_bsd_clause_count(self):
        self.assertEqual(metadata.classify_license("a 3-clause BSD-style license. Redistribution and use"),
                         "BSD-3-Clause")

    def test_unclear_is_none(self):
        self.assertIsNone(metadata.classify_license("Copyright " + "x " * 300 + "MIT License " + "Apache License 2.0"))
        self.assertIsNone(metadata.classify_license("All rights reserved."))

    def test_every_licence_maps_to_a_wikidata_item(self):
        self.assertTrue(all(v.startswith("wd:Q") for v in metadata.LICENSES.values()))


class TestCitations(unittest.TestCase):
    def test_doi_inside_a_publisher_url_is_not_a_doi(self):
        bib = """@article{x, doi = {10.5334/jors.151},
          url = {http://openresearchsoftware.metajnl.com/articles/10.5334/jors.151/galley/245/download/}}"""
        self.assertEqual(metadata.citation_ids(bib), (["10.5334/JORS.151"], []))

    def test_arxiv_forms(self):
        _, arxiv = metadata.citation_ids("eprint = {2403.16341v2}\ndoi = {10.48550/arXiv.2110.06048}")
        self.assertEqual(sorted(arxiv), ["2110.06048", "2403.16341"])

    def test_zenodo_is_the_software_itself(self):
        self.assertTrue(metadata.is_zenodo("10.5281/ZENODO.11314275"))


class TestAuthors(unittest.TestCase):
    def test_address_binds_to_the_name_before_it(self):
        self.assertEqual(
            metadata.parse_author_entries(["Vaibhav Dixit <vd@example.org>, Guillaume Dalle and contributors"]),
            [("Vaibhav Dixit", "vd@example.org"), ("Guillaume Dalle", None)])

    def test_filler_and_company_suffix(self):
        names = [n for n, _ in metadata.parse_author_entries(
            ["JuliaHub, Inc. and contributors", "SciML contributors"])]
        self.assertEqual(names, ["JuliaHub, Inc."])

    def test_cff_software_authors_only(self):
        cff = ("authors:\n  - given-names: Guillaume\n    family-names: Dalle\n"
               "    orcid: 'https://orcid.org/0000-0003-4866-1687'\n"
               "preferred-citation:\n  authors:\n    - given-names: Paper\n      family-names: Author\n")
        self.assertEqual(metadata.cff_authors(cff),
                         [{"name": "Guillaume Dalle", "orcid": "0000-0003-4866-1687", "email": None}])


class TestPeople(unittest.TestCase):
    def test_email_joins_spellings_across_packages(self):
        ppl = People()
        for pkg, name in (("A", "Chris Rackauckas"), ("B", "ChrisRackauckas"), ("C", "Christopher Rackauckas")):
            ppl.add(Mention(pkg, name, *REF, email="cr@example.org"))
        persons, _ = ppl.clusters()
        self.assertEqual(len(persons), 1)
        self.assertEqual(persons[0].packages, {"A", "B", "C"})
        self.assertIn(persons[0].canonical, {"Chris Rackauckas", "Christopher Rackauckas"})

    def test_person_carries_no_address(self):
        ppl = People()
        ppl.add(Mention("A", "Jane Doe", *REF, email="jd@example.org"))
        persons, _ = ppl.clusters()
        assert_no_emails(repr(persons), "persons")

    def test_similar_names(self):
        self.assertTrue(same_person_possible("Tim Holy", "Timothy E. Holy"))
        self.assertTrue(same_person_possible("Luis Benet", "L. Benet"))
        self.assertFalse(same_person_possible("Chad Scherrer", "Christian Scherrer"))

    def test_guard(self):
        with self.assertRaises(ValueError):
            assert_no_emails("Jane <jd@example.org>", "x")
        assert_no_emails("JuliaHub, Inc. https://github.com/JuliaHub/X.jl", "x")


class TestSameSoftware(unittest.TestCase):
    """An existing item is updated only when it is the same software."""

    def pkg(self, name, repo, subdir=None):
        return {"name": name, "repo": repo, "subdir": subdir}

    def test_own_repository(self):
        p = self.pkg("Optim", "https://github.com/JuliaNLSolvers/Optim.jl.git")
        self.assertEqual(same_software(p, {"Q41713"}, {"Q41713"}, {}), ["Q41713"])

    def test_repository_moved_to_another_org(self):
        p = self.pkg("BFloat16s", "https://github.com/JuliaMath/BFloat16s.jl.git")
        urls = {"Q42445": ["https://github.com/JuliaComputing/BFloat16s.jl"]}
        self.assertEqual(same_software(p, set(), {"Q42445"}, urls), ["Q42445"])

    def test_namesakes_are_not_the_same_software(self):
        sundials = self.pkg("Sundials", "https://github.com/SciML/Sundials.jl.git")
        self.assertEqual(same_software(sundials, set(), {"Q13671"},
                                       {"Q13671": ["https://computation.llnl.gov/casc/sundials/"]}), [])
        fftw = self.pkg("FFTW", "https://github.com/JuliaMath/FFTW.jl.git")
        self.assertEqual(same_software(fftw, set(), {"Q44007"}, {"Q44007": ["https://github.com/cran/fftw"]}), [])

    def test_monorepo_parent_is_not_the_subpackage(self):
        p = self.pkg("DelayDiffEq", "https://github.com/SciML/OrdinaryDiffEq.jl.git", "lib/DelayDiffEq")
        self.assertEqual(same_software(p, {"Q46330"}, set(), {}), [])

    def test_subpackage_with_its_own_old_item(self):
        p = self.pkg("StochasticDiffEq", "https://github.com/SciML/OrdinaryDiffEq.jl.git", "lib/StochasticDiffEq")
        urls = {"Q52240": ["https://github.com/SciML/StochasticDiffEq.jl"]}
        self.assertEqual(same_software(p, {"Q46330"}, {"Q52240"}, urls), ["Q52240"])

    def test_one_of_two_namesakes(self):
        p = self.pkg("NFFT", "https://github.com/JuliaMath/NFFT.jl.git")
        urls = {"Q19637": ["http://www.tu-chemnitz.de/~potts/nfft/"], "Q41845": ["https://github.com/tknopp/NFFT.jl"]}
        self.assertEqual(same_software(p, set(), {"Q19637", "Q41845"}, urls), ["Q41845"])


class TestPersonDecision(unittest.TestCase):
    """Reuse an item only when certain; otherwise create one for identifiable people."""

    def test_orcid_on_one_item_is_reused(self):
        p = person(orcids=["0000-0001-5850-0663"])
        decide_person(p, {"0000-0001-5850-0663": ["Q2236695"]}, {"Q83354": "paper"})
        self.assertEqual((p.action, p.qid), ("link", "Q2236695"))
        self.assertEqual(p.possible_duplicates, ["Q83354"])

    def test_orcid_on_two_items_reuses_one_and_logs_the_other(self):
        p = person(orcids=["0000-0003-4866-1687"])
        decide_person(p, {"0000-0003-4866-1687": ["Q6709553", "Q6709549"]}, {})
        self.assertEqual((p.action, p.qid, p.possible_duplicates), ("link", "Q6709549", ["Q6709553"]))

    def test_cited_paper_author_is_reused(self):
        p = person("Gleb Pogudin")
        decide_person(p, {}, {"Q263318": "Q6043379 cited by StructuralIdentifiability"})
        self.assertEqual((p.action, p.qid, p.linked_by), ("link", "Q263318", "cited paper author"))

    def test_identifiable_but_uncertain_is_created(self):
        p = person("Tim Holy", packages=("A", "B"), evidence=["email"])
        decide_person(p, {}, {})
        self.assertEqual((p.action, p.qid), ("create", None))

    def test_single_mention_stays_a_name_string(self):
        p = person("Solo Author")
        decide_person(p, {}, {})
        self.assertIsNone(p.action)

    def test_handle_stays_a_name_string(self):
        p = person("jClugstor", packages=("A", "B"), evidence=["email"])
        decide_person(p, {}, {})
        self.assertIsNone(p.action)


def package(**kw):
    md = RepoMetadata(retrieved="2026-10-01", project_url=REF[0], license="MIT",
                      license_url="https://github.com/o/r/blob/abc/LICENSE.md",
                      citation_url="https://github.com/o/r/blob/abc/CITATION.bib", deps=["NLSolversBase", "LinearAlgebra"])
    base = dict(name="Optim", uuid="u", repo="https://github.com/JuliaNLSolvers/Optim.jl.git",
                path="O/Optim", registry_sha="sha1", retrieved="2026-10-01", metadata=md,
                version="2.3.2", papers=["Q999"])
    base.update(kw)
    return JuliaPackage(**base)


class TestPlannedStatements(unittest.TestCase):
    def plan(self):
        jp = package()
        linked = person("Patrick Kofod Mogensen")
        linked.qid = "Q5"
        jp.authors = [(Mention("Optim", "Patrick K. Mogensen", *REF), linked),
                      (Mention("Optim", "Asbjørn Riseth", *REF), person("Asbjørn Riseth"))]
        return jp.plan("Q1")

    def test_every_statement_is_referenced(self):
        for st in self.plan():
            self.assertTrue(st.referenced, st.prop)

    def test_registry_facts_are_stated_in_the_registry(self):
        by = {st.prop: st for st in self.plan()}
        for prop in (PACKAGE_NAME, SOURCE_REPOSITORY, VERSION):
            self.assertTrue(by[prop].stated_in_registry)
            self.assertIn("/JuliaRegistries/General/blob/sha1/", by[prop].ref_url)

    def test_identifier_without_jl_and_publications_as_described_by_source(self):
        by = {st.prop: st for st in self.plan()}
        self.assertEqual(by[PACKAGE_NAME].value, "Optim")
        self.assertEqual(by[DESCRIBED_BY_SOURCE].value, "Q999")
        self.assertEqual(by[LICENSE].value, "wd:Q334661")

    def test_authors_item_with_name_as_stated_else_string(self):
        sts = [st for st in self.plan() if st.prop in (AUTHOR, AUTHOR_NAME_STRING)]
        self.assertEqual([(st.prop, st.value) for st in sts],
                         [(AUTHOR, "Q5"), (AUTHOR_NAME_STRING, "Asbjørn Riseth")])
        self.assertEqual(sts[0].qualifiers, [(OBJECT_NAMED_AS, "Patrick K. Mogensen")])

    def test_dependencies_only_to_known_packages(self):
        deps = package().plan_dependencies({"NLSolversBase": "Q7"})
        self.assertEqual([(d.value, d.ref_url) for d in deps], [("Q7", REF[0])])


class TestUpdateRule(unittest.TestCase):
    """Updates only add: present values are skipped, single-valued conflicts left alone."""

    def test_add_only(self):
        planned = [Planned(LICENSE, "Q56842", "u", "d"), Planned(VERSION, "2.0.0", "u", "d"),
                   Planned(SOURCE_REPOSITORY, "https://github.com/A/B.jl.git", "u", "d"),
                   Planned(PACKAGE_NAME, "B", "u", "d")]
        existing = {LICENSE: ["Q56634"], VERSION: ["1.0.0"],
                    SOURCE_REPOSITORY: ["https://github.com/a/b.jl"], PACKAGE_NAME: []}
        add, conflicts = JuliaPackage.merge(planned, existing)
        self.assertEqual([(st.prop, st.value) for st in add], [(VERSION, "2.0.0"), (PACKAGE_NAME, "B")])
        self.assertEqual([c["property"] for c in conflicts], [LICENSE])


class _Container:
    """Stands in for WikibaseIntegrator's Qualifiers / Reference / References."""

    def __init__(self):
        self.added = []

    def add(self, x):
        self.added.append(x)


class TestWritingAStatement(unittest.TestCase):
    """Independent of whether another test module stubbed wikibaseintegrator."""

    def setUp(self):
        # the package re-exports the class under the module's name, so take the module itself
        jp_module = sys.modules["mardi_importer.julia.JuliaPackage"]
        self._orig = jp_module._wbi_models
        jp_module._wbi_models = lambda: (_Container, _Container, _Container)
        self.addCleanup(setattr, jp_module, "_wbi_models", self._orig)
        self.api = Mock()
        self.api.get_claim.side_effect = lambda prop, value, **kw: (prop, value)
        self.item = Mock()

    def test_reference_block(self):
        add_planned(self.api, self.item, Planned(VERSION, "2.3.2", "https://x/Versions.toml", "2026-10-01", True), "Q42")
        refs = self.item.add_claim.call_args.kwargs["references"]
        (ref,) = refs.added
        self.assertEqual(ref.added, [("wdt:P248", "Q42"), ("wdt:P854", "https://x/Versions.toml"),
                                     ("wdt:P813", "+2026-10-01T00:00:00Z")])

    def test_qualifier_keeps_the_name_as_stated(self):
        add_planned(self.api, self.item, Planned(AUTHOR, "Q5", "u", "2026-10-01",
                                                 qualifiers=[(OBJECT_NAMED_AS, "ChrisRackauckas")]), "Q42")
        quals = self.item.add_claim.call_args.kwargs["qualifiers"]
        self.assertEqual(quals.added, [(OBJECT_NAMED_AS, "ChrisRackauckas")])

    def test_refuses_an_address(self):
        with self.assertRaises(ValueError):
            add_planned(self.api, self.item, Planned(AUTHOR_NAME_STRING, "Jane <jd@example.org>", "u", "d"), "Q42")
        self.item.add_claim.assert_not_called()


class TestReport(unittest.TestCase):
    def test_summary_has_no_addresses(self):
        src = object.__new__(JuliaSource)
        src.registry_sha, src.publications = "sha1", {}
        src.packages = [package()]
        ppl = People()
        ppl.add(Mention("Optim", "Jane Doe", *REF, email="jd@example.org"))
        src.persons, _ = ppl.clusters()
        report = src.summary()
        self.assertNotIn("@", json.dumps(report))
        self.assertEqual(report["packages"]["Optim"]["action"], "create")


if __name__ == "__main__":
    unittest.main()
