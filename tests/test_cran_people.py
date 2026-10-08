"""CRAN people: reading Authors@R, grouping mentions.

No network and no Wikibase. Fields and names are real ones from CRAN's package
database and the MaRDI graph (October 2026), with addresses replaced.
"""

import os
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from mardi_importer.cran import authors, people  # noqa: E402
from mardi_importer.cran.authors import Entry  # noqa: E402

GGPLOT2 = '''c(
    person("Hadley", "Wickham", , "hadley@example.org", role = "aut",
           comment = c(ORCID = "0000-0003-4757-117X")),
    person("Winston", "Chang", role = "aut",
           comment = c(ORCID = "0000-0002-1576-2126")),
    person("Thomas Lin", "Pedersen", , "thomas@example.org", role = c("aut", "cre"),
           comment = c(ORCID = "0000-0002-5147-4711")),
    person("Posit, PBC", role = c("cph", "fnd"),
           comment = c(ROR = "03wc8by49"))
  )'''


class TestAuthorsR(unittest.TestCase):
    def test_ggplot2(self):
        got = authors.parse_authors_r(GGPLOT2, "ggplot2")
        self.assertEqual([(e.name, e.roles, e.orcid, e.kind) for e in got], [
            ("Hadley Wickham", ("aut",), "0000-0003-4757-117X", "person"),
            ("Winston Chang", ("aut",), "0000-0002-1576-2126", "person"),
            ("Thomas Lin Pedersen", ("aut", "cre"), "0000-0002-5147-4711", "person"),
            ("Posit, PBC", ("cph", "fnd"), None, "organisation"),
        ])
        self.assertEqual(got[2].family, "Pedersen")
        self.assertEqual(got[0].email, "hadley@example.org")
        self.assertIsNone(got[1].email)

    def test_named_arguments_escapes_and_old_forms(self):
        text = ('person(given = c("Fr\\u00e9d\\u00e9ric", "J."), family = "Bertrand", '
                'email = "f@example.org", role = c("aut", "cre"), '
                'comment = "ORCID: https://orcid.org/0000-0002-0837-8281") + '
                "person('R Core Team', role = 'ctb')")
        got = authors.parse_authors_r(text, "x")
        self.assertEqual(got[0].name, "Frédéric J. Bertrand")
        self.assertEqual(got[0].orcid, "0000-0002-0837-8281")
        self.assertEqual((got[1].name, got[1].kind), ("R Core Team", "organisation"))

    def test_as_person_text(self):
        got = authors.parse_authors_r('as.person("Jane Roe <jane@example.org> [aut, cre]")', "x")
        self.assertEqual([(e.name, e.roles, e.email) for e in got],
                         [("Jane Roe", ("aut", "cre"), "jane@example.org")])

    def test_unreadable_falls_back_to_author_text(self):
        entries, notes = authors.package_entries({
            "Package": "robust", "Authors@R": 'c(person("A", "B"), ))',
            "Author": "Jiahui Wang [aut], Ruben Zamar [aut]",
            "Maintainer": "Kjell Konis <kjell@example.org>"})
        self.assertEqual([e.name for e in entries], ["Jiahui Wang", "Ruben Zamar", "Kjell Konis"])
        self.assertEqual(len(notes), 1)


class TestFreeText(unittest.TestCase):
    def test_author_field(self):
        text = ("Hadley Wickham [aut] (ORCID: <https://orcid.org/0000-0003-4757-117X>),\n"
                "  Lionel Henry [aut], with contributions from Oliver Buschor and RStudio [cph]")
        got = authors.parse_author_text(text, "x")
        self.assertEqual([(e.name, e.roles, e.orcid, e.kind) for e in got], [
            ("Hadley Wickham", ("aut",), "0000-0003-4757-117X", "person"),
            ("Lionel Henry", ("aut",), None, "person"),
            ("Oliver Buschor", (), None, "person"),
            ("RStudio", ("cph",), None, "organisation"),
        ])
        got = authors.parse_author_text("J. Graham, code for case-control data contributed by Zhijian Chen", "x")
        self.assertEqual([e.name for e in got], ["J. Graham", "Zhijian Chen"])

    def test_maintainer(self):
        m = authors.parse_maintainer('"Dirk Eddelbuettel" <edd@example.org>', "Rcpp")
        self.assertEqual((m.name, m.email, m.roles), ("Dirk Eddelbuettel", "edd@example.org", ("cre",)))
        self.assertEqual(authors.parse_maintainer("ORPHANED", "x").kind, "orphaned")


class TestNames(unittest.TestCase):
    def test_compatible(self):
        yes = [("Frederic Bertrand", "Frédéric Bertrand"), ("Bettina Grün", "Bettina Gruen"),
               ("Øystein Sørensen", "Oystein Sorensen"), ("Vincent Brault", "Brault Vincent"),
               ("J. O. Ramsay", "James Ramsay"), ("Mr. Sandip Garai", "Sandip Garai"),
               ("Tim Smith", "Timothy Smith"), ("Mark van der Loo", "Mark P. J. Van Der Loo")]
        no = [("Thibault Laurent", "Stéphane Laurent"), ("Hadley Wickham", "Wickham"),
              ("Joe Song", "Mingzhou Song")]
        for a, b in yes:
            self.assertTrue(people.compatible(a, b) or people.compatible(b, a), (a, b))
        for a, b in no:
            self.assertFalse(people.compatible(a, b) or people.compatible(b, a), (a, b))
        self.assertTrue(people.compatible("Mark van der Loo", "Mark Loo van der", family="van der Loo"))


def e(pkg, name, source="Authors@R", roles=("aut",), email=None, orcid=None, family=None):
    return Entry(pkg, name, source, roles, family=family, orcid=orcid, email=email)


class TestCluster(unittest.TestCase):
    def test_joins_by_address_orcid_and_maintainer_only_when_names_agree(self):
        entries = [
            e("Rcpp", "Dirk Eddelbuettel", roles=("aut", "cre"), email="edd@x", orcid="0000-0001-6419-907X"),
            e("Rcpp", "Dirk Eddelbuettel", "Maintainer", ("cre",), email="edd@x"),
            e("RQuantLib", "Dirk Eddelbuettel", "Maintainer", ("cre",), email="edd@x"),
            e("digest", "Dirk Eddelbuettel", roles=("aut", "cre")),
            e("digest", "Dirk Eddelbuettel", "Maintainer", ("cre",), email="edd@y"),
            e("other", "Dirk Eddelbuettel", roles=("ctb",)),                      # name only
            e("lab", "Jane Roe", "Maintainer", ("cre",), email="edd@x"),          # shared address
            e("typo", "Aaron Wolen", orcid="0000-0001-6419-907X"),                # someone else's ORCID
        ]
        ps, of = people.cluster(entries)
        dirk = of[0]
        self.assertEqual({i for i in of if of[i] is dirk}, {0, 1, 2})
        self.assertIs(of[3], of[4])                 # maintainer and cre of one package
        self.assertIsNot(of[3], dirk)               # another address, no ORCID: kept apart
        self.assertIsNot(of[5], dirk)
        self.assertIsNot(of[6], dirk)
        self.assertIsNot(of[7], dirk)
        self.assertEqual(dirk.shared_id_conflicts, 2)
        self.assertEqual(dirk.joined_by, {"email", "maintainer"})


if __name__ == "__main__":
    unittest.main()
