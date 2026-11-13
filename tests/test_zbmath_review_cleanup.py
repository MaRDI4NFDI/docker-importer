"""Tests for bot_scripts/zbmath_clean_review_text.py.

Some review texts are taken from the portal (Q6887239, Q6908215, Q6826847,
Q1510548, Q5909218); the others are short made-up examples.
"""

import importlib.util
import os
import unittest
from types import SimpleNamespace

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "bot_scripts", "zbmath_clean_review_text.py")
spec = importlib.util.spec_from_file_location("zbmath_clean_review_text", SCRIPT)
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)
clean = script.clean_review


class CleanReviewTest(unittest.TestCase):
    def test_code_running_into_the_next_word(self):
        self.assertEqual(clean(r"Mittag-Leffler kernel.\NThe main properties"),
                         "Mittag-Leffler kernel. The main properties")

    def test_codes_inside_a_formula(self):
        self.assertEqual(clean(r"such as \N\[\N\pi^2 = 8 + \sum_{n \ge 1} \frac{16}{(4n^2-1)^2}.\N\]"),
                         r"such as \[ \pi^2 = 8 + \sum_{n \ge 1} \frac{16}{(4n^2-1)^2}. \]")
        self.assertEqual(clean(r"Determinanten \N\[\N |(x_i+a_k)^n| \N\]"),
                         r"Determinanten \[ |(x_i+a_k)^n| \]")

    def test_long_codes(self):
        self.assertEqual(clean(r"one\NEWLINEtwo\TABthree"), "one two three")
        self.assertEqual(clean(r"approximation.NEWLINENEWLINETo be specific"), "approximation. To be specific")
        self.assertEqual(clean(r"REPRESENTABLE"), "REPRESENTABLE")

    def test_codes_before_letters_that_look_like_commands(self):
        self.assertEqual(clean(r"\begin{align*}\Nu_t&=d_1\Delta u"), r"\begin{align*} u_t&=d_1\Delta u")
        self.assertEqual(clean(r"\[\NN_K'(T)\gg T\]"), r"\[ N_K'(T)\gg T\]")
        self.assertEqual(clean(r"available\Nat \url{x}"), r"available at \url{x}")

    def test_latex_is_kept(self):
        for text in (r"polare Gleichung: \[ r = a\frac{\sin\Theta}{\Theta} \] ist.",
                     r"\(\frac {\Theta_1(n)}{n^2 \pi}\)", r"$a \Rightarrow b$", r"$\Re z$", r"in \TeX",
                     r"first row \\Next row"):
            self.assertEqual(clean(text), text)

    def test_codes_next_to_formulas(self):
        self.assertEqual(clean(r"In this group \N$\Gamma$ acts"), r"In this group $\Gamma$ acts")
        self.assertEqual(clean(r"maps from\N$A$ into\N$B$"), r"maps from $A$ into $B$")
        self.assertEqual(clean(r"\[\N^C D^\alpha_t y(t)\]"), r"\[ ^C D^\alpha_t y(t)\]")

    def test_text_without_codes_is_unchanged(self):
        text = "No codes here,  not even double spaces are touched."
        self.assertEqual(clean(text), text)


class IsZbmathPaperTest(unittest.TestCase):
    def test_needs_the_class_and_a_zbmath_id(self):
        def claim(value):
            return SimpleNamespace(mainsnak=SimpleNamespace(datavalue={"value": value}))

        def item(claims):
            return SimpleNamespace(claims=SimpleNamespace(get=lambda pid: claims.get(pid, [])))

        article = claim({"id": "Q56887"})
        self.assertTrue(script.is_zbmath_paper(item({"P31": [article], "P1451": [claim("1004187")]}),
                                               "P31", "Q56887", ["P225", "P1451"]))
        self.assertFalse(script.is_zbmath_paper(item({"P31": [article]}), "P31", "Q56887", ["P225", "P1451"]))
        self.assertFalse(script.is_zbmath_paper(item({"P1451": [claim("1004187")]}),
                                                "P31", "Q56887", ["P225", "P1451"]))


if __name__ == "__main__":
    unittest.main()
