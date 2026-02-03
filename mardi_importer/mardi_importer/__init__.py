__version__ = "0.0.1"

from .importer import Importer
from .arxiv import ArxivSource
from .cran import CRANSource
from .crossref import CrossrefSource
from .polydb import PolyDBSource
from .zbmath import ZBMathSource
from .zenodo import ZenodoSource

Importer.register("cran", CRANSource, "CRAN_USER", "CRAN_PASS")
Importer.register("polydb", PolyDBSource, "POLYDB_USER", "POLYDB_PASS")
Importer.register("zbmath", ZBMathSource, "ZBMATH_USER", "ZBMATH_PASS")
Importer.register("zenodo", ZenodoSource, "ZENODO_USER", "ZENODO_PASS")
Importer.register("crossref", CrossrefSource, "CROSSREF_USER", "CROSSREF_PASS")
Importer.register("arxiv", ArxivSource, "ARXIV_USER", "ARXIV_PASS")
