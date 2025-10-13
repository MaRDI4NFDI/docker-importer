__version__ = "0.0.1"

from .registry import SourceRegistry
from .cran import CRANSource
from .polydb import PolyDBSource
from .zbmath import ZBMathSource
from .zenodo import ZenodoSource

SourceRegistry.register('cran', CRANSource, 'CRAN_USER', 'CRAN_PASS')
SourceRegistry.register('polydb', PolyDBSource, 'POLYDB_USER', 'POLYDB_PASS')
SourceRegistry.register('zbmath', ZBMathSource, 'ZBMATH_USER', 'ZBMATH_PASS')
SourceRegistry.register('zenodo', ZenodoSource, 'ZENODO_USER', 'ZENODO_PASS')