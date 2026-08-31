Importer Module for MIPLIB
==========================

Imports the problem instances of the `MIPLIB 2017 <https://miplib.zib.de/>`_
mixed-integer programming library.

MaRDI already holds the collection itself as ``Q6826034``, *MIPLIB 2017
benchmark set for mixed-integer programming*, imported from MathAlgoDB. That
item carries five statements and nothing points at it, so what this source adds
is its membership: one item per instance, each linked back to the collection
with *part of* (``wdt:P361``).

MIPLIB publishes no CSV or JSON export. The roster is a plain-text list, one
``<name>.mps.gz`` per line, and instance statistics are parsed out of the
per-instance HTML pages.

Field mapping
-------------

============================  ==========================================  ============================
MIPLIB field                  Property                                    Notes
============================  ==========================================  ============================
instance name                 ``MIPLIB instance ID`` (external-id)        Also the item label
\-                            ``wdt:P31`` instance of                     *MIPLIB instance*
\-                            ``wdt:P361`` part of                        ``Q6826034``
\-                            ``wdt:P1343`` described by source           ``Q2062317``
tags, Status                  ``MIPLIB tag``                              One claim per tag
Submitter                     ``wdt:P2093`` author name string            Split on commas
Group                         ``MIPLIB instance group``
MPS File                      ``MPS file name``
Variables                     ``number of variables``                     Original column
Constraints                   ``number of constraints``                   Original column
Binaries                      ``number of binary variables``              Original column
Integers                      ``number of integer variables``             Original column
Continuous                    ``number of continuous variables``          Original column
Implicit Integers             ``number of implicit integer variables``    Original column
Fixed Variables               ``number of fixed variables``               Original column
Nonzeroes                     ``number of nonzeros``                      Original column
Density                       ``nonzero density``
Objective                     ``objective value``
============================  ==========================================  ============================

Only the *Original* column of the statistics table is imported; presolved
figures depend on the presolver used and are not properties of the instance.

The instance identifier carries a formatter URL
(``https://miplib.zib.de/instance_details_$1.html``), so it resolves to the
MIPLIB page on its own and no per-instance URL is stored.

The paper is linked to ``Q2062317``, the zbMATH-derived item, in preference to
``Q6824324`` — a thinner MathAlgoDB-derived duplicate of the same DOI.

Usage
-----

.. code-block:: bash

   python -m mardi_importer.scripts.import --mode miplib

Requires ``MIPLIB_USER`` and ``MIPLIB_PASS``, alongside the usual Wikibase
environment variables.

mardi_importer.miplib.MIPLIBSource module
-----------------------------------------

.. automodule:: mardi_importer.miplib
   :members:
   :undoc-members:
   :show-inheritance:
