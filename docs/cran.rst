CRAN module
===========

This module imports R packages published at the Comprehensive R Archive Network `(CRAN) <https://cran.r-project.org/>`_. 

Specifically, it reads the table of `packages ordered by date of publication <https://cran.r-project.org/web/packages/available_packages_by_date.html>`_.
This table contains for each  R package the **package name**, **title** and **date of publication**. Based on the **package name**, 
each package url can be accessed from:

``https://cran.r-project.org/web/packages/<package_name>/index.html``

Several attributes are listed for each package. Among them, the following attributes are imported, when present, to the MaRDI knowledge graph:

:Version: Version of the package.
:Depends: Software and package dependencies, including other R packages.
:Published: Date of publication.
:Author: Authors of the package are to be indicated according to the `CRAN Repository Policy <https://cran.r-project.org/web/packages/policies.html>`_ with the abbreviation \[aut\]. Given that this guideline is not always implemented, it is not always
  possible to properly parse the authors. 

  When no abbrevations describing the role of each individual are included, just the first listed author is imported.
:License: `List of accepted licenses <https://svn.r-project.org/R/trunk/share/licenses/license.db>`_.
:Maintainer: Software maintainer (Generally one of the authors).


mardi_importer.cran.CRANSource class
--------------------------------------------

.. automodule:: mardi_importer.cran.CRANSource
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

mardi_importer.cran.RPackage class
--------------------------------------------

.. automodule:: mardi_importer.cran.RPackage
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource