Importer Module for Julia
=========================

This module imports Julia packages for numerical analysis from the
`Julia General registry <https://github.com/JuliaRegistries/General>`_, the
community registry of Julia packages (MIT-licensed). The registry plays the role
CRAN plays for R; a package's own repository supplies the rest.

Scope
-----

Packages whose repository is under one of the GitHub organisations SciML,
JuliaMath, JuliaLinearAlgebra, JuliaNLSolvers or JuliaDiff. Binary wrappers
(``_jll`` packages) are excluded, and so are packages registered in a
sub-directory of a shared repository (internal splits such as the
``OrdinaryDiffEq*`` solver packages), except StochasticDiffEq and DelayDiffEq.
About 290 packages.

What is imported
----------------

From the registry, pinned to the registry commit:

:Name: As the identifier *Julia General registry package name* (without
  ``.jl``), linked to the package's page on JuliaHub.
:Repository: *source code repository*.
:Version: The latest non-yanked version, as *software version identifier*.

From the package's repository, pinned to a commit:

:License: Read from the licence file. The licence named at the start of the
  file decides; bundled third-party notices further down are ignored. A package
  whose licence cannot be determined is not written.
:Authors: From ``Project.toml`` and the software authors of ``CITATION.cff``.
  A person with an item becomes *author*, with the name as stated in *object
  named as*; anyone else an *author name string*.
:Dependencies: *depends on software*, to packages that exist in the knowledge
  graph or are imported in the same run.
:Publications: DOIs and arXiv ids from ``CITATION.bib``/``CITATION.cff`` become
  *described by source*. Zenodo DOIs are the software's own releases and are
  skipped.

Every statement carries a reference: *stated in* the Julia General registry for
registry facts, the *reference URL* of the file it was read from, and the
*retrieved* date.

Matching existing items
-----------------------

An existing item is updated only when it is the same software: it already
carries the package's registry name, or it records the package's own
repository, or it has the package's name and a repository named ``<Name>.jl``
(the same package before its repository moved organisation). A name shared with
different software — SUNDIALS and Sundials.jl, the FFTW C library and FFTW.jl —
creates a new item and leaves the existing one untouched. Updates only add
statements; nothing is overwritten or removed.

People
------

An existing person item is reused only when it is certainly the same person: it
carries the person's ORCID, or it is the same-named author of a paper the
person's own package cites. Otherwise a person who can be identified — the same
e-mail address across packages, a co-author of a cited paper being created, or
an ORCID — gets a new item; similarly named existing items are logged as
possible duplicates for manual merging. E-mail addresses are used only to group
mentions of the same person and are never written; every written value is
checked for one.

Publications
------------

A cited DOI or arXiv id already in the knowledge graph is linked. Otherwise the
journal DOI of an arXiv preprint and the exact title are tried, and only then is
the publication created, through the Crossref or arXiv module.

Usage
-----

::

  mardi-importer import-julia --dry-run            # decide and report, write nothing
  mardi-importer import-julia                      # full import of every package in scope
  mardi-importer import-julia --packages Optim NLsolve

Credentials are read from ``JULIA_USER`` / ``JULIA_PASS``; publications use the
Crossref and arXiv credentials. Re-running is safe: packages are found again by
their registry name and only new information is added.

mardi_importer.julia.JuliaSource class
--------------------------------------

.. automodule:: mardi_importer.julia.JuliaSource
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

mardi_importer.julia.JuliaPackage class
---------------------------------------

.. automodule:: mardi_importer.julia.JuliaPackage
   :members:
   :undoc-members:
   :show-inheritance:
