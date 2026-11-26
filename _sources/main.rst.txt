Getting Started
===============

What is this about?


Installation
------------

Without Docker
--------------

The project uses a :code:`src/` layout, so it has to be installed before
anything is importable. Dependencies are declared in :code:`pyproject.toml` and
are resolved by pip:

.. code:: shell

   pip install -U -e .

This installs both the :code:`mardi_importer` and :code:`mardi_portal`
packages. :code:`-U` enforces reinstalling the package, with :code:`-e`
modifications in the source files are automatically taken into account.

Note: for convenience, local installations not using docker can be placed within
virtual environments by first calling

.. code:: shell

   python3 -m venv env
   source env/bin/activate

With Docker
-----------

TODO


Build documentation
-------------------

In :code:`docs/`, run :code:`make html` to generate the documentation for a
local installation. The modules have to be installed and findable by :code:`import
module`. To view the docs, open the file :code:`docs/_build/html/index.html`.



Version
-------

The release version is derived from the git tag by setuptools-scm; there is no
checked-in ``VERSION`` file. It is available via:

- CLI: ``mardi-importer --version``
- Flask: ``GET /version``

