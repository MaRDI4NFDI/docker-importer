Introduction
============

What is this about?


Installation
============

Without Docker
--------------

Install the python package of :code:`mardi-importer` by first installing the
requirements from :code:`requirements.txt`,

.. code:: shell

   pip install -r requirements.txt

Then install the packages via

.. code:: shell

   pip install -U -e .

:code:`-U` enforces reinstalling the package, with :code:`-e` modifications in
the source files are automatically taken into account.

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
module`. To view the docs, open the file :code:`docs/src/index.html`.


Usage
=====

TODO

