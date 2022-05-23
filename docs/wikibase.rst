Wikibase module
===============

Classes to interact with the local wikibase instance, i.e. the 
MaRDI knowledge graph.

The connection to the local Wikibase instance to create and edit
entities requires the creation of a bot with the corresponding
permissions.

The required bot can be created on the MediaWiki instance in the
`Special:BotPasswords page <http://localhost:8000/wiki/Special:BotPasswords>`_.

1. Login to the as Admin.
2. Create a bot user (e.g. import).
3. Grant it "High-volume editing", "Edit existing pages" and "Create, 
   edit, and move pages".
4. Replace the username and password in `config/credentials.ini` by those 
   of the newly created bot user.

WBAPIConnection class
---------------------

.. automodule:: mardi_importer.wikibase.WBAPIConnection
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

WBEntity class
--------------

.. automodule:: mardi_importer.wikibase.WBEntity
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

WBItem class
------------

.. automodule:: mardi_importer.wikibase.WBItem
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

WBProperty class
----------------

.. automodule:: mardi_importer.wikibase.WBProperty
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

WBMapping module
----------------

.. automodule:: mardi_importer.wikibase.WBMapping
   :members:
   :undoc-members:
   :show-inheritance: