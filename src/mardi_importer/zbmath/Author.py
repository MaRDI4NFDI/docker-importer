from mardi_importer.wikibase.WBItem import WBItem


class Author:
    """Class to manage Author items in the local Wikibase instance.

    Attributes:


    """

    def __init__(self, label, given_names, surname, orcid, zbmath_author_id):
        self.label = label
        self.given_names = given_names
        self.surname = surname
        self.orcid = orcid
        self.zbmath_author_id = zbmath_author_id

    def exists_in_wikidata(self):
        """
        Checks if a WB item corresponding to the author exists in Wikidat; if yes, it gets imported.
        Searches by ?.
        """
        pass

    def exists(self):
        """
        Checks if a WB item corresponding to the author already exists.

        Searches for a WB item with the author label and returns **True**
        if a matching result is found.
        """
        item = WBItem(self.label)
        return not not item.SQL_exists()

    def import_item(self):
        """
        Imports author item from wikidata
        """
        pass

    def create(self):
        """Creates a WB item with the imported author metadata from zbMath.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the
        corresponding new item.
        """
        # Create publisher Item with label
        item = WBItem(self.label)

        # Instance of: human
        item.add_statement("?", "?")

        # given name
        for given_name in self.given_names:
            item.add_statement("?", given_name)

        # family name
        item.add_statement("?", self.surname)

        # orcid
        item.add_statement("?", self.orcid)

        # zbmath_author_id
        item.add_statement("?", self.zbmath_author_id)
