from mardi_importer.wikibase.WBItem import WBItem


class Journal:
    """Class to manage Journal items in the local Wikibase instance.

    Attributes:


    """

    def __init__(self, label, publisher):
        self.label = label
        self.publisher = publisher

    def exists_in_wikidata(self):
        """
        Checks if a WB item corresponding to the journal exists in Wikidat; if yes, it gets imported.
        Searches by label.
        """
        pass

    def exists(self):
        """
        Checks if a WB item corresponding to the journal already exists.

        Searches for a WB item with the journal label and returns **True**
        if a matching result is found.
        """
        item = WBItem(self.label)
        return not not item.SQL_exists()

    def import_item(self):
        """
        Imports journal item from wikidata
        """
        pass

    def create(self):
        """Creates a WB item with the imported journal metadata from zbMath.

        Before creating the new entity
        corresponding to a journal, a new entity corresponding to the publisher has alreday been created,
        when it did not already exist in the local Wikibase instance.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the
        corresponding new item.
        """
        # Create journal Item with label
        item = WBItem(self.label)

        # Instance of: scientific journal/academic journal?
        item.add_statement("?", "?")

        # publisher: publisher
        item.add_statement("?", self.publisher)
