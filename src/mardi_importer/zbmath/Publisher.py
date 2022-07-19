from mardi_importer.wikibase.WBItem import WBItem
import requests


class Publisher:
    """Class to manage Publisher items in the local Wikibase instance.

    Attributes:


    """

    def __init__(self, label):
        self.label = label
        self.wikidata_id = None
        self.internal_id = None

    def exists_in_wikidata(self):
        """
        Checks if a WB item corresponding to the publisher exists in Wikidat; if yes, it gets imported.
        Searches by label.
        """
        base_url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&language=en&search="
        response_json = requests.get(base_url + self.label).json()
        if response_json["search"]:
            self.wikidata_id = response_json["search"][0]["id"]
            return True
        else:
            return False

    def exists(self):
        """
        Checks if a WB item corresponding to the publisher already exists.

        Searches for a WB item with the publisher label and returns **True**
        if a matching result is found.
        """
        item = WBItem(self.label)
        return not not item.SQL_exists()

    def import_item(self):
        """
        Imports publisher item from wikidata
        """
        pass

    def create(self):
        """Creates a WB item with the imported publisher metadata from zbMath.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the
        corresponding new item.
        """
        # Create publisher Item with label
        item = WBItem(self.label)

        # Instance of: Wikidata property to indicate a source
        item.add_statement("?", "?")
