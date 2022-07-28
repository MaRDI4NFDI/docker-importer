from mardi_importer.wikibase.WBItem import WBItem
import requests
import os
from mardi_importer.wikidata.EntityCreator import EntityCreator
from mardi_importer.importer.Importer import ImporterException
import pandas as pd
from .misc import get_internal_id


class Publisher:
    """Class to manage Publisher items in the local Wikibase instance.

    Attributes:


    """

    def __init__(self, label, tracker):
        self.label = label
        self.tracker = tracker
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
        if not self.wikidata_id:
            raise Exception("Publisher does not have a wikidata_id in import_item")
        # call WikibaseImport from the Wikibase container to import the properties from Wikidata

        # re-add --do-not-recurse before --conf
        command = "php /var/www/html/extensions/WikibaseImport/maintenance/importEntities.php --entity {} --conf /shared/LocalSettings.php".format(
            self.wikidata_id
        )
        return_code = os.system(command)
        if return_code != 0:
            raise ImporterException(f"Error attempting to import {self.wikidata_id}")
        self.internal_id = get_internal_id(self.wikidata_id)

    def create(self):
        """Creates a WB item with the imported publisher metadata from zbMath.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the
        corresponding new item.
        """
        # Create publisher Item with label
        item = WBItem(self.label)

        # Instance of: Wikidata property to indicate a source
        if "instance of" not in self.tracker.property_id_mapping:
            instance_of = "WD_Q21503252"
        else:
            instance_of = self.tracker.property_id_mapping["instance_of"]
        if (
            "Wikidata property to indicate a source"
            not in self.tracker.property_id_mapping
        ):
            source = "WD_Q18608359"
        else:
            source = self.tracker.property_id_mapping[
                "Wikidata property to indicate a source"
            ]
        item.add_statement(instance_of, source)
