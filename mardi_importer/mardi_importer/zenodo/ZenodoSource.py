import requests
import json
import os

from mardi_importer.importer import ADataSource
from mardi_importer.integrator import MardiIntegrator
from mardi_importer.publications import ZenodoResource

class ZenodoSource(ADataSource):
    """Reads data from Zenodo API."""

    def __init__(
        self
    ):    
        self.integrator = MardiIntegrator()
        self.zenodo_ids = []    
        self.filepath = os.path.realpath(os.path.dirname(__file__))

    def setup(self):
        """Create all necessary properties and entities for zenodo"""
        
        filename = self.filepath + "/wikidata_entities.txt"
        self.integrator.import_entities(filename=filename)
        self.create_local_entities() 

    def create_local_entities(self):
        filename = self.filepath + "/new_entities.json"
        f = open(filename)
        entities = json.load(f)

        for prop_element in entities["properties"]:
            prop = self.integrator.property.new()
            prop.labels.set(language="en", value=prop_element["label"])
            prop.descriptions.set(language="en", value=prop_element["description"])
            prop.datatype = prop_element["datatype"]
            if not prop.exists():
                prop.write()

        for item_element in entities["items"]:
            item = self.integrator.item.new()
            item.labels.set(language="en", value=item_element["label"])
            item.descriptions.set(language="en", value=item_element["description"])
            if "claims" in item_element:
                for key, value in item_element["claims"].items():
                    item.add_claim(key, value=value)
            if not item.exists():
                item.write()

    def pull(self):
        """
        This method queries the Zenodo API to get a data dump of all records.
        """

        response = requests.get('https://zenodo.org/api/records',
                                params={'size' : 1,
                                'communities' : 'mathplus'})
        response_json = response.json()
        total_hits = response_json.get("hits").get("total")

        for page in range(1, total_hits+1):
            url = 'https://zenodo.org/api/records?communities=mathplus&page=' + str(page) + "&size=1&sort=newest"
            response = requests.get(url)    
            response_json = response.json()

            zenodo_id = response_json.get("hits").get("hits")[0].get("id")
            self.zenodo_ids.append(str(zenodo_id))

    def push(self):
        for id in self.zenodo_ids:   
            entry = ZenodoResource.ZenodoResource(
                self.integrator,
                zenodo_id = id
            )

            if not entry.exists():
                print (f"Creating entry for zenodo id {id}")
                entry.create(update = False)
            else:
                print (f"Entry for zenodo id: {id} already exists. Updating.")
                entry.create(update = True)

