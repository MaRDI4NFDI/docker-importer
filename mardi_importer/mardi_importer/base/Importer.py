from abc import ABC, abstractmethod
from mardiclient import MardiClient
from mardi_importer.wikidata import WikidataImporter
from typing import Dict, Any
import logging
import inspect
import os
import json

class Importer:
    """Controller class for importing data from an external source to the local Wikibase."""

    def __init__(self, dataSource: "ADataSource"):
        """
        Construct.
        Args:
            dataSource: object implementig ADataSource
        """
        self.dataSource = dataSource

    def import_all(self, pull=True, push=True) -> None:
        """
        Manages the import process.
        """
        self.dataSource.setup()
        if pull:
            self.dataSource.pull()
        if push:
            self.dataSource.push()

class ADataSource(ABC):
    """Abstract base class for reading data from external sources."""
    
    def __init__(self, user: str, password: str):
        """Initialize common attributes for all sources.
        
        Args:
            user: Username for authentication
            password: Password for authentication
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.filepath = os.path.realpath(os.path.dirname(inspect.getfile(self.__class__)))
        self.api = MardiClient(
            user=user, 
            password=password,
            mediawiki_api_url=os.environ.get("MEDIAWIKI_API_URL"),
            sparql_endpoint_url=os.environ.get("SPARQL_ENDPOINT_URL"),
            wikibase_url=os.environ.get("WIKIBASE_URL"),
            importer_api_url="http://importer-api"
        )
        self.wdi = WikidataImporter()

    def import_wikidata_entities(self, filename: str):
        filename = self.filepath + filename
        self.wdi.import_entities(filename=filename)

    def create_local_entities(self, filename: str):
        filename = self.filepath + filename
        f = open(filename)
        entities = json.load(f)

        for prop_element in entities['properties']:
            prop = self.api.property.new()
            prop.labels.set(language='en', value=prop_element['label'])
            prop.descriptions.set(language='en', value=prop_element['description'])
            prop.datatype = prop_element['datatype']
            if not prop.exists(): prop.write()

        for item_element in entities['items']:
            item = self.api.item.new()
            item.labels.set(language='en', value=item_element['label'])
            item.descriptions.set(language='en', value=item_element['description'])
            for key, value in item_element['claims'].items():
                item.add_claim(key,value=value)
            if not item.exists(): item.write()

    @abstractmethod
    def setup(self) -> None:
        """Set up the data source connection/configuration."""
        pass

    @abstractmethod
    def pull(self) -> None:
        """Pull data from the external source."""
        pass
    
    @abstractmethod
    def push(self) -> None:
        """Push data to the MaRDI knowledge graph."""
        pass
