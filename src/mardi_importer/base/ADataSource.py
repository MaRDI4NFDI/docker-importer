from abc import ABC, abstractmethod
from mardiclient import MardiClient
from mardi_importer.wikidata import WikidataImporter
from datetime import datetime
import logging
import inspect
import os
import json

class ADataSource(ABC):
    """Abstract base class for reading data from external sources."""
    _instances = {}
    _initialized = set()
    _setup_complete = set()

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]
    
    def __init__(self, user: str, password: str):
        """Initialize common attributes for all sources.
        
        Args:
            user: Username for authentication
            password: Password for authentication
        """
        if self.__class__ in ADataSource._initialized:
            return

        self.logger = logging.getLogger(self.__class__.__name__)
        self.filepath = os.path.realpath(os.path.dirname(inspect.getfile(self.__class__)))
        self.api = MardiClient(
            user=user, 
            password=password,
            mediawiki_api_url=os.environ.get("MEDIAWIKI_API_URL"),
            sparql_endpoint_url=os.environ.get("SPARQL_ENDPOINT_URL"),
            wikibase_url=os.environ.get("WIKIBASE_URL"),
            importer_api_url=os.environ.get("IMPORTER_API_URL"),
        )
        self._wdi = None
        
        if not self._should_run_setup():
            self.logger.info(f"Setup for {self.__class__.__name__} already complete")
        else:
            self.setup()
            self._mark_setup_complete()

        ADataSource._initialized.add(self.__class__)

    @property
    def wdi(self):
        """Lazy initialization of WikidataImporter."""
        if self._wdi is None:
            self._wdi = WikidataImporter()
        return self._wdi
    
    def _get_setup_marker_path(self) -> str:
        """Get path to setup marker file in a writable location."""
        marker_dir = '/tmp/mardi_importer'
        os.makedirs(marker_dir, exist_ok=True)
        
        marker_filename = f'.{self.__class__.__name__}_setup_complete'
        return os.path.join(marker_dir, marker_filename)
    
    def _should_run_setup(self) -> bool:
        """Check if setup needs to be run."""
        if self.__class__ in ADataSource._setup_complete:
            return False
        
        setup_marker = self._get_setup_marker_path()
        if os.path.exists(setup_marker):
            ADataSource._setup_complete.add(self.__class__)
            return False
        
        return True

    def _mark_setup_complete(self):
        """Mark setup as complete."""
        ADataSource._setup_complete.add(self.__class__)
        setup_marker = self._get_setup_marker_path()
        with open(setup_marker, 'w') as f:
            f.write(f"Setup completed at {datetime.now().isoformat()}\n")

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
            for key, value in item_element.get('claims', {}).items():
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

    def import_all(self, pull=True, push=True) -> None:
        """
        Manages the import process.
        """
        if pull:
            self.pull()
        if push:
            self.push()
