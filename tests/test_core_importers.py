import unittest
from unittest.mock import patch, Mock, MagicMock, mock_open
import os
import sys
import types
import logging

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

def _install_mardi_importer_inner_stub() -> None:
    if "mardi_importer.mardi_importer" in sys.modules:
        return

    inner_path = os.path.join(REPO_ROOT, "mardi_importer", "mardi_importer")
    inner_module = types.ModuleType("mardi_importer.mardi_importer")

    class Importer:
        registry = {}

        @classmethod
        def register(cls, *_args, **_kwargs):
            return None

        @classmethod
        def get_api(cls, *_args, **_kwargs):
            return Mock()

    inner_module.Importer = Importer
    inner_module.__path__ = [inner_path]

    sys.modules["mardi_importer.mardi_importer"] = inner_module
    try:
        import importlib
        top_level = importlib.import_module("mardi_importer")
        setattr(top_level, "mardi_importer", inner_module)
    except ModuleNotFoundError:
        pass

_install_mardi_importer_inner_stub()

# Set up a minimal Flask stub for app imports if not already done
def _install_flask_stub() -> None:
    if "flask" in sys.modules:
        return

    def jsonify(data=None, **kwargs):
        if kwargs:
            return kwargs
        return data

    fake_request = Mock()
    fake_request.args = Mock()
    fake_request.args.get.return_value = None
    fake_request.get_json.return_value = {}

    class FakeFlask:
        def __init__(self, name):
            self.name = name

        def get(self, path):
            def decorator(func):
                return func
            return decorator

        def post(self, path):
            def decorator(func):
                return func
            return decorator

    fake_flask = types.ModuleType("flask")
    fake_flask.Flask = FakeFlask
    fake_flask.jsonify = jsonify
    fake_flask.request = fake_request

    sys.modules["flask"] = fake_flask

_install_flask_stub()

def _install_mardiclient_stub() -> None:
    if "mardiclient" in sys.modules:
        return

    mardiclient_module = types.ModuleType("mardiclient")

    class MardiClient:
        @staticmethod
        def _config(*_args, **_kwargs):
            return None

        def __init__(self, *args, **kwargs):
            pass

    class MardiItem:
        pass

    mardiclient_module.MardiClient = MardiClient
    mardiclient_module.MardiItem = MardiItem

    sys.modules["mardiclient"] = mardiclient_module

_install_mardiclient_stub()

def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    sqlalchemy_module = types.ModuleType("sqlalchemy")
    schema_module = types.ModuleType("sqlalchemy.schema")

    class MetaData:
        def create_all(self, *_args, **_kwargs):
            return None

    class Table:
        def __init__(self, *_args, **_kwargs):
            pass

        def insert(self):
            return self

        def update(self):
            return self

        def values(self, **_kwargs):
            return self

    class Column:
        def __init__(self, *_args, **_kwargs):
            pass

    class Integer:
        pass

    class Boolean:
        def __init__(self, *_args, **_kwargs):
            pass

    def create_engine(*_args, **_kwargs):
        return Mock()

    def inspect(*_args, **_kwargs):
        return Mock(has_table=Mock(return_value=False))

    schema_module.MetaData = MetaData

    sqlalchemy_module.MetaData = MetaData
    sqlalchemy_module.Table = Table
    sqlalchemy_module.Column = Column
    sqlalchemy_module.Integer = Integer
    sqlalchemy_module.Boolean = Boolean
    sqlalchemy_module.create_engine = create_engine
    sqlalchemy_module.inspect = inspect
    sqlalchemy_module.schema = schema_module

    sys.modules["sqlalchemy"] = sqlalchemy_module
    sys.modules["sqlalchemy.schema"] = schema_module

_install_sqlalchemy_stub()

def _install_wikibaseintegrator_stub() -> None:
    if "wikibaseintegrator" in sys.modules:
        return

    wbi_module = types.ModuleType("wikibaseintegrator")
    models_module = types.ModuleType("wikibaseintegrator.models")
    config_module = types.ModuleType("wikibaseintegrator.wbi_config")
    enums_module = types.ModuleType("wikibaseintegrator.wbi_enums")
    datatypes_module = types.ModuleType("wikibaseintegrator.datatypes")
    exceptions_module = types.ModuleType("wikibaseintegrator.wbi_exceptions")

    class Claim:
        pass

    class Claims:
        def __init__(self):
            self.claims = {}

    class Qualifiers:
        pass

    class Reference:
        pass

    class Sitelinks:
        def __init__(self):
            self.sitelinks = {}

    class ActionIfExists:
        pass

    class URL:
        pass

    class CommonsMedia:
        pass

    class ExternalID:
        pass

    class Form:
        pass

    class GeoShape:
        pass

    class GlobeCoordinate:
        pass

    class Item:
        pass

    class Lexeme:
        pass

    class Math:
        pass

    class MonolingualText:
        pass

    class MusicalNotation:
        pass

    class Property:
        pass

    class Quantity:
        pass

    class Sense:
        pass

    class String:
        pass

    class TabularData:
        pass

    class Time:
        pass

    class ModificationFailed(Exception):
        pass

    models_module.Claim = Claim
    models_module.Claims = Claims
    models_module.Qualifiers = Qualifiers
    models_module.Reference = Reference
    models_module.Sitelinks = Sitelinks

    config_module.config = {}

    enums_module.ActionIfExists = ActionIfExists

    datatypes_module.URL = URL
    datatypes_module.CommonsMedia = CommonsMedia
    datatypes_module.ExternalID = ExternalID
    datatypes_module.Form = Form
    datatypes_module.GeoShape = GeoShape
    datatypes_module.GlobeCoordinate = GlobeCoordinate
    datatypes_module.Item = Item
    datatypes_module.Lexeme = Lexeme
    datatypes_module.Math = Math
    datatypes_module.MonolingualText = MonolingualText
    datatypes_module.MusicalNotation = MusicalNotation
    datatypes_module.Property = Property
    datatypes_module.Quantity = Quantity
    datatypes_module.Sense = Sense
    datatypes_module.String = String
    datatypes_module.TabularData = TabularData
    datatypes_module.Time = Time

    exceptions_module.ModificationFailed = ModificationFailed

    sys.modules["wikibaseintegrator"] = wbi_module
    sys.modules["wikibaseintegrator.models"] = models_module
    sys.modules["wikibaseintegrator.wbi_config"] = config_module
    sys.modules["wikibaseintegrator.wbi_enums"] = enums_module
    sys.modules["wikibaseintegrator.datatypes"] = datatypes_module
    sys.modules["wikibaseintegrator.wbi_exceptions"] = exceptions_module

_install_wikibaseintegrator_stub()

def _install_feedparser_stub() -> None:
    if "feedparser" in sys.modules:
        return

    feedparser_module = types.ModuleType("feedparser")
    util_module = types.ModuleType("feedparser.util")

    class FeedParserDict(dict):
        pass

    def parse(*_args, **_kwargs):
        return Mock(entries=[])

    feedparser_module.parse = parse
    util_module.FeedParserDict = FeedParserDict

    sys.modules["feedparser"] = feedparser_module
    sys.modules["feedparser.util"] = util_module

_install_feedparser_stub()

def _install_requests_stub() -> None:
    if "requests" in sys.modules:
        return

    requests_module = types.ModuleType("requests")

    def get(*_args, **_kwargs):
        return Mock(text="", content=b"")

    requests_module.get = get

    sys.modules["requests"] = requests_module

_install_requests_stub()

def _install_bs4_stub() -> None:
    if "bs4" in sys.modules:
        return

    bs4_module = types.ModuleType("bs4")

    class BeautifulSoup:
        def __init__(self, *_args, **_kwargs):
            pass

        def find(self, *_args, **_kwargs):
            return None

        def find_all(self, *_args, **_kwargs):
            return []

    bs4_module.BeautifulSoup = BeautifulSoup

    sys.modules["bs4"] = bs4_module

_install_bs4_stub()

def _install_nameparser_stub() -> None:
    if "nameparser" in sys.modules:
        return

    nameparser_module = types.ModuleType("nameparser")
    config_module = types.ModuleType("nameparser.config")

    class HumanName:
        def __init__(self, *_args, **_kwargs):
            self.first = ""
            self.last = ""
            self.middle = ""
            self.title = ""

    nameparser_module.HumanName = HumanName

    class DummyConstants:
        def __init__(self):
            self.titles = ["Mahdi", "Bodhisattva"]

    config_module.CONSTANTS = DummyConstants()

    sys.modules["nameparser"] = nameparser_module
    sys.modules["nameparser.config"] = config_module

_install_nameparser_stub()

def _install_mardi_utils_stub() -> None:
    if "mardi_importer.utils" in sys.modules:
        return

    utils_module = types.ModuleType("mardi_importer.utils")
    author_module = types.ModuleType("mardi_importer.utils.Author")

    class Author:
        def __init__(self, *args, **kwargs):
            self.api = kwargs.get("api")
            self.name = kwargs.get("name")

    utils_module.Author = Author
    utils_module.__all__ = ["Author"]
    author_module.Author = Author

    sys.modules["mardi_importer.utils"] = utils_module
    sys.modules["mardi_importer.utils.Author"] = author_module

_install_mardi_utils_stub()

def _install_coolname_stub() -> None:
    if "coolname" in sys.modules:
        return

    coolname_module = types.ModuleType("coolname")
    impl_module = types.ModuleType("coolname.impl")

    def load_config():
        return {}

    impl_module.load_config = load_config
    coolname_module.impl = impl_module

    sys.modules["coolname"] = coolname_module
    sys.modules["coolname.impl"] = impl_module

_install_coolname_stub()

coolname_load_config_patcher = patch('coolname.impl.load_config', return_value={})
coolname_load_config_patcher.start()

# Import necessary modules
import importlib

from mardi_importer.mardi_importer.wikidata.WikidataImporter import WikidataImporter
from mardi_importer.mardi_importer.arxiv.ArxivSource import ArxivSource
from mardi_importer.mardi_importer.arxiv.ArxivPublication import ArxivPublication

ADataSourceModule = importlib.import_module("mardi_importer.base.ADataSource")

# Mock external dependencies (environment variables)
MOCK_ENV_VARS = {
    "WIKIDATA_USER": "test_wd_user",
    "WIKIDATA_PASS": "test_wd_pass",
    "MEDIAWIKI_API_URL": "http://test.mediawiki.org/api.php",
    "SPARQL_ENDPOINT_URL": "http://test.sparql.org",
    "WIKIBASE_URL": "http://test.wikibase.org",
    "IMPORTER_API_URL": "http://test.importer.org",
    "DB_USER": "test_db_user",
    "DB_PASS": "test_db_pass",
    "DB_NAME": "test_db",
    "DB_HOST": "test_db_host",
    "ARXIV_USER": "test_arxiv_user",
    "ARXIV_PASS": "test_arxiv_pass",
}

class TestCoreImporters(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher_env = patch.dict('os.environ', MOCK_ENV_VARS)
        self.patcher_env.start()

        # Suppress logging output during tests
        self.logger_wikidata = logging.getLogger("mardi_importer.wikidata.WikidataImporter")
        self.logger_arxiv = logging.getLogger("mardi_importer.arxiv.ArxivSource")
        self.logger_datasource = logging.getLogger("mardi_importer.base.ADataSource")
        
        self.original_wikidata_level = self.logger_wikidata.level
        self.original_arxiv_level = self.logger_arxiv.level
        self.original_datasource_level = self.logger_datasource.level
        
        self.logger_wikidata.setLevel(logging.CRITICAL)
        self.logger_arxiv.setLevel(logging.CRITICAL)
        self.logger_datasource.setLevel(logging.CRITICAL)

    def tearDown(self) -> None:
        self.patcher_env.stop()
        
        self.logger_wikidata.setLevel(self.original_wikidata_level)
        self.logger_arxiv.setLevel(self.original_arxiv_level)
        self.logger_datasource.setLevel(self.original_datasource_level)

    @patch('mardiclient.MardiClient._config') # Patching _config method
    @patch('sqlalchemy.schema.MetaData.create_all')
    @patch('mardi_importer.mardi_importer.wikidata.WikidataImporter.MardiClient')
    @patch('sqlalchemy.create_engine')
    @patch('wikibaseintegrator.wbi_config.config', new={'WIKIBASE_URL': MOCK_ENV_VARS["WIKIBASE_URL"]})
    def test_wikidata_importer_init_and_setup(
        self,
        mock_create_engine,
        mock_mardi_client,
        mock_metadata_create_all,
        mock_config # Argument for the _config patch
    ) -> None:
        # Configure mock_config to return a mock Clientlogin object
        mock_config.return_value = Mock(login=Mock())

        # Mock objects that Mardiclient returns
        mock_property_obj = Mock()
        mock_property_obj.new.return_value = Mock()
        mock_property_obj.new.return_value.labels.set.return_value = None
        mock_property_obj.new.return_value.descriptions.set.return_value = None
        mock_property_obj.new.return_value.exists.return_value = "P123" # PID exists
        mock_property_obj.new.return_value.write.return_value.id = "P123"
        mock_property_obj.get.return_value = Mock() # For _get_wikidata_information

        mock_mardi_client_instance = Mock()
        mock_mardi_client_instance.login = mock_config.return_value.login # Use the mocked Clientlogin from _config
        mock_mardi_client_instance.property = mock_property_obj
        mock_mardi_client_instance.item = Mock()

        mock_mardi_client.return_value = mock_mardi_client_instance

        # Mock SQLAlchemy engine and connection
        mock_connection = Mock()
        mock_connect_method = Mock()
        mock_connect_method.return_value = mock_connection
        mock_connect_method.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_connect_method.return_value.__exit__ = Mock(return_value=False)

        mock_engine = Mock()
        mock_engine.connect = mock_connect_method
        mock_create_engine.return_value = mock_engine

        # Mock inspect.has_table
        with patch('sqlalchemy.inspect') as mock_inspect:
            mock_inspect.return_value.has_table.return_value = False # Tables don't exist yet

            # Reset singleton instance for proper re-initialization
            WikidataImporter._instance = None
            WikidataImporter._initialized = False

            importer = WikidataImporter()

            mock_mardi_client.assert_called_once_with(
                user=MOCK_ENV_VARS["WIKIDATA_USER"],
                password=MOCK_ENV_VARS["WIKIDATA_PASS"],
                mediawiki_api_url=MOCK_ENV_VARS["MEDIAWIKI_API_URL"],
                sparql_endpoint_url=MOCK_ENV_VARS["SPARQL_ENDPOINT_URL"],
                wikibase_url=MOCK_ENV_VARS["WIKIBASE_URL"],
                importer_api_url=MOCK_ENV_VARS["IMPORTER_API_URL"],
                user_agent="MaRDI4NFDI (portal.mardi4nfdi.de; urgent_ta5@mardi4nfdi.de)"
            )
            self.assertTrue(mock_create_engine.called)
            self.assertTrue(mock_inspect.called)
            self.assertTrue(mock_metadata_create_all.called) # Assert that create_all was called
            self.assertTrue(mock_mardi_client_instance.property.new.called) # _init_wikidata_PID/_QID

    @patch('mardiclient.MardiClient._config') # Patching _config method
    @patch('mardi_importer.mardi_importer.wikidata.WikidataImporter.MardiClient')
    @patch('sqlalchemy.create_engine')
    @patch('wikibaseintegrator.wbi_config.config', new={'WIKIBASE_URL': MOCK_ENV_VARS["WIKIBASE_URL"]})
    def test_wikidata_importer_get_wikidata_information(
        self,
        mock_create_engine,
        mock_mardi_client,
        mock_config # Argument for the _config patch
    ) -> None:
        # Configure mock_config to return a mock Clientlogin object
        mock_config.return_value = Mock(login=Mock())

        # Setup mocks for Mardiclient and its internal methods
        mock_item_get = Mock()
        mock_item_get.return_value = Mock(
            labels=Mock(values={'en': 'Test Item', 'de': 'Test Gegenstand'}),
            descriptions=Mock(values={'en': 'A test item'}),
            aliases=Mock(aliases={}),
            claims=Mock(),
            sitelinks=Mock(),
            type='item',
            datatype=Mock(value='wikibase-item')
        )
        mock_property_obj = Mock()
        mock_property_obj.new.return_value = Mock(
            exists=Mock(return_value="P123"),
            write=Mock(return_value=Mock(id="P123"))
        )
        mock_property_obj.get.return_value = Mock()

        mock_mardi_client_instance = Mock(
            item=Mock(get=mock_item_get),
            property=mock_property_obj,
            login=mock_config.return_value.login, # Use the mocked Clientlogin from _config
        )
        mock_mardi_client.return_value = mock_mardi_client_instance

        # Mock SQLAlchemy engine and connection (not directly used by _get_wikidata_information, but by init/setup)
        mock_connection = Mock()
        mock_connect_method = Mock()
        mock_connect_method.return_value = mock_connection
        mock_connect_method.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_connect_method.return_value.__exit__ = Mock(return_value=False)

        mock_engine = Mock()
        mock_engine.connect = mock_connect_method
        mock_create_engine.return_value = mock_engine

        # Mock inspect.has_table
        with patch('sqlalchemy.inspect') as mock_inspect:
            mock_inspect.return_value.has_table.return_value = True # Assume tables exist

            # Reset singleton instance for proper re-initialization
            WikidataImporter._instance = None
            WikidataImporter._initialized = False

            importer = WikidataImporter(languages=["en"])
            
            # Test with a valid QID
            entity = importer._get_wikidata_information("Q123", recurse=False)

            mock_item_get.assert_called_once_with(entity_id="Q123", mediawiki_api_url='https://www.wikidata.org/w/api.php')
            self.assertEqual(entity.labels.values['en'], 'Test Item')
            self.assertNotIn('de', entity.labels.values) # Language filtering
            self.assertEqual(entity.descriptions.values['en'], 'A test item')
            self.assertFalse(entity.claims.claims) # claims should be empty if recurse=False
            self.assertFalse(entity.sitelinks.sitelinks) # sitelinks should be empty

            # Test with an invalid ID format
            with self.assertRaises(ValueError):
                importer._get_wikidata_information("X123")


class TestArxivSource(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher_env = patch.dict('os.environ', MOCK_ENV_VARS)
        self.patcher_env.start()
        # Suppress logging output during tests
        self.logger_arxiv = logging.getLogger("mardi_importer.arxiv.ArxivSource")
        self.logger_datasource = logging.getLogger("mardi_importer.base.ADataSource")
        self.original_arxiv_level = self.logger_arxiv.level
        self.original_datasource_level = self.logger_datasource.level
        self.logger_arxiv.setLevel(logging.CRITICAL)
        self.logger_datasource.setLevel(logging.CRITICAL)

    def tearDown(self) -> None:
        self.patcher_env.stop()
        
        self.logger_arxiv.setLevel(self.original_arxiv_level)
        self.logger_datasource.setLevel(self.original_datasource_level)
    
    @patch('mardiclient.MardiClient._config') # Patching _config method
    @patch.object(ADataSourceModule, "MardiClient")
    @patch.object(ADataSourceModule, "WikidataImporter")
    @patch.object(ArxivPublication, "__post_init__", return_value=None)
    @patch('os.path.exists', return_value=False) # To ensure setup() is called
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open) # Use mock_open
    def test_arxiv_source_init_and_setup(
        self,
        mock_open,
        mock_makedirs,
        mock_path_exists,
        mock_arxiv_post_init,
        mock_wikidata_importer,
        mock_mardi_client,
        mock_config # Argument for the _config patch
    ) -> None:
        # Configure mock_config to return a mock Clientlogin object
        mock_config.return_value = Mock(login=Mock())
        
        # Mock ADataSource.__init__
        mock_mardi_client_instance = Mock()
        mock_mardi_client.return_value = mock_mardi_client_instance
        mock_mardi_client_instance.login = mock_config.return_value.login # Ensure login attribute is correctly mocked

        # Mock WikidataImporter instance returned by ADataSource.wdi property
        mock_wdi_instance = mock_wikidata_importer.return_value
        mock_wdi_instance.import_entities.return_value = None

        # Reset singleton instance for proper re-initialization
        ArxivSource._instances = {}
        ArxivSource._initialized = set()
        ArxivSource._setup_complete = set()

        arxiv_source = ArxivSource(
            user=MOCK_ENV_VARS["ARXIV_USER"],
            password=MOCK_ENV_VARS["ARXIV_PASS"]
        )

        mock_mardi_client.assert_called_once_with(
            user=MOCK_ENV_VARS["ARXIV_USER"],
            password=MOCK_ENV_VARS["ARXIV_PASS"],
            mediawiki_api_url=MOCK_ENV_VARS["MEDIAWIKI_API_URL"],
            sparql_endpoint_url=MOCK_ENV_VARS["SPARQL_ENDPOINT_URL"],
            wikibase_url=MOCK_ENV_VARS["WIKIBASE_URL"],
            importer_api_url=MOCK_ENV_VARS["IMPORTER_API_URL"],
        )
        mock_wdi_instance.import_entities.assert_called_once_with(filename=arxiv_source.filepath + "/wikidata_entities.txt")
        self.assertTrue(mock_makedirs.called)
        mock_open.assert_called_once()

        # Test new_publication
        arxiv_publication = arxiv_source.new_publication("1234.56789")
        self.assertIsInstance(arxiv_publication, ArxivPublication)
        self.assertEqual(arxiv_publication.arxiv_id, "1234.56789")
