from mardiclient import MardiClient
from mardiclient import config

mc = MardiClient(user="Rim", password="M!dight^2469")

config['IMPORTER_API_URL'] = 'https://importer.staging.mardi4nfdi.org'
config['MEDIAWIKI_API_URL'] = 'https://staging.mardi4nfdi.org/w/api.php'
config['SPARQL_ENDPOINT_URL'] = 'http://query.staging.mardi4nfdi.org/proxy/wdqs/bigdata/namespace/wdq/sparql'
config['WIKIBASE_URL'] = 'https://staging.mardi4nfdi.org'

from mardi_importer.integrator import MardiIntegrator

mi = MardiIntegrator()