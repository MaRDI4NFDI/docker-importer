import sys
import json
import re
from SPARQLWrapper import SPARQLWrapper, JSON
from mardi_importer.wikibase.WBMapping import get_wbs_local_id

def SPARQL_exists(instance_of, property, value):
    # Check if there exists an Entity which is an instance of instance_of with a given property and a value.
    endpoint_url = "http://query.portal.mardi4nfdi.de/proxy/wdqs/bigdata/namespace/wdq/sparql"
    
    instance_property = get_wbs_local_id("P31")
    query = f"SELECT ?item WHERE {{ ?item wdt:{instance_property} wd:{instance_of}. ?item wdt:{property} \"{value}\". }}"

    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()

    for result in results["results"]["bindings"]:
        if ("item" in result.keys()):
            return re.findall("Q\d+", result['item']['value'])[0]
        
    return None