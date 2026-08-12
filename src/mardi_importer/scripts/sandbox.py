from mardi_importer.wikidata import WikidataImporter

wi = WikidataImporter()

test = wi.import_entities('Q4917')
print(test)