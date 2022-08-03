def get_wikidata_information(wikidata_id, languages):
    labels = {}
    descriptions = {}
    aliases = {}
    item = self.wikibase_integrator.item.get(entity_id=item_id).get_json()
    for lang in languages:
        labels[lang] = item['labels'].get(lang)
        descriptions[lang] = item['descriptions'].get(lang)
        aliases[lang] = item['aliases'].get(lang)
    wikidata_id = item_id
    if recurse == False:
        claims = None
    else:
        #for each of the ids in the claims, get stuff with recurse = False
        #and add to secondary
        claims = item['claims']