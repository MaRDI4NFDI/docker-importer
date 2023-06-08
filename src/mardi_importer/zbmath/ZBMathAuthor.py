from wikibaseintegrator.wbi_helpers import execute_sparql_query, merge_items

class ZBMathAuthor:
    """
    Class to merge zbMath author items in the local wikibase instance
    """
    def __init__(self, integrator, name, zbmath_author_id):
        self.api = integrator
        self.name = name.strip()
        self.QID = None
        self.zbmath_author_id = zbmath_author_id.strip()
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        #instance of: human
        item.add_claim("wdt:P31", "wd:Q5")
        if self.zbmath_author_id:
            self.QID = item.is_instance_of_with_property('wd:Q5', 'wdt:P1556', self.zbmath_author_id)
            QID = self.query_wikidata()
            if QID:
                self.QID = QID
            else:
                item.add_claim('wdt:P1556', self.zbmath_author_id)
        return item  


    def create(self):
        if self.QID: 
            print(f"Author {self.name} exists")
            return self.QID
        print(f"Creating author {self.name}")
        author_id = self.item.write().id
        return(author_id)


    def query_wikidata(self):
        sparql = (f"SELECT ?item WHERE {{ ?item wdt:P31 wd:Q5 . "
                f"?item wdt:P1556 '{self.zbmath_author_id}' . SERVICE wikibase:label "
                f"{{ bd:serviceParam wikibase:language '[AUTO_LANGUAGE],en'. }} }}")
        query = execute_sparql_query(query = sparql, endpoint="https://query.wikidata.org/sparql?")
        result = query["results"]
        #no results
        if not result['bindings']:
            return None
        #more than one result: unlcear
        elif len(result['bindings']) > 1:
            return None
        else:
            wikidata_id = result["bindings"][0]["item"]["value"].split("/")[-1]
            item = self.api.item.get(
                entity_id=wikidata_id,
                mediawiki_api_url='https://www.wikidata.org/w/api.php'
                )
            if 'P1556' in item.claims.get_json().keys():
                print(f"Potential wikidata match for author {self.name}")
                id_claims = item.claims.get('P1556')
                if id_claims:
                    for claim in id_claims:
                        claim = claim.get_json()
                        if claim['mainsnak']['datatype'] == "external-id":
                            if claim['mainsnak']['datavalue']['value'] == self.zbmath_author_id:
                                if self.QID:
                                    imported_id = self.api.query('local_id', item.id)
                                    if imported_id and imported_id != self.QID:
                                        test_redirection = self.api.item.get(self.QID)
                                        test_id = test_redirection.id
                                        if test_id == self.QID:
                                            self.api.overwrite_entity(wikidata_id, imported_id)
                                            merge_items(self.QID, imported_id, login=self.api.login, is_bot=True)
                                    else:
                                        return self.api.overwrite_entity(wikidata_id, self.QID)
                                else:
                                    try:
                                        imported = self.api.import_entities(wikidata_id)
                                        return(imported)
                                    except:
                                        return(None)

    def update(self):
        #author does not have an update function, 
        #because it has no attributes that could be updated
        pass
    