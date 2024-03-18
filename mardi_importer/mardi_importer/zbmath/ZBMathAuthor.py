from wikibaseintegrator.wbi_helpers import execute_sparql_query, merge_items


class ZBMathAuthor:
    """
    Class to merge zbMath author items in the local wikibase instance.

    Attributes:
        integrator:
            MardiIntegrator instance
        name:
            Author name
        zbmath_author_id:
            zbmath author id
    """

    def __init__(self, integrator, name, zbmath_author_id):
        self.api = integrator
        if name:
            name_parts = name.strip().split(",")
            self.name = ((" ").join(name_parts[1:]) + " " + name_parts[0]).strip()
        else:
            self.name = None
        self.QID = None
        self.zbmath_author_id = zbmath_author_id.strip()
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        # instance of: human
        item.add_claim("wdt:P31", "wd:Q5")
        profile_prop = self.api.get_local_id_by_label("MaRDI profile type", "property")
        profile_target = self.api.get_local_id_by_label(
            "MaRDI person profile", "property"
        )
        item.add_claim(profile_prop, profile_target)
        if self.zbmath_author_id:
            # if self.name:
            #     # is there a human with zbmath author ID = zbmath_author_id
            #     self.QID = item.is_instance_of_with_property(
            #         "wd:Q5", "wdt:P1556", self.zbmath_author_id
            #     )
            # else:
            QID_list = self.api.search_entity_by_value(
                "wdt:P1556", self.zbmath_author_id
            )
            if not QID_list:
                self.QID = None
            else:
                # should not be more than one
                self.QID = QID_list[0]
                print(f"Id for empty author found, QID {self.QID}")
            if self.QID:
                return item
            else:
                item.add_claim("wdt:P1556", self.zbmath_author_id)
        return item

    def create(self):
        if self.QID:
            print(f"Author {self.name} exists")
            return self.QID
        print(f"Creating author {self.name}")
        author_id = self.item.write().id
        return author_id

    def update(self):
        # author does not have an update function,
        # because it has no attributes that could be updated
        pass

    # def query_wikidata(self):
    #     """
    #     Function for executing a SPARQL query to query wikidata for a
    #     human that has the zbmath_author_id in question.
    #     """
    #     sparql = (f"SELECT ?item WHERE {{ ?item wdt:P31 wd:Q5 . "
    #             f"?item wdt:P1556 '{self.zbmath_author_id}' . SERVICE wikibase:label "
    #             f"{{ bd:serviceParam wikibase:language '[AUTO_LANGUAGE],en'. }} }}")
    #     query = execute_sparql_query(query = sparql, endpoint="https://query.wikidata.org/sparql?")
    #     result = query["results"]
    #     #no results
    #     if not result['bindings']:
    #         return None
    #     #more than one result: unclear, skip
    #     elif len(result['bindings']) > 1:
    #         return None
    #     else:
    #         wikidata_id = result["bindings"][0]["item"]["value"].split("/")[-1]
    #         item = self.api.item.get(
    #             entity_id=wikidata_id,
    #             mediawiki_api_url='https://www.wikidata.org/w/api.php'
    #             )
    #         if 'P1556' in item.claims.get_json().keys():
    #             print(f"Potential wikidata match for author {self.name}")
    #             id_claims = item.claims.get('P1556')
    #             if id_claims:
    #                 for claim in id_claims:
    #                     claim = claim.get_json()
    #                     if claim['mainsnak']['datatype'] == "external-id":
    #                         if claim['mainsnak']['datavalue']['value'] == self.zbmath_author_id:
    #                             #if the item already exists
    #                             if self.QID:
    #                                 #check if found wikidata item is already in local
    #                                 imported_id = self.api.query('local_id', item.id)
    #                                 #if that is another item than self
    #                                 if imported_id and imported_id != self.QID:
    #                                     test_redirection = self.api.item.get(self.QID)
    #                                     test_id = test_redirection.id
    #                                     if test_id == self.QID:
    #                                         self.api.overwrite_entity(wikidata_id, imported_id)
    #                                         merge_items(self.QID, imported_id, login=self.api.login, is_bot=True)
    #                                 #update
    #                                 else:
    #                                     return self.api.overwrite_entity(wikidata_id, self.QID)
    #                             #if it doesnt exist yet, import
    #                             else:
    #                                 try:
    #                                     imported = self.api.import_entities(wikidata_id)
    #                                     return(imported)
    #                                 except:
    #                                     return(None)
