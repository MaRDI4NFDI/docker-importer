from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, merge_items

class Author:
    def __init__(self, integrator, name, orcid=None, coauthors=None):
        self.api = integrator
        self.name = name
        self.orcid = orcid
        self.coauthors = coauthors
        self.QID = ""        
        self.item = self.init_item()
                 
    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        human = "wd:Q5"
        item.add_claim("wdt:P31", human)

        if self.coauthors:
            QID = self.check_coauthors()
            if QID:
                self.QID = QID
                if self.orcid:                
                    self.sync_with_wikidata()
            else:
                if self.orcid:
                    self.QID = item.is_instance_of_with_property(human, 'ORCID iD', self.orcid)
                    QID = self.sync_with_wikidata()
                    if QID:
                        self.QID = QID
                    else:
                        item.add_claim('ORCID iD', self.orcid)
                        return item
                else:
                    return item
        else:
            if self.orcid:
                self.QID = item.is_instance_of_with_property(human, 'ORCID iD', self.orcid)
                QID = self.sync_with_wikidata()
                if QID:
                    self.QID = QID
                else:
                    item.add_claim('ORCID iD', self.orcid)
                    return item
            else:
                return item

    def create(self):
        if self.QID: 
            return self.QID
        return self.item.write().id

    def check_coauthors(self):
        for coauthor_id in self.coauthors:
            coauthor = self.api.item.get(entity_id=coauthor_id)
            coauthor_name = coauthor.labels.values['en'].value
            coauthor_orcid = coauthor.get_value('ORCID iD')
            coauthor_orcid = coauthor_orcid[0] if coauthor_orcid else None

            corrected_name = Author(self.api, coauthor_name).compare_names(self.name)
            if corrected_name:
                if not coauthor_orcid and self.orcid:
                    coauthor.labels.set(language="en", value=corrected_name)
                    coauthor.add_claim('ORCID iD', self.orcid)
                    coauthor.write()
                return coauthor_id
            if self.orcid and coauthor_orcid == self.orcid:
                return coauthor_id
        return None

    def compare_names(self, author):
        if self.name == author:
            return self.name
        author_1_vec = self.name.split()
        author_2_vec = author.split()
        if author_1_vec[0].lower() == author_2_vec[0].lower():
            if len(author_1_vec) > 2 and len(author_2_vec) > 1:
                if author_1_vec[2].lower() == author_2_vec[1].lower():
                    return self.name
            if len(author_2_vec) == 1:
                return self.name
            if len(author_2_vec) > 2 and len(author_1_vec) > 1:
                if author_1_vec[1].lower() == author_2_vec[2].lower():
                    return author
            if len(author_1_vec) > 2 and len(author_2_vec) > 2:
                if author_1_vec[0].lower() == author_2_vec[0].lower() and \
                author_1_vec[2].lower() == author_2_vec[2].lower() and \
                author_1_vec[1][0] == author_2_vec[1][0] and \
                len(author_1_vec[1]) <= 2 and \
                len(author_2_vec[1]) <= 2:
                    return f"{author_1_vec[0]} {author_1_vec[1][0]}. {author_1_vec[2]}"
        return None

    def sync_with_wikidata(self, name=None):

        if name: 
            search_string = name
        else:
            search_string = self.name

        results = search_entities(
            search_string=search_string,
            mediawiki_api_url='https://www.wikidata.org/w/api.php'
            )   

        for result in results:
            item = self.api.item.get(
                entity_id=result,
                mediawiki_api_url='https://www.wikidata.org/w/api.php'
                )

            if 'P496' in item.claims.get_json().keys():
                orcid_claims = item.claims.get('P496')
                if orcid_claims:
                    for claim in orcid_claims:
                        claim = claim.get_json()
                        if claim['mainsnak']['datatype'] == "external-id":
                            if claim['mainsnak']['datavalue']['value'] == self.orcid:
                                if self.QID:
                                    imported_id = self.api.query('local_id', item.id)
                                    if imported_id and imported_id != self.QID:
                                        test_redirection = self.api.item.get(self.QID)
                                        test_id = test_redirection.id
                                        if test_id == self.QID:
                                            self.api.overwrite_entity(result, imported_id)
                                            merge_items(self.QID, imported_id, login=self.api.login, is_bot=True)
                                    else:
                                        return self.api.overwrite_entity(result, self.QID)
                                else:
                                    return self.api.import_entities(result)
        if not name:
            words = (self.name).split(" ")
            if len(words) == 3 and len(words[1]) <= 2:
                shortened_name = " ".join([words[0], words[2]])
                return self.sync_with_wikidata(shortened_name)

