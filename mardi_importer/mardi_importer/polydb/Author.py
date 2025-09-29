from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, merge_items

from nameparser import HumanName

class Author:
    def __init__(self, api, name="", orcid=None, arxiv_id=None, affiliation=None, aliases=[], QID=None):
        self.api = api
        self.name = self.parse_name(name)
        self.orcid = orcid
        self.arxiv_id = arxiv_id
        self.affiliation = affiliation
        self._aliases = aliases
        self._QID = QID
        self._item = None

    @staticmethod
    def parse_name(name: str) -> str:
        parsed_name = HumanName(name)
        parsed_name.capitalize(force=True)
        return str(parsed_name)

    def __eq__(self, other):
        if self.orcid and self.orcid == other.orcid:
            return True
        self_name = HumanName(self.name)
        other_name = HumanName(other.name)
        if self_name.first == other_name.first and self_name.last == other_name.last:
            return True
        if ( self_name.first.lower().replace('-','') == other_name.first.lower().replace('-','') and 
             self_name.last.lower().replace('-','') == other_name.last.lower().replace('-','') ):
            return True
        if ( len(self_name.first) == 2 and self_name.first[0] == other_name.first[0] 
             and self_name.last == other_name.last ):
            return True
        if ( len(other_name.first) == 2 and self_name.first[0] == other_name.first[0] 
             and self_name.last == other_name.last ):
            return True
        if ( self_name.first == other_name.first and len(self_name.last) == 2 
             and self_name.last[0] == other_name.last[0] ):
            return True
        if ( self_name.first == other_name.first and len(other_name.last) == 2 
             and self_name.last[0] == other_name.last[0] ):
            return True
        return False

    def __add__(self, other):
        self_name = HumanName(self.name)
        other_name = HumanName(other.name)

        name_attributes = ['first', 'middle', 'last']
        name_values = [
            getattr(self_name, attr)
            if len(getattr(self_name, attr)) >= len(getattr(other_name, attr))
            else getattr(other_name, attr)
            for attr in name_attributes
        ]
        long_name = " ".join([s for s in name_values if s])

        aliases = []
        if self.name != long_name:
            aliases.append(self.name)
        if other.name != long_name:
            aliases.append(other.name)
        aliases += [x for x in self.aliases if x != long_name]
        aliases += [x for x in other.aliases if (x != long_name and x not in aliases)]
        
        orcid = self.orcid if self.orcid else other.orcid
        arxiv_id = self.arxiv_id if self.arxiv_id else other.arxiv_id
        if self.affiliation and self.affiliation.startswith('wd:'):
            affiliation = self.affiliation
        elif other.affiliation and other.affiliation.startswith('wd:'):
            affiliation = other.affiliation
        else:
            affiliation = self.affiliation if self.affiliation else other.affiliation
        QID = self.QID if self.QID else other.QID

        if QID:
            item = self.api.item.get(entity_id=QID)
            item.labels.set(language="en", value=long_name)
            item.aliases.set(language="en", values=aliases)
            item.write()

        return Author(self.api, 
                      name=long_name, 
                      orcid=orcid, 
                      arxiv_id=arxiv_id,
                      affiliation=affiliation,
                      aliases=aliases,
                      QID=QID)

    def __repr__(self):
        rep = f'Author: {self.name}, ORCID: {self.orcid}, arXiv: {self.arxiv_id}, QID: {self.QID}, {self.aliases}'
        return rep
        
    @property
    def aliases(self):
        if self._aliases:
            return self._aliases
        
        aliases = []
        if self.QID:
            item = self.api.item.get(entity_id=self.QID)
            if item.aliases.get('en'):
                for alias in item.aliases.get('en'):
                    aliases.append(str(alias))
                self._aliases = aliases
        return aliases

    @property
    def QID(self) -> str:
        if self._QID:
            return self._QID

        if not self.orcid:
            return None
        
        results = self.api.search_entity_by_value('wdt:P496', self.orcid)
        if results:
            self._QID = results[0]
            return self._QID

    @QID.setter
    def QID(self, value):
        self._QID = value

    @classmethod
    def disambiguate_authors(cls, authors):
        disambiguated_authors = [authors[0]]
        for author in authors:
            if author not in disambiguated_authors:
                disambiguated_authors.append(author)
            else:
                index = disambiguated_authors.index(author)
                disambiguated_authors[index] += author
        for author in disambiguated_authors:
            if not author.QID:
                author.create()
            else:
                author.update()
        return disambiguated_authors

    def pull_QID(self, author_pool):
        if self in author_pool:
            index = author_pool.index(self)
            self._QID = author_pool[index].QID

    def create(self):
        if self.QID: 
            return self.QID
        
        self._item = self.api.item.new()
        self._item.labels.set(language="en", value=self.name)
        self._item.aliases.set(language="en", values=self.aliases)

        # Instance of human
        human = "wd:Q5"
        self._item.add_claim("wdt:P31", human)

        # Orcid ID
        if self.orcid:
            self._item.add_claim('ORCID iD', self.orcid)
            self.sync_with_wikidata()

        if self.QID:
            self._item = self.api.item.get(self.QID)

        # ArXiv ID
        if self.arxiv_id:
            self._item.add_claim("wdt:P4594", self.arxiv_id)

        if self.affiliation:
            self.add_affiliation()

        self._QID = self._item.write().id
        return self.QID

    def update(self):
        if not self.QID: return None

        self._item = self.api.item.get(self.QID)
        if self.orcid:
            self._item.add_claim('ORCID iD', self.orcid)
        if self.arxiv_id:
            self._item.add_claim("wdt:P4594", self.arxiv_id)
        if self.affiliation:
            self.add_affiliation()
        self._item.write()

    def add_affiliation(self):
        if self.affiliation.startswith('wd:'):
            affiliation_qid = self.affiliation.split(':')[1]
            local_qid = self.api.query('local_id', affiliation_qid)
            if not local_qid:
                local_qid = self.api.import_entities(affiliation_qid)
            self._item.add_claim("wdt:P108", local_qid)

    def sync_with_wikidata(self, name=None):

        def get_orcid(item) -> str:
            """Inner function to extract ORCID iD value
            """
            orcid_claims = []
            if 'P496' in item.claims.get_json().keys():
                orcid_claims = item.claims.get('P496')

            for claim in orcid_claims:
                claim = claim.get_json()
                if claim['mainsnak']['datatype'] == "external-id":
                    return claim['mainsnak']['datavalue']['value']

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

            if get_orcid(item) == self.orcid:
                if self.QID:
                    local_id = self.api.query('local_id', item.id)
                    if local_id and local_id != self.QID:
                        self.api.overwrite_entity(result, local_id)
                        merge_items(self.QID, local_id, login=self.api.login, is_bot=True)
                        self._QID = local_id
                    else:
                        self._QID = self.api.overwrite_entity(result, self.QID)
                else:
                    self._QID = self.api.import_entities(result)
                return self.QID

        if not name:
            words = (self.name).split(" ")
            if len(words) == 3 and len(words[1]) <= 2:
                shortened_name = " ".join([words[0], words[2]])
                if self.sync_with_wikidata(shortened_name):
                    item = self.api.item.get(entity_id=self.QID)
                    item.aliases.set(language='en', values=self.name)
                    item.write()
                    return self.QID