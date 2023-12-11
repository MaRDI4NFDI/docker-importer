from .Author import Author
from wikibaseintegrator.wbi_enums import ActionIfExists

import urllib.request, json, logging

log = logging.getLogger('CRANlogger')

class authorZenodo():
    def __init__(self, name, orcid = None, affiliation = None):
        self.name = self.__preprocess_name(name)
        self.orcid = orcid
        self.affiliation = affiliation

    def __preprocess_name(self, name):
        if ',' in name:
            words = name.split(', ')
            if len(words) < 2:
                words = name.split(',')
            return f"{words[1]} {words[0]}"
        return name

class ZenodoResource():
    def __init__(self, integrator, zenodo_id, coauthors=[]):
        self.api = integrator
        self.zenodo_id = zenodo_id
        self._title = ''
        self._publication_date = ''
        self._authors = []
        self._resource_type = ''
        self._main_subject = []
        self._license = ''
        self.metadata = ''
        self.coauthors = coauthors
        self.__pull()

    def __pull(self):
        with urllib.request.urlopen(f"https://zenodo.org/api/records/{self.zenodo_id}") as url:
            json_data = json.load(url)
            self.metadata = json_data['metadata']

    @property
    def title(self):
        if not self._title:
            self._title = self.metadata['title']
        return self._title

    @property
    def publication_date(self):
        if not self._publication_date:
            publication_date = f"{self.metadata['publication_date']}T00:00:00Z"
            self._publication_date = publication_date
        return self._publication_date
    
    @property
    def license(self):
        if not self._license:
            self._license = self.metadata['license']
        return self._license            

    @property
    def authors(self):
        if not self._authors:
            for creator in self.metadata['creators']:
                name = creator.get('name')
                orcid = creator.get('orcid')              
                author = authorZenodo(name, orcid)
                self._authors.append(author)
        return self._authors

    @property
    def resource_type(self):
        if not self._resource_type:
            resource_type = self.metadata['resource_type']['title']
            if resource_type == "Dataset":
                self._resource_type = "wd:Q1172284"
            elif resource_type == "Software":
                self._resource_type = "wd:Q7397"
            elif resource_type == "Presentation":
                self._resource_type = "wd:Q604733"
            elif resource_type == "Report":
                self._resource_type = "wd:Q10870555"
            elif resource_type == "Poster":
                self._resource_type = "wd:Q429785"
            elif resource_type == "Figure":
                self._resource_type = "wd:Q478798"
            elif resource_type == "Video/Audio":
                self._resource_type = "wd:Q2431196"
            elif resource_type == "Lesson":
                self._resource_type = "wd:Q379833"
            elif resource_type == "Preprint":
                self._resource_type = "wd:Q580922"
            else:
                # Other -> Information resource
                self._resource_type = "wd:Q37866906"
        return self._resource_type

    def update(self):
        # description_prop_nr = "P727"
        zenodo_item = self.api.item.new()
        zenodo_item.labels.set(language="en", value=self.title)

        zenodo_id = zenodo_item.is_instance_of_with_property("wd:Q1172284", "wdt:P4901", self.zenodo_id)
        new_item = self.api.item.get(entity_id=zenodo_id)

        if self.license['id'] == "cc-by-4.0":
            new_item.add_claim("wdt:P275", "wd:Q20007257")
        elif self.license['id'] == "cc-by-sa-4.0":
            new_item.add_claim("wdt:P275", "wd:Q18199165")
        elif self.license['id'] == "cc-by-nc-sa-4.0":
            new_item.add_claim("wdt:P275", "wd:Q42553662")
        elif self.license['id'] == "mit-license":
            new_item.add_claim("wdt:P275", "wd:Q334661")

        return new_item.write()        

    def create(self):
        item = self.api.item.new()

        # Add title
        if self.title:
            item.labels.set(language="en", value=self.title)

        # Add description and instance information
        if self.resource_type and self.resource_type != "wd:Q37866906":
            item.descriptions.set(
                language="en", 
                value=f"{self.metadata['resource_type']['title']} published at Zenodo repository"
            )
            item.add_claim('wdt:P31',self.resource_type)
        else:
            item.descriptions.set(
                language="en", 
                value="Resource published at Zenodo repository"
            )

        # Publication date
        if self.publication_date:
            item.add_claim('wdt:P577', self.publication_date)

        # Authors
        if self.authors:
            author_claims = []
            for author in self.authors:
                author_item = Author(self.api, 
                                    author.name, 
                                    author.orcid, 
                                    self.coauthors)
                author_id = author_item.create()

                if author.affiliation:
                    update_item = self.api.item.get(entity_id=author_id)
                    affiliation_wd = update_item.get_value('wdt:P108')
                    if author.affiliation not in affiliation_wd:
                        claim = self.api.get_claim('wdt:P108', author.affiliation)
                        update_item.claims.add(
                            claim,
                            ActionIfExists.APPEND_OR_REPLACE,
                        )
                        update_item.write()
                
                claim = self.api.get_claim('wdt:P50', author_id)
                author_claims.append(claim)
            item.add_claims(author_claims)

        # Zenodo ID & DOI
        if self.zenodo_id:
            item.add_claim('wdt:P4901', self.zenodo_id)
            doi = f"10.5281/zenodo.{self.zenodo_id}"
            item.add_claim('wdt:P356', doi)

        # Check that the item does not exist already
        QID = None
        if self.resource_type != "Other":
            QID = item.is_instance_of_with_property(self.resource_type, 'wdt:P4901', self.zenodo_id)

        if QID:
            log.info(f"Zenodo resource with Zenodo id: {self.zenodo_id} already exists with QID: {QID}.")
            return QID
        else: 
            resource_ID = item.write().id
            if resource_ID:
                log.info(f"Zenodo resource with Zenodo id: {self.zenodo_id} created with ID {resource_ID}.")
                return resource_ID
            else:
                log.info(f"Zenodo resource with Zenodo id: {self.zenodo_id} could not be created.")
                return None
        
