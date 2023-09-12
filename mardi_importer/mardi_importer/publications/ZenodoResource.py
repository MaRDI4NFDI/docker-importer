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
    def authors(self):
        if not self._authors:
            for creator in self.metadata['creators']:
                name = creator.get('name')
                orcid = creator.get('orcid')
                affiliation_str = creator.get('affiliation')
                affiliation = None
                if affiliation and len(affiliation_str) > 8:
                    affiliation = self.api.import_from_label(affiliation_str)                    
                author = authorZenodo(name, orcid, affiliation)
                self._authors.append(author)
        return self._authors

    @property
    def main_subject(self):
        if not self._main_subject:
            if 'keywords' in self.metadata.keys():
                keywords_raw = self.metadata['keywords']
                keywords = []
                short_keywords = []
                long_keywords = []
                alias = {}

                for keyword in keywords_raw:
                    if ';' in keyword:
                        keywords += keyword.split(';')
                    if ',' in keyword:
                        keywords += keyword.split(',')
                    else:
                        keywords.append(keyword)

                for keyword in keywords:
                    if len(keyword) <= 5:
                        short_keywords.append(keyword)
                    else:
                        long_keywords.append(keyword)

                for keyword in long_keywords:
                    result_id = self.api.import_from_label(keyword)
                    if result_id:
                        alias[result_id] = []
                        self._main_subject.append(result_id)
                        alias_item = self.api.item.get(entity_id=result_id)
                        if 'en' in alias_item.aliases.aliases.keys():
                            for alias_str in alias_item.aliases.aliases['en']:
                                alias[result_id].append(alias_str.value)
                
                for keyword in short_keywords:
                    similar_alias = False
                    for entity_id in alias.keys():
                        if keyword in alias[entity_id]:
                            similar_alias = True
                    if not similar_alias:
                        result_id = self.api.import_from_label(keyword)
                        if result_id:
                            self._main_subject.append(result_id)

                
        return self._main_subject

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

        # Add subjects
        if self.main_subject:
            subject_claims = []
            for subject in self.main_subject:
                claim = self.api.get_claim('wdt:P921', subject)
                subject_claims.append(claim)
            item.add_claims(subject_claims)

        # Publication date
        if self.publication_date:
            item.add_claim('wdt:P577', time=self.publication_date)

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
        
