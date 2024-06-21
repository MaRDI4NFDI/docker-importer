from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
from mardi_importer.publications.Author import Author
from mardi_importer.zenodo.Community import Community
from mardi_importer.zenodo.Project import Project
from wikibaseintegrator.wbi_enums import ActionIfExists

import logging
import urllib.request, json, re
from dataclasses import dataclass, field
from typing import Dict, List

log = logging.getLogger('CRANlogger')

@dataclass
class ZenodoResource():
    api: MardiIntegrator
    zenodo_id: str
    title: str = None
    _publication_date: str = None
    _authors: List[Author] = field(default_factory=list)
    _resource_type: str = None
    _license: str = None
    _communities: List[Community] = field(default_factory=list)
    _projects: List[Project] = field(default_factory = list)
    metadata: Dict[str, object] = field(default_factory=dict)
    QID: str = None

    def __post_init__(self):
        with urllib.request.urlopen(f"https://zenodo.org/api/records/{self.zenodo_id}") as url:
            json_data = json.load(url)
            self.metadata = json_data['metadata']
        if self.metadata:
            self.title = self.metadata['title']

        zenodo_id = 'wdt:P4901'

        QID_results = self.api.search_entity_by_value(zenodo_id, self.zenodo_id)
        if QID_results: self.QID = QID_results[0]

        if self.QID:
            # Get authors.
            item = self.api.item.get(self.QID)
            author_QID = item.get_value('wdt:P50')
            for QID in author_QID:
                author_item = self.api.item.get(entity_id=QID)
                name = str(author_item.labels.get('en'))
                orcid = author_item.get_value('wdt:P496')
                orcid = orcid[0] if orcid else None
                aliases = []
                if author_item.aliases.get('en'):
                    for alias in author_item.aliases.get('en'):
                        aliases.append(str(alias))
                author = Author(self.api, 
                                name=name,
                                orcid=orcid,
                                _aliases=aliases,
                                _QID=QID)
                self._authors.append(author)
            return self.QID

    @property
    def publication_date(self):
        if not self._publication_date:
            if re.match("\d{4}-\d{2}-\d{2}",self.metadata['publication_date']):
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
                affiliation = creator.get('affiliation')
                author = Author(self.api, name=name, orcid=orcid, affiliation=affiliation)
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

    @property
    def communities(self):
        if not self._communities:
            for communityCur in self.metadata["communities"]:
                community_id = communityCur.get("id")
                if not community_id == "mathplus":
                    next
                community = Community(api = self.api, community_id = community_id)
                community_qid = community.create()
                self._communities.append(community)
        return self._communities
    
    @property
    def projects(self):
        community = None
        if self._communities:
            for communityCur in self._communities:
                if communityCur.community_id == "mathplus":
                    community = communityCur
                    break
        if (not self._projects) and community and self.metadata.get("related_identifiers"):
            #print(Project.get_project_ids())
            for related_ids in self.metadata.get("related_identifiers"):
                print("identifier: " + related_ids["identifier"])
                if related_ids["identifier"] in Project.get_project_ids():
                    project = Project(api = self.api, community = community, project_id = related_ids["identifier"])
                    #if project.exis
                    projet_QID = project.create()
                    self._projects.append(project)
        return self._projects

    def exists(self):
        
        if self.QID:
            return self.QID

        QID_results = self.api.search_entity_by_value("wdt:P4901", str(self.zenodo_id))
        
        if QID_results: 
            self.QID = QID_results[0]

        if self.QID:
            print(f"Zenodo item exists with QID {self.QID}")
        return self.QID

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

    def update2(self):

        self.item = self.api.item.get(entity_id=self.QID)

        self.insert_claims()
        self.item.write()

        if self.QID:
            print(f"zenodo item with ID {self.QID} has been updated.")
            return self.QID
        else:
            print(f"zenodo item could not be updated.")
            return None      

    def create(self, update = False):

        if not update:
            if self.QID:
                return self.QID

            item = self.api.item.new()
        else:
            item = self.api.item.get(entity_id=self.QID)
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
        #item.add_claim('wdt:P31',self.resource_type)  
   
        # Publication date
        if self.publication_date:
            item.add_claim('wdt:P577', self.publication_date)
        
        # Authors
        author_QID = self.__preprocess_authors()
        claims = []
        for author in author_QID:
            claims.append(self.api.get_claim("wdt:P50", author))
        item.add_claims(claims)

        # Zenodo ID & DOI
        if self.zenodo_id:
            item.add_claim('wdt:P4901', self.zenodo_id)
            doi = f"10.5281/zenodo.{self.zenodo_id}"
            #item.add_claim("doi", doi)
            item.add_claim('wdt:P356', doi)

        #License
        if self.license['id'] == "cc-by-4.0":
            item.add_claim("wdt:P275", "wd:Q20007257")
        elif self.license['id'] == "cc-by-sa-4.0":
            item.add_claim("wdt:P275", "wd:Q18199165")
        elif self.license['id'] == "cc-by-nc-sa-4.0":
            item.add_claim("wdt:P275", "wd:Q42553662")
        elif self.license['id'] == "mit-license":
            item.add_claim("wdt:P275", "wd:Q334661")

        #communities
        if self.communities:
            for community in self._communities:
                prop_nr = self.api.get_local_id_by_label("community", "property")
                community_nr = self.api.get_local_id_by_label(community.community_id, "item")
                item.add_claim(prop_nr, community.QID)

        #projects
        if self.projects:
            for project in self._projects:
                prop_nr = self.api.get_local_id_by_label("Internal Project ID", "property")
                #project_qid = self.api.get_local_id_by_label(community.community_id, "item")
                item.add_claim(prop_nr, project.QID)
        

        #print(item.claims.get_json())

        self.QID = item.write().id

        if self.QID:
            log.info(f"Zenodo resource with Zenodo id: {self.zenodo_id} created with ID {self.QID}.")
            return self.QID
        else:
            log.info(f"Zenodo resource with Zenodo id: {self.zenodo_id} could not be created.")
            return None

    def __preprocess_authors(self) -> List[str]:
        """Processes the author information of each publication.

        Create the author if it does not exist already as an 
        entity in wikibase.
            
        Returns:
          List[str]: 
            QIDs corresponding to each author.
        """
        author_QID = []
        for author in self.authors:
            if not author.QID:
                author.create()
            author_QID.append(author.QID)
        return author_QID
        
