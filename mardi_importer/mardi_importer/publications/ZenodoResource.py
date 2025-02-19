from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
from mardi_importer.publications.Author import Author
from mardi_importer.zenodo.Community import Community
from mardi_importer.zenodo.Project import Project

import logging
import urllib.request, json, re
from dataclasses import dataclass, field
from typing import Dict, List

log = logging.getLogger('CRANlogger')
CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});') # used to parse out html tags

@dataclass
class ZenodoResource():
    api: MardiIntegrator
    zenodo_id: str
    title: str = None
    _description: str = None
    _publication_date: str = None
    _authors: List[Author] = field(default_factory=list)
    _resource_type: str = None
    _mardi_type: str = None
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
    def description(self):
        desc_long = ""
        if "description" in self.metadata.keys():
            desc_long = self.metadata["description"]
            desc_long = re.sub(CLEANR, '', desc_long) # parse out html tags from the description
            desc_long = re.sub(r'\n|\\N|\t|\\T', ' ', desc_long) # parse out tabs and new lines
            desc_long = re.sub(r'^\s+|\s+$', '', desc_long) # parse out leading and trailing white space
        if re.match("\w+", desc_long):
            self._description = desc_long
        return self._description


    @property
    def publication_date(self):
        if not self._publication_date:
            if re.match("\d{4}-\d{2}-\d{2}",self.metadata['publication_date']):
                publication_date = f"{self.metadata['publication_date']}T00:00:00Z"
                self._publication_date = publication_date
        return self._publication_date
    
    @property
    def license(self):
        if not self._license and ('license' in self.metadata.keys()):
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
                self._mardi_type = "MaRDI dataset profile"
            elif resource_type == "Software":
                self._resource_type = "wd:Q7397"
                self._mardi_type = "MaRDI software profile"
            elif resource_type == "Presentation":
                self._resource_type = "wd:Q604733"
            elif resource_type == "Report":
                self._resource_type = "wd:Q10870555"
                self._mardi_type = "MaRDI publication profile"
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
                self._mardi_type = "MaRDI publication profile"
            else:
                # Other -> Information resource
                self._resource_type = "wd:Q37866906"
        return self._resource_type

    @property
    def communities(self):
        if not self._communities and "communities" in self.metadata.keys():
            #if "communities" in self.metadata.keys():
                for communityCur in self.metadata["communities"]:
                    community_id = communityCur.get("id")
                    if community_id == "mathplus":
                        community = Community(api = self.api, community_id = community_id)
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
        if not self._projects and community and self.metadata.get("related_identifiers"):
            for related_ids in self.metadata.get("related_identifiers"):
                #print("identifier: " + related_ids["identifier"])
                if related_ids["identifier"] in Project.get_project_ids():
                    project = Project(api = self.api, community = community, project_id = related_ids["identifier"])
                    self._projects.append(project)
        return self._projects

    def exists(self):        
        if self.QID:
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
   

    def create(self, update = False):

        if not update:
            if self.QID:
                return self.QID

            item = self.api.item.new()

        else:
            item = self.api.item.get(entity_id=self.QID)
    
        
        if self.title:
            item.labels.set(language="en", value=self.title)

        if self.resource_type and self.resource_type != "wd:Q37866906":
                desc = f"{self.metadata['resource_type']['title']} published at Zenodo repository. "
                item.add_claim('wdt:P31',self.resource_type)
        else:
            desc = "Resource published at Zenodo repository. "
        item.descriptions.set(language="en", value=desc)


        if self.description:
            prop_nr = self.api.get_local_id_by_label("description", "property")
            item.add_claim(prop_nr, self.description)

   
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
            item.add_claim('wdt:P356', doi)

        # License
        if self.license:
            if self.license['id'] == "cc-by-4.0":
                item.add_claim("wdt:P275", "wd:Q20007257")
            elif self.license['id'] == "cc-by-sa-4.0":
                item.add_claim("wdt:P275", "wd:Q18199165")
            elif self.license['id'] == "cc-by-nc-sa-4.0":
                item.add_claim("wdt:P275", "wd:Q42553662")
            elif self.license['id'] == "mit-license":
                item.add_claim("wdt:P275", "wd:Q334661")

        # Communities
        if self.communities:
            for community in self.communities:
                prop_nr = self.api.get_local_id_by_label("community", "property")
                item.add_claim(prop_nr, community.QID)

        # Projects
        if self.projects:
            for project in self.projects:
                project.create()
                prop_nr = self.api.get_local_id_by_label("Internal Project ID", "property")
                item.add_claim(prop_nr, project.QID)

        if self._mardi_type:
            item.add_claim('MaRDI profile type', self._mardi_type)


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
        
