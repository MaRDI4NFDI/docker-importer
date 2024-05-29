from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, merge_items
from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
from mardi_importer.integrator.MardiEntities import MardiItemEntity

from dataclasses import dataclass, field
from typing import List

@dataclass
class Community:
    api: MardiIntegrator
    community_id : str
    community_title: str = None
    community_str : str = None
    description : str = None
    url : str = None
    QID: str = None

    def __post_init__(self):


        item = self.api.item.new()
        item.labels.set(language="en", value=self.community_title)
        

        # TODO: eventually get this stuff from the zenodo API
        zenodo_community_id = "wdt:P9934"
        QID_results = self.api.search_entity_by_value(zenodo_community_id, self.community_id)
        if QID_results: self.QID = QID_results[0]
        
        if self.community_id == "mathplus":
            self.community_title = "MATH+"
            self.community_str = "The Berlin Mathematics Research Center MATH+ is a cross-institutional and interdisciplinary Cluster of Excellence."



    def create(self):

        if self.QID:
            return self.QID
        
        item = self.api.item.new()
        item.labels.set(language="en", value=self.community_title)
        item.descriptions.set(language="en", value = self.community_str)

        # instance of = community
        item.add_claim("wdt:P31", "wdt:Q177634")

        # Add zenodo community ID
        if self.community_id:
            item.add_claim("wdt:P9934", self.community_id)

        # mardi profile type: mardi community profile
        item.add_claim("wd:P1460", "wd:Q6205095")

        if self.url:
            item.add_claim("wdt:P973", self.url)

        if self.description:
            item.add_claim("wdt:Q1200750", self.description)
        
        self.QID = item.write().id
        if self.QID:
            log.info(f"Zenodo community with community id: {self.community_id} created with ID {self.QID}.")
            return self.QID
        else:
            log.info(f"Zenodo community with community id: {self.community_id} could not be created.")
            return None
        
        # add community str:
        # for now manual, api still not released (https://developers.zenodo.org/#rest-api)


    


