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
    _item: MardiItemEntity = None

    def __post_init__(self):


        #self._item = self.api.item.new()
        #self._item.labels.set(language="en", value=self.community_title)
        

        # TODO: eventually get this stuff from the zenodo API
        zenodo_community_id = "wdt:P9934"
        QID_results = self.api.search_entity_by_value(zenodo_community_id, self.community_id)
        if QID_results: self.QID = QID_results[0]
        
        if self.community_id == "mathplus":
            self.community_title = "MATH+"
            self.community_str = "The Berlin Mathematics Research Center MATH+ is a cross-institutional and interdisciplinary Cluster of Excellence."

        
    # @property
    # def QID(self) -> str:
    #     if self._QID:
    #         return self._QID

    #     if not self.orcid:
    #         return None
        
    #     results = self.api.search_entity_by_value('wdt:P496', self.orcid)
    #     if results:
    #         self._QID = results[0]
    #         return self._QID
    def exists(self):
        
        if self.QID:
            return self.QID

        #prop_nr = self.api.get_local_id_by_label("Zenodo community", "property")
        QID_results = self.api.search_entity_by_value("wdt:P9934", str(self.community_id))

        if QID_results: 
            self.QID = QID_results[0]

        if self.QID:
            print(f"Community exists with QID {self.QID}")
        return self.QID


    def create(self):

        if not self.exists():
            if self.QID:
                return self.QID
        
            self._item = self.api.item.new()
        else:
            self._item = self.api.item.get(entity_id=self.QID)

        self._item.labels.set(language="en", value=self.community_title)
        self._item.descriptions.set(language="en", value = self.community_str)

        # instance of = community
        self._item.add_claim("wdt:P31", "wd:Q177634")

        # Add zenodo community ID
        if self.community_id:
            self._item.add_claim("wdt:P9934", self.community_id)

        # mardi profile type: mardi community profile
        prop_nr = self.api.get_local_id_by_label("MaRDI profile type", "property")
        item_nr = self.api.get_local_id_by_label("MaRDI community profile", "item")[0]
        self._item.add_claim(prop_nr, item_nr)

        if self.url:
            self._item.add_claim("wdt:P973", self.url)

        if self.description:
            self._item.add_claim("wdt:Q1200750", self.description)

        # if self.QID:
        #     self._item = self.api.item.get(self.QID)

        self.QID = self._item.write().id

        if self.QID:
            print(f"Zenodo community with community id: {self.community_id} created with ID {self.QID}.")
            return self.QID
        else:
            print(f"Zenodo community with community id: {self.community_id} could not be created.")
            return None
        
        # add community str:
        # for now manual, api still not released (https://developers.zenodo.org/#rest-api)


    


