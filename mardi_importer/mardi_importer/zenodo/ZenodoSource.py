import requests
import json
import os
import sys
import pandas as pd

from mardi_importer.base import ADataSource
from mardi_importer.publications import ZenodoResource
from typing import List

class ZenodoSource(ADataSource):
    """Reads data from Zenodo API."""

    def __init__(
        self,
        user: str,
        password: str,
        communities: List[str] = None,
        resourceTypes: List[str] = None,
        orcid_id_file: str = None,
        customQ: str = None

    ):    
        super().__init__(user, password)
        self.zenodo_ids = []    
        self.filepath = os.path.realpath(os.path.dirname(__file__))

        self.communities = communities
        self.resourceTypes = resourceTypes
        self.orcid_id_file = orcid_id_file
        self.customQ = customQ
        self.orcid_ids = None

        if self.orcid_id_file:
            self.orcid_ids = self.parse_orcids(orcid_id_file)

        # if all parameters are set to None, issue a warning


    def setup(self):
        """Create all necessary properties and entities for Zenodo
        """
        # Import entities from Wikidata
        self.import_wikidata_entities("/wikidata_entities.txt")

        # Create new required local entities
        self.create_local_entities("/new_entities.json")

    @staticmethod
    def parse_orcids(file):
        
        # check that path and file exists
        if not os.path.isfile(file):
            sys.exit("File" + file + "not found")

        orcid_df = pd.read_csv(file)
        if not "orcid" in orcid_df.columns:
            sys.exit("The file containing ORCID IDs must contain a column 'orcid'.")
        
        orcid_df.drop_duplicates()
        orcids_all = orcid_df['orcid'].tolist()
        return orcids_all

    def pull(self):
        """
        This method queries the Zenodo API to get a data dump of all records.
        """

        total_hits = 0
        q_list = []

        if self.communities:
            community_str = "communities:(" + ' '.join(self.communities) + ")"
            q_list.append(community_str)
        if self.resourceTypes:
            resources_str = "resource_type.type:(" + ' OR '.join(self.resourceTypes) + ")"
            q_list.append(resources_str)
        if self.customQ:
            q_list.append(self.customQ)

        q_str = ' AND '.join(q_list)
        
        if self.orcid_ids: 
            i=0
            while i <= len(self.orcid_ids): # if there are too many orcids the initial request needs to be sent out in batches

                orcid_str ='metadata.creators.\*:("' + '" "'.join(self.orcid_ids[i:i+50]) + '")'
                print("retrieving zenodo entries for the following ORCID IDs: " + orcid_str)

                response = requests.get('https://zenodo.org/api/records',
                                        params={'q' : q_str + ' AND ' + orcid_str, 
                                        'sort':'-mostrecent'})
                response_json = response.json()
                total_hits = response_json.get("hits").get("total")

                page_cur = 1
                while total_hits > 0:
                    response = requests.get('https://zenodo.org/api/records',
                                            params={'q' :  q_str + ' AND ' + orcid_str, 
                                            'sort':'-mostrecent',
                                            'size' : 50,
                                            'page' : page_cur})
                    response_json = response.json()
                    total_hits = total_hits - len(response_json.get("hits").get("hits"))
                    page_cur = page_cur + 1

                    for entry in response_json.get("hits").get("hits"):
                        self.zenodo_ids.append(str(entry.get("id")))

                i = i+50
        else:
            response = requests.get('https://zenodo.org/api/records',
                                    params={'q' : q_str})   
            response_json = response.json()
            total_hits = response_json.get("hits").get("total")     

            page_cur = 1
            while total_hits > 0:
                response = requests.get('https://zenodo.org/api/records',
                                    params={'q' : q_str,
                                            'sort':'-mostrecent',
                                            'size' : 50,
                                            'page' : page_cur})
                response_json = response.json()
                total_hits = total_hits - len(response_json.get("hits").get("hits"))
                page_cur = page_cur + 1

                for entry in response_json.get("hits").get("hits"):
                    self.zenodo_ids.append(str(entry.get("id")))

       

        # response = requests.get('https://zenodo.org/api/records',
        #                         params={'size' : 1,
        #                         'communities' : 'mathplus'})
        # response_json = response.json()
        # total_hits = response_json.get("hits").get("total")

        # for page in range(1, total_hits+1):
        #     url = 'https://zenodo.org/api/records?communities=mathplus&page=' + str(page) + "&size=1&sort=newest"
        #     response = requests.get(url)    
        #     response_json = response.json()

        #     zenodo_id = response_json.get("hits").get("hits")[0].get("id")
        #     self.zenodo_ids.append(str(zenodo_id))

    def push(self):
        for id in self.zenodo_ids:   
            entry = ZenodoResource.ZenodoResource(
                self.api,
                self.wdi,
                zenodo_id = id
            )

            if not entry.exists():
                print (f"Creating entry for zenodo id {id}")
                entry.create(update = False)
            else:
                print (f"Entry for zenodo id: {id} already exists. Updating.")
                entry.create(update = True)

