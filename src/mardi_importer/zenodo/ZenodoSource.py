import requests
import os
import sys
import time
import logging
import pandas as pd

from mardi_importer.base import ADataSource
from .ZenodoResource import ZenodoResource
from typing import List

log = logging.getLogger(__name__)

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

        if self.communities is None:
            self.communities = ['mathplus', 'mardigmci', 'mardi']
        # if self.orcid_id_file is None:
        #     current_dir = os.path.dirname(os.path.abspath(__file__))
        #     self.orcid_id_file = os.path.join(current_dir, 'orcids-all.csv')

        # self.orcid_ids = self.parse_orcids(self.orcid_id_file)

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

    @staticmethod
    def get_with_retries(url, params, retries=5, base_wait=3):
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=10)

                if response.status_code == 429:
                    wait_time = base_wait * (2 ** attempt)
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError:
                if 400 <= response.status_code < 500:
                    raise

            except (requests.exceptions.RequestException, ValueError):
                if attempt == retries - 1:
                    raise
                wait_time = base_wait * (2 ** attempt)
                time.sleep(wait_time)



    def pull(self, since: str | None = None):
        """
        This method queries the Zenodo API to get a data dump of all records.

        Args:
            since: Only records updated on Zenodo on or after this day (YYYY-MM-DD).
        """

        total_hits = 0
        q_list = []
        if since:
            q_list.append(f"updated:[{since} TO *]")

        if self.communities:
            community_str = "communities:(" + ' '.join(self.communities) + ")"
            q_list.append(community_str)
        if self.resourceTypes:
            resources_str = "resource_type.type:(" + ' OR '.join(self.resourceTypes) + ")"
            q_list.append(resources_str)
        if self.customQ:
            q_list.append(self.customQ)

        q_str = ' AND '.join(q_list)
        print(q_str)

        if self.orcid_ids: 
            i = 0
            while i < len(self.orcid_ids):  # batch ORCIDs

                orcid_str = 'metadata.creators.\*:("' + '" "'.join(self.orcid_ids[i:i+20]) + '")'
                print("retrieving zenodo entries for the following ORCID IDs: " + orcid_str)

                response_json = self.get_with_retries(
                    'https://zenodo.org/api/records',
                    params={'q': q_str + ' AND ' + orcid_str,
                            'sort': '-mostrecent'}
                )

                total_hits = response_json.get("hits").get("total")

                page_cur = 1
                while total_hits > 0:
                    response_json = self.get_with_retries(
                        'https://zenodo.org/api/records',
                        params={'q': q_str + ' AND ' + orcid_str,
                                'sort': '-mostrecent',
                                'size': 20,
                                'page': page_cur}
                    )

                    hits = response_json.get("hits").get("hits")
                    total_hits -= len(hits)
                    page_cur += 1

                    for entry in hits:
                        self.zenodo_ids.append(str(entry.get("id")))

                i += 20
        else:
            response_json = self.get_with_retries(
                'https://zenodo.org/api/records',
                params={'q': q_str}
            )

            total_hits = response_json.get("hits").get("total")

            page_cur = 1
            while total_hits > 0:
                response_json = self.get_with_retries(
                    'https://zenodo.org/api/records',
                    params={'q': q_str,
                            'sort': '-mostrecent',
                            'size': 20,
                            'page': page_cur}
                )

                hits = response_json.get("hits").get("hits")
                if not hits:
                    break
                total_hits -= len(hits)
                page_cur += 1

                for entry in hits:
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

    def push(self) -> List[str]:
        """Creates or updates an item for every pulled record; returns the IDs that failed."""
        failed = []
        for zenodo_id in self.zenodo_ids:
            try:
                entry = ZenodoResource(zenodo_id)

                if not entry.exists():
                    print (f"Creating entry for zenodo id {zenodo_id}")
                    entry.create(update = False)
                else:
                    print (f"Entry for zenodo id: {zenodo_id} already exists. Updating.")
                    entry.create(update = True)
            except Exception as exc:
                log.error("Zenodo record %s not imported: %s", zenodo_id, exc, exc_info=True)
                failed.append(zenodo_id)
        return failed

    def new_resource(self, zenodo_id: str) -> 'ZenodoResource':
        return ZenodoResource(zenodo_id)
