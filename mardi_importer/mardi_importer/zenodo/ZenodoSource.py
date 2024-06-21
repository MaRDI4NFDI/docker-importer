import requests
import json
import time
import os
import re
import pickle
from datetime import datetime
from glob import glob
from pathlib import Path

from mardi_importer.importer import ADataSource
from mardi_importer.integrator import MardiIntegrator
from mardi_importer.publications import ZenodoResource
from mardiclient import config
from mardiclient import MardiClient



class ZenodoSource(ADataSource):
    """Reads data from Zenodo API."""

    def __init__(
        self,
        out_dir,
        raw_dump_path=None,
        processed_dump_path=None,
    ):
        """
        Args:
            out_dir (string): target directory for saved files
            tags (list): list of tags to extract from the zbMath response
            from_date (string, optional): earliest date from when to pull information
            until_date (string, optional): latest date from when to pull information
            raw_dump_path (string, optional): path where the raw data dump is located, in case it has previously been pulled
            processed_dump_path (string, optional): path to the processed dump file
            split_id (string, optional): Zenodo id from where to start processing the raw dump, in case it aborted mid-processing
        """


        config['IMPORTER_API_URL'] = 'https://importer.staging.mardi4nfdi.org'
        config['MEDIAWIKI_API_URL'] = 'https://staging.mardi4nfdi.org/w/api.php'
        config['SPARQL_ENDPOINT_URL'] = 'http://query.staging.mardi4nfdi.org/proxy/wdqs/bigdata/namespace/wdq/sparql'
        config['WIKIBASE_URL'] = 'https://staging.mardi4nfdi.org'
    
        if out_dir[-1] != "/":
            out_dir = out_dir + "/"
        self.out_dir = out_dir

        self.integrator = MardiIntegrator()
        #self.integrator = mc
        self.filepath = os.path.realpath(os.path.dirname(__file__))
        self.raw_dump_path = raw_dump_path
        os.makedirs(self.raw_dump_path, exist_ok=True)
        self.processed_dump_path = processed_dump_path
        os.makedirs(self.processed_dump_path, exist_ok=True)

    def setup(self):
        """Create all necessary properties and entities for zenodo"""
        
        filename = self.filepath + "/wikidata_entities.txt"
        self.integrator.import_entities(filename=filename)
        self.create_local_entities()

    def write_data_dump(self):
        """
        Overrides abstract method.
        This method queries the Zenodo API to get a data dump of all records.
        """

        access_token = "OjHPMu82rl7uLYf2YjGzhxrUVCEGuwhLHMGsP97Yg5X5fjPIC59ChKI7sUoT"

        timestr = time.strftime("%Y%m%d-%H%M%S")
        #self.raw_dump_path = self.out_dir + "/raw_zenodo_data_dump/" + timestr + ".txt"

        # TODO: better way to get all hits? could use a really large number. but this works
        response = requests.get('https://zenodo.org/api/records',
                                params={'size' : 1,
                                'communities' : 'mathplus',
                                'access_token': access_token})
        response_json = response.json()
        total_hits = response_json.get("hits").get("total")

        for page in range(1, total_hits+1):
            url = 'https://zenodo.org/api/records?communities=mathplus&page=' + str(page) + "&size=1&sort=newest"
            response = requests.get(url, params = {'access_token' : access_token})    
            response_json = response.json()

            zenodo_id = response_json.get("hits").get("hits")[0].get("id")
            # TODO: reformat time
            #date_created = response_json.get("hits").get("hits")[0].get("created")
            # TODO: can probably use date created for early stopping
            out_file = "/id_" + str(zenodo_id) + ".json"
            with open(self.raw_dump_path + out_file, 'w+') as f:
                json.dump(response_json, f)
        

    #     # TODO: do i want to save records as a batch per json file or 1 json file per hit.
        

    #     # TODO: error handling if other than Response 200. seems to time out (504) when it exceeds 1000 records per page
    # def write_data_dump(self):
    #     """
    #     Overrides abstract method.
    #     This method queries the Zenodo API to get a data dump of all records.
    #     """

    #     access_token = "OjHPMu82rl7uLYf2YjGzhxrUVCEGuwhLHMGsP97Yg5X5fjPIC59ChKI7sUoT"

    #     timestr = time.strftime("%Y%m%d-%H%M%S")
    #     self.raw_dump_path = self.out_dir + "raw_zenodo_data_dump" + timestr + ".txt"

    #     response = requests.get('https://zenodo.org/api/records',
    #                     params={'size' : 200,
    #                             'communities' : 'mathplus',
    #                             'access_token': access_token})
        
    #     zenodo_id = response_json.get("hits").get("hits")[0].get("id")
    #         # TODO: reformat time
    #         date_created = response_json.get("hits").get("hits")[0].get("created")
    #         # TODO: can probably use date created for early stopping
    #         out_file = "id_" + str(zenodo_id) + "\tcreation_date_" + date_created 
    #         with open(self.raw_dump_path + out_file, 'w+') as f:
    #             json.dump(response_json, f)
     


    def process_data(self, update = True):
        """
        Overrides abstract method.
        Reads a raw Zenodo data dump and processes it, then saves it as a csv.
        """
        # Load the dict object with prev records

        file = Path(self.processed_dump_path + "/zenodoData_dict.pkl")
        if file.is_file():
            print ("Loading existing processed data file")
            file = open(self.processed_dump_path + "/zenodoData_dict.pkl" , 'rb')
            records_all = pickle.load(file)
            file.close()
        else:
            records_all = {}

        
        for fname in glob(self.raw_dump_path + "*.json"):
            id = str(re.findall(r'\d+', fname)[0])
            print ("processing file with id " + str(id)) 
            if (id not in records_all.keys()) or update :
                print (id)
                with open(fname, 'r') as f:
                    record_json = json.load(f) 
                    record_cur = self.parse_record(record_json)
                    if record_cur:
                            records_all[id] =  record_cur

        with open(self.processed_dump_path + "/zenodoData_dict.pkl", 'wb') as outfile:
            pickle.dump(records_all, outfile)
        outfile.close()




    def parse_record(self, json_record):

        """Parse JSON record from Zenodo API

        Args: 
            json_record:

        Returns:
            dict: dict of (tag;value) pairs extracted from json record
        """

        new_entry = {}

        for entry in json_record.get("hits").get("hits"):
            entry_metadata = entry.get("metadata")
            new_entry["zenodo_id"] = entry.get("id")
            #new_entry["creation_date"] = entry.get("created")
            new_entry["title"] = entry.get("title")
            new_entry["doi"] = entry.get("doi") 
            # retrieve list of authors
            # new_entry["authors"] = []
            # for auth in entry_metadata.get("creators"):
            #     new_entry["authors"].append(auth.get("name"))
            # new_entry["description"] = entry_metadata.get("description")
            new_entry["publication_date"] = entry_metadata.get("publication_date")
            # new_entry["journal"] = entry_metadata.get("journal")
            # new_entry["language"] = entry_metadata.get("language")
            
            # new_entry["license"] = entry_metadata.get("license")
            # new_entry["instance_of"] = entry_metadata.get("resource_type").get("type")
            # # retrieve list of community ids
            # new_entry["communities"] = []
            # if entry_metadata.get("communities") is not None:
            #     for c in entry_metadada.get("communities"):
            #         new_entry["communities"].append(c.get("id"))
            new_entry["metadata"] = entry_metadata
        
        return new_entry

    def pull(self):
        #self.write_data_dump()
        #self.process_data()
        print("skip")

    def create_local_entities(self):
        filename = self.filepath + "/new_entities.json"
        f = open(filename)
        entities = json.load(f)

        for prop_element in entities["properties"]:
            prop = self.integrator.property.new()
            prop.labels.set(language="en", value=prop_element["label"])
            prop.descriptions.set(language="en", value=prop_element["description"])
            prop.datatype = prop_element["datatype"]
            if not prop.exists():
                prop.write()

        for item_element in entities["items"]:
            item = self.integrator.item.new()
            item.labels.set(language="en", value=item_element["label"])
            item.descriptions.set(language="en", value=item_element["description"])
            if "claims" in item_element:
                for key, value in item_element["claims"].items():
                    item.add_claim(key, value=value)
            if not item.exists():
                item.write()

        

    def push(self):

        file = open(self.processed_dump_path + "/zenodoData_dict.pkl" , 'rb')
        records_all = pickle.load(file)
        file.close()

        for x in records_all:
            
            entry = ZenodoResource.ZenodoResource(
                mc,
                zenodo_id = str(x),
                #title = records_all[x]['title'],
            #_publication_date = entry['publication_date'],
            #_authors = entry['authors'],
            #_resource_type = entry['resource_type'],
            #_license = entry['lisence'],
            metadata = records_all[x]['metadata'])

            if not entry.exists():
                print ("creatig entry for zenodo id" + str(x))
                entry.create(update = False)
            else:
                print (" entry for zenodo id " + str(x) + " already exists. updating")
                entry.create(update = True)
            
            #break


