import requests
import json
import time
from glob import glob
import re
import pickle
# response = requests.get('https://zenodo.org/api/records',
#                         params={#'status': 'published',
#                                 'size' : 10,
#                                 'access_token': "OjHPMu82rl7uLYf2YjGzhxrUVCEGuwhLHMGsP97Yg5X5fjPIC59ChKI7sUoT"})
# #print(response.json())

# response = requests.get('https://zenodo.org/api/records',
#                         params={#'status': 'published',
#                                 #'size' : 200,
#                                 'communities' : 'mathplus',
#                                 'access_token': "OjHPMu82rl7uLYf2YjGzhxrUVCEGuwhLHMGsP97Yg5X5fjPIC59ChKI7sUoT"})
# #print(json.dumps(response.json(), indent=2))

# response_json = response.json()

# for x in response_json.keys():
#         print(x)

#print(response_json.get("hits"))

# entry = response_json.get("hits").get("hits")
# #print(response_json.get("hits").get("total"))

# print(entry[1])
# for key in entry[1].keys():
#         print(key)
# # for (entry in test):
# #         metadata = test.get("metadata")
# print ("~~~~~")
# for key in entry[1].get("metadata").keys():
#         print(key)


# write the data dump

# access_token = "OjHPMu82rl7uLYf2YjGzhxrUVCEGuwhLHMGsP97Yg5X5fjPIC59ChKI7sUoT"

# # timestr = time.strftime("%Y%m%d-%H%M%S")
# # #self.raw_dump_path = self.out_dir + "raw_zenodo_data_dump" + timestr + ".txt"
# out_dir = "data_dump/"
# # # TODO: better way to get all hits? could use a really large number. but this works
# response = requests.get('https://zenodo.org/api/records',
#                         params={'size' : 1,
#                         'communities' : 'mathplus',
#                         'access_token': access_token})
# response_json = response.json()
# total_hits = response_json.get("hits").get("total")

# for page in range(1, total_hits+1):
#         url = 'https://zenodo.org/api/records?communities=mathplus&page=' + str(page) + "&size=1&sort=newest"
#         response = requests.get(url, params = {'access_token' : access_token})    
#         response_json = response.json()

#         zenodo_id = response_json.get("hits").get("hits")[0].get("id")
#         # TODO: reformat time
#         #date_created = response_json.get("hits").get("hits")[0].get("created")
#         # TODO: can probably use date created for early stopping
#         out_file = "id_" + str(zenodo_id) + ".json"
#         with open(out_dir + out_file, 'w+') as f:
#                 json.dump(response_json, f)


def parse_record(json_record):

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



out_dir = "data_dump/"

records_all = {}
update = True

results_dir = "data_processed/"


for fname in glob(out_dir + "*.json"):
        print(fname)
        id = str(re.findall(r'\d+', fname)[0])
        
        if (id not in records_all.keys()) or update :
                print (id)
                with open(fname, 'r') as f:
                        record_json = json.load(f) 
                        record_cur = parse_record(record_json)
                        if record_cur:
                              records_all[id] =  record_cur

with open(results_dir + "zenodoData_dict.pkl", 'wb') as outfile:
        pickle.dump(records_all, outfile)
outfile.close()


file = open("data_processed/zenodoData_dict.pkl", 'rb')
records_all = pickle.load(file)
file.close()