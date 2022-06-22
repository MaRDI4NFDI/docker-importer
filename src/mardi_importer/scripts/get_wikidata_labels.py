import pandas as pd
import requests
import sys

df = pd.read_csv("../../../../config/Properties_to_import_from_WD.txt", header=None)
label_dict = {}
for my_id in df[0]:
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={my_id}&props=labels&format=json"
    response = requests.post(url)
    try:
        label_dict[my_id] = response.json()["entities"][my_id]["labels"]["en"]["value"]
    except:
        print("Response invalid")
        print(response.json())
        sys.exit()
print(label_dict)
