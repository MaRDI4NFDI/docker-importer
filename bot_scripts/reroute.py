import pandas as pd 
from mardiclient import MardiClient
from wikibaseintegrator.wbi_helpers import execute_sparql_query, remove_claims
from wikibaseintegrator import wbi_login
import sys




mc = MardiClient(user='RedirectionBot', password='password')
df = pd.read_csv("redirection.csv")
new_ids = df["sa"].unique()
old_new_mapping = dict(zip(df["o"],df["sa"]))
old_new_mapping = {k.split("/")[-1] : v.split("/")[-1] for (k, v) in old_new_mapping.items()}
instance_property_mapping = {'Q57083':'P19'}
#found = False
for str_qid in new_ids:
    qid = str_qid.split("/")[-1]
    # if qid == "Q1770742":
    #     found = True
    #     continue
    # if not found: continue
    print(f"new id string: {str_qid}")
    print(f"new qid: {qid}")

    #now get all old ones that map to this id
    old_qids = df[df["sa"] == str_qid]["o"].unique()
    old_qids = [x.split("/")[-1] for x in old_qids]
    print(f"old_qids: {old_qids}")
    for q in old_qids:
        print(f"old qid in progress: {q}")
        query = ("PREFIX wdt: <https://portal.mardi4nfdi.de/prop/direct/> "
            "PREFIX wd: <https://portal.mardi4nfdi.de/entity/> "
            f"SELECT ?item ?itemLabel WHERE "
            f" {{  ?item ?property wd:{q} . }}")
        results = execute_sparql_query(query= query, endpoint="http://query.portal.mardi4nfdi.de/proxy/wdqs/bigdata/namespace/wdq/sparql")
        results = results["results"]["bindings"]
        #print(f"cases where old id is referenced: {results}")
        for r_dict in results:
            try:
                outdated_qid = r_dict["item"]["value"]
            except Exception as e:
                print(e)
                print(r_dict)
                print(a)
            if "/statement/" in outdated_qid:
                continue
            outdated_qid = outdated_qid.split("/")[-1]
            print(f"currently working on outdated qid {outdated_qid}")
            try:
                outdated_item = mc.item.get(entity_id=outdated_qid)
            except:
                print(f"trying to do item.get for outdated qid {outdated_qid} failed, skipping")
                continue
            claims = outdated_item.claims.get_json()
            found_instances = []
            #test = False
            for k in claims:
                for kk in claims[k]:
                    if 'entity-type' in kk['mainsnak']['datavalue']['value']:                        
                        try:
                            search_id = kk['mainsnak']['datavalue']['value']['id']
                        except Exception as e:
                            if isinstance(kk['mainsnak']['datavalue']['value'], str):
                                continue
                            print(e)
                            print("claim")
                            print(k)
                            print("kk")
                            print(kk)
                            print("value")
                            print(kk['mainsnak']['datavalue']['value'])
                            print(a)
                        if search_id in old_new_mapping:
                            #test = True
                            found_id = old_new_mapping[search_id]
                            breaker = False
                            while not breaker:
                                if found_id in old_new_mapping:
                                    found_id = old_new_mapping[found_id]
                                else:
                                    breaker = True
                        #else:
                        
                            #found_id = search_id
                            found_instances.append({"c_id":kk["id"], "pid": k, "correct_id": found_id})
            if not found_instances:
                continue
            print(f"found instances: {found_instances}")
            new_claims = []
            claims_to_remove  = []
            new_id_tracker = []
            for d in found_instances:
                print(f"current found instances dict {d}")
                claims_to_remove.append(d["c_id"])
                if (d["pid"] + d["correct_id"]) not in new_id_tracker:
                    new_claims.append({"pid": d["pid"], "qid": d["correct_id"]})
                    new_id_tracker.append(d["pid"] + d["correct_id"])
            print(f"new_id_tracker: {new_id_tracker}")
            print(f"claims to remove: {claims_to_remove}")
            print(f"new claims {new_claims}")
            remove_claims("|".join(claims_to_remove), login=mc.login)
            #for c in claims_to_remove:
            #    remove_claims(c, login=login)
            outdated_item = mc.item.get(entity_id=outdated_qid)
            print(f"new claims: {new_claims}")
            for d in new_claims:
                outdated_item.add_claim(d["pid"], d["qid"])
            outdated_item.write()
            print(f"write for outdated item {outdated_qid}")
            #print(a)
