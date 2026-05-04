import pandas as pd
from mardiclient import MardiClient
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_exceptions import ModificationFailed
from collections import defaultdict
import os
import json
import re
import sys

#This is a script for deleting references to deleted items.

input_file = "input_file.json/.csv"
if input_file.endswith(".json"):
    with open(input_file) as f:
        qids = list(json.load(f).keys())
else:
    df = pd.read_csv(input_file, sep="\t")
    qids = [row["item"].split("/")[-1] for _, row in df.iterrows()]

mc = MardiClient(user='CorrectionBot', password=os.environ["MARDI_CORRECTION_BOT_PASSWORD"])

not_found_re = re.compile(r"Item:(Q\d+)\|")

for qid in qids:

    query = f"""
    SELECT ?item ?property WHERE {{
    ?item ?property wd:{qid} .
    FILTER(!CONTAINS(STR(?item), "statement"))
    FILTER(!CONTAINS(STR(?item), "Publication"))
    FILTER(?property != <http://www.w3.org/2002/07/owl#sameAs>)
    }}
    """
    results = execute_sparql_query(query)
    target_prop_dict = defaultdict(set)
    for binding in results["results"]["bindings"]:
        item = binding["item"]["value"]
        item = item.split("/")[-1]
        prop = binding["property"]["value"].split("/")[-1]
        target_prop_dict[item].add(prop)
    for k, v in target_prop_dict.items():
        qids_to_remove = {qid}

        for attempt in range(10):  # safety cap; usually 1-2 iterations
            change_item = mc.item.get(entity_id=k)
            for prop in v:
                claims = change_item.claims.get(prop)
                for claim in list(claims):
                    if (claim.mainsnak.datavalue
                        and claim.mainsnak.datavalue["value"]["id"] in qids_to_remove):
                        claim.remove()
            # Also sweep ALL properties for references to the dangling QIDs we've discovered: the offending claim may
            # live on a property that wasn't in our original target set for this item.
            extra_dangling = qids_to_remove - {qid}
            if extra_dangling:
                for prop_id in list(change_item.claims.claims.keys()):
                    for claim in list(change_item.claims.get(prop_id)):
                        dv = claim.mainsnak.datavalue
                        if (dv and isinstance(dv.get("value"), dict)
                            and dv["value"].get("id") in extra_dangling):
                            claim.remove()
            try:
                change_item.write()
                break
            except ModificationFailed as e:
                msg = str(e)
                match = not_found_re.search(msg)
                if not match:
                    sys.exit(f"Problem with writing {k}: {msg}")
                bad_qid = match.group(1)
                if bad_qid in qids_to_remove:
                    # Same QID reported twice — something else is wrong, don't loop forever.
                    sys.exit(f"Repeated failure on {k} for {bad_qid}: {msg}")
                print(f"Discovered dangling reference {bad_qid} on {k}, retrying")
                qids_to_remove.add(bad_qid)
        else:
            sys.exit(f"Too many retries on {k}")