import pandas as pd
import os
#from .ZBMathJournal import ZBMathJournal
from ZBMathJournal import ZBMathJournal
from ZBMathAuthor import ZBMathAuthor
from misc import get_info_from_doi
from wikibaseintegrator.wbi_enums import ActionIfExists
from mardiclient import MardiClient
import traceback
import time
import re
import sys

username = os.environ.get('MARDI_USERNAME')
password = os.environ.get('MARDI_PASSWORD')
mc = MardiClient(user=username, password=password)
error_string = 'zbMATH Open Web Interface contents unavailable due to conflicting licenses.'

label_id_dict = {}
label_id_dict["de_number_prop"] = mc.get_local_id_by_label( #de_number_prop
    "zbMATH DE Number", "property"
)
label_id_dict["keyword_prop"] = mc.get_local_id_by_label( #keyword_prop
    "zbMATH Keywords", "property"
)
label_id_dict["review_prop"] = mc.get_local_id_by_label("review text", "property")
label_id_dict["mardi_profile_type_prop"] = mc.get_local_id_by_label("MaRDI profile type", "property")
label_id_dict["mardi_publication_profile_item"] = mc.get_local_id_by_label(
    "Publication", "item"
)[0]
label_id_dict["mardi_person_profile_item"] = mc.get_local_id_by_label("Person", "item")[0]

property_dict = {"document_title":"P159","classifications":"P226","keywords":"P1450","publication_year":"P28","serial":"P200","links":"P205",
     "doi":"P27","author":"P16","reviewers":"P1447","review_text":"P1448", "arxiv_id":"P21","source":"P22","preprint":"P1676"}

df = pd.read_csv('/scratch/visual/lpusch/mardi/differences_zbmath_data_dump20231221_TO_zbmath_data_dump20240912.csv', sep='\t')

def is_empty(val):
    if isinstance(val,str):
        val = val.strip()
    if pd.isna(val):
        return True
    elif not val:
        return True
    elif val == error_string:
        return True
    elif val == "0001-01-01T00:00:00Z":
        return True
    elif val == "None":
        return True
    return False

def generate_authors(author_strings,author_ids):
    authors = []
    if len(author_strings) != len(author_ids):
        sys.exit("author strings has different length than author ids")
    for a, a_id in zip(author_strings, author_ids):
        if is_empty(a_id):
            continue
        if is_empty(a):
            a = ""
        for attempt in range(5):
            try:
                author = ZBMathAuthor(
                    integrator=mc,
                    name=a,
                    zbmath_author_id=a_id,
                    label_id_dict=label_id_dict,
                )
                if author.exists():
                    print(f"Author {a} exists!")
                    author_id = author.QID
                    #potentially, the name is wrong, then it needs to be fixed
                    author_item = mc.item.get(entity_id=author_id)
                    if a:
                        name_parts = a.strip().split(",")
                        name = ((" ").join(name_parts[1:]) + " " + name_parts[0]).strip()
                        try:
                            label = author_item.labels.values['en'].value
                        except:
                            label = ""
                        if label != name:
                            author_item.labels.set(language="en", value=name)
                            author_item.write()
                else:
                    print(f"Creating author {a}")
                    author_id = author.create()
            except Exception as e:
                print(f"Exception: {e}, sleeping")
                print(traceback.format_exc())
                time.sleep(120)
            else:
                break
        else:
            sys.exit("Uploading author did not work after retries!")
        authors.append(author_id)
    return authors

#author_ids will be used together with authors
found = False
for _, row in df.iterrows():
    old_cols = [x for x in list(df) if not x.endswith("_new") and x not in ["de_number","source", "zbl_id", "language"]]
    if not is_empty(row["zbl_id"]):
        if isinstance(row["zbl_id"], str):
            if "arXiv" in row["zbl_id"]:
                old_cols = ["document_title", "classifications","author","author_ids","publication_year","source"]
            
    if not found:
        if row["de_number"] == 1566567:
            found= True
        continue
    authors_done = False
    reviewers_done=False
    change_dict = {}
    for col in old_cols:
        old_val = row[col]
        new_val = row[col+"_new"]
        if not is_empty(new_val) and old_val!=new_val:
            # if col == "creation_date":
            #     change_dict[col] = f"{new_val.split('T')[0]}T00:00:00Z"
            if col == "document_title":
                change_dict[col] = new_val.strip()
            elif col == "classifications":
                change_dict[col] = new_val.strip().split(";")
            # elif col == "language":
            #     change_dict[col] = new_val.strip()
            elif col == "keywords":
                change_dict[col] = [x.strip() for x in new_val.split(";")]
            elif col == "publication_year":
                change_dict[col] = f"+{new_val.strip()}-00-00T00:00:00Z"
            elif col == "source":
                change_dict[col] = re.search(r'\[(.*?)\]', new_val).group(1)
            elif col == "serial":
                #I don't need to check the doi here because this means
                #that there definitely is a serial
                new_val = new_val.split(";")[-1].strip()
                if new_val == old_val.split(";")[-1].strip():
                    continue
                for attempt in range(5):
                            try:
                                journal_item = ZBMathJournal(
                                    integrator=mc, name=new_val
                                )
                                if journal_item.exists():
                                    print(f"Journal {new_val} exists!")
                                    journal = journal_item.QID
                                else:
                                    print(f"Creating journal {new_val}")
                                    journal = journal_item.create()
                            except Exception as e:
                                print(f"Exception: {e}, sleeping")
                                print(traceback.format_exc())
                                time.sleep(120)
                            else:
                                break
                else:
                    sys.exit("Uploading journal did not work after retries!")
                change_dict[col] = journal
            elif col == "links":
                pattern = re.compile(
                        r"^([a-z][a-z\d+.-]*):([^][<>\"\x00-\x20\x7F])+$"
                    )
                links = new_val.split(";")
                links = [
                    x.strip() for x in links if (pattern.match(x) and "http" in x)
                ]
                arxiv_prefix = "https://arxiv.org/abs/"
                arxiv_id = None
                for l in links:
                    if arxiv_prefix in l:
                        arxiv_id = l.removeprefix(arxiv_prefix)
                        links.remove(l)
                        change_dict["arxiv_id"] = arxiv_id
                        break
                if links:
                    change_dict[col] = links
            elif col == "doi":
                new_val = new_val.strip()
                if is_empty(row["document_title"]) and is_empty(row["document_title_new"]):
                    document_title = get_info_from_doi(
                            doi=new_val, key="document_title"
                        )
                    if document_title:
                        change_dict["document_title"] = document_title
                if is_empty(row["serial"]) and is_empty(row["serial_new"]):
                    journal_string = get_info_from_doi(
                            doi=new_val, key="journal"
                        )
                    if journal_string:
                        for attempt in range(5):
                            try:
                                journal_item = ZBMathJournal(
                                    integrator=mc, name=journal_string
                                )
                                if journal_item.exists():
                                    print(f"Journal {journal_string} exists!")
                                    journal = journal_item.QID
                                else:
                                    print(f"Creating journal {journal_string}")
                                    journal = journal_item.create()
                            except Exception as e:
                                print(f"Exception: {e}, sleeping")
                                print(traceback.format_exc())
                                time.sleep(120)
                            else:
                                break
                        else:
                            sys.exit("Uploading journal did not work after retries!")
                        change_dict["serial"] = journal
                change_dict[col] = new_val
            elif col == "author":
                if authors_done:
                    continue
                author_strings = [x.strip() for x in  new_val.split(";")]
                if not is_empty(row["author_ids_new"]):
                    author_ids = [x.strip() for x in row["author_ids_new"].split(";")]
                elif not is_empty(row["author_ids"]):
                    author_ids = [x.strip() for x in row["author_ids"].split(";")]
                else:
                    continue
                authors = generate_authors(author_strings=author_strings,author_ids=author_ids)
                if authors: change_dict["author"] = authors   
                authors_done = True
            elif col == "author_ids":
                if authors_done:
                    continue
                author_ids = [x.strip() for x in new_val.split(";")]
                if not is_empty(row["author_new"]):
                    author_strings = [x.strip() for x in row["author_new"].split(";")]
                elif not is_empty(row["author"]):
                    author_strings = [x.strip() for x in row["author"].split(";")]
                else:
                    author_strings = [""] * len(author_ids)
                authors = generate_authors(author_strings=author_strings,author_ids=author_ids)
                if authors: change_dict["author"] = authors
                authors_done = True
            elif col == "review_sign":
                if reviewers_done:
                    continue
                reviewer_strings = [x.strip() for x in  new_val.split(";")]
                if not is_empty(row["reviewer_id_new"]):
                    reviewer_ids = [x.strip() for x in row["reviewer_id_new"].split(";")]
                elif not is_empty(row["reviewer_id"]):
                    reviewer_ids = [x.strip() for x in row["reviewer_id"].split(";")]
                else:
                    continue
                reviewers = generate_authors(author_strings=reviewer_strings,author_ids=reviewer_ids)
                if reviewers: change_dict["reviewers"] = reviewers
                reviewers_done = True
            elif col == "reviewer_id":
                if reviewers_done:
                    continue
                reviewer_ids = [x.strip() for x in new_val.split(";")]
                if not is_empty(row["review_sign_new"]):
                    reviewer_strings = [x.strip() for x in row["review_sign_new"].split(";")]
                elif not is_empty(row["review_sign"]):
                    reviewer_strings = [x.strip() for x in row["review_sign"].split(";")]
                else:
                    reviewer_strings = [""] * len(reviewer_ids)
                reviewers = generate_authors(author_strings=reviewer_strings,author_ids=reviewer_ids)
                if reviewers: change_dict["reviewers"] = reviewers
                reviewers_done = True
            elif col == "review_text":
                change_dict[col] = new_val.strip()
    if change_dict:
        de_number = row["de_number"]
        qid = mc.search_entity_by_value("P1451", str(de_number))[0]
        if not qid:
            print(f"No item for de number {de_number}")
            print(wtf)
        item = mc.item.get(qid)
        arxiv_switch = False
        claim_list = []
        stop_switch = False
        for key, val in change_dict.items():
            if key in ['creation_date','links', 'source', 'classifications', 'keywords', 'doi', 'publication_year', "arxiv_id"]:
                stop_switch = True
                    
            if key == "document_title":
                item.labels.set(language="en", value=val)
            elif key == "arxiv_id":
                arxiv_switch = True
                continue
            prop = property_dict[key]
            if isinstance(val, list):
                for v in val:
                    claim = mc.get_claim(prop, v)
                    claim_list.append(claim)
            else:
                claim = mc.get_claim(prop, val)
                claim_list.append(claim)
            item.add_claims(claim_list,ActionIfExists.REPLACE_ALL)
        item.write()
        if arxiv_switch:
            arxiv_paper = mc.search_entity_by_value(property_dict["arxiv_id"], change_dict["arxiv_id"])[0]
            if arxiv_paper:
                arxiv_item = mc.item.get(arxiv_paper)
                claim = mc.claim.get(property_dict["preprint"],qid)
                arxiv_item.add_claims([claim])
                arxiv_item.write()
        if stop_switch:
            print(de_number)
            print(change_dict)
            print(a)
