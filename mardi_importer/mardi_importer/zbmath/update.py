import pandas as pd
import os
import json
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
print(f"starting mardiclient login for username {username}")
mc = MardiClient(user=username, password=password)
print("finished mardiclient login")
error_string = 'zbMATH Open Web Interface contents unavailable due to conflicting licenses.'

label_id_dict = {}
#label_id_dict["de_number_prop"] = mc.get_local_id_by_label( #de_number_prop
#    "zbMATH DE Number", "property"
#)
label_id_dict["de_number_prop"] = "P1451"
#label_id_dict["keyword_prop"] = mc.get_local_id_by_label( #keyword_prop
#    "zbMATH Keywords", "property"
#)
label_id_dict["keyword_prop"] = "P1450"
#label_id_dict["review_prop"] = mc.get_local_id_by_label("review text", "property")
label_id_dict["review_prop"] = "P1448"
#label_id_dict["mardi_profile_type_prop"] = mc.get_local_id_by_label("MaRDI profile type", "property")
label_id_dict["mardi_profile_type_prop"] = "P1460"
#label_id_dict["mardi_publication_profile_item"] = mc.get_local_id_by_label(
#    "Publication", "item"
#)[0]
label_id_dict["mardi_publication_profile_item"] = "Q5976449"
#label_id_dict["mardi_person_profile_item"] = mc.get_local_id_by_label("Person", "item")[0]
label_id_dict["mardi_person_profile_item"] = "Q5976445"

property_dict = {"document_title":"P159","classifications":"P226","keywords":"P1450","publication_year":"P28","serial":"P200","links":"P205",
     "doi":"P27","author":"P16","reviewers":"P1447","review_text":"P1448", "arxiv_id":"P21","source":"P22","preprint":"P1676"}

print("started reading df")
df = pd.read_csv('/scratch/visual/lpusch/mardi/differences_zbmath_data_dump20231221_TO_zbmath_data_dump20240912.csv', sep='\t')
print("finished reading df")

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
    """Resolve (and CREATE if missing) author items for the NEW data.

    Returns the list of QIDs to put on the item.
    """
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

def lookup_authors(author_strings, author_ids):
    """Resolve author items for the OLD data WITHOUT creating anything.

    Only authors that already exist can possibly still be on the item, so a
    missing author is simply skipped. Used to figure out which old author
    claims to remove.
    """
    authors = []
    if len(author_strings) != len(author_ids):
        # be tolerant for the old side: pad the names to match the ids
        if len(author_strings) < len(author_ids):
            author_strings = author_strings + [""] * (len(author_ids) - len(author_strings))
        else:
            author_strings = author_strings[:len(author_ids)]
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
                    authors.append(author.QID)
            except Exception as e:
                print(f"Exception: {e}, sleeping")
                print(traceback.format_exc())
                time.sleep(120)
            else:
                break
        else:
            sys.exit("Looking up old author did not work after retries!")
    return authors

def _old_authors(row):
    """Old author QIDs (lookup only) from the non-`_new` columns."""
    if is_empty(row["author_ids"]):
        return []
    old_ids = [x.strip() for x in row["author_ids"].split(";")]
    if not is_empty(row["author"]):
        old_strings = [x.strip() for x in row["author"].split(";")]
    else:
        old_strings = [""] * len(old_ids)
    return lookup_authors(author_strings=old_strings, author_ids=old_ids)

def _old_reviewers(row):
    """Old reviewer QIDs (lookup only) from the non-`_new` columns."""
    if is_empty(row["reviewer_id"]):
        return []
    old_ids = [x.strip() for x in row["reviewer_id"].split(";")]
    if not is_empty(row["review_sign"]):
        old_strings = [x.strip() for x in row["review_sign"].split(";")]
    else:
        old_strings = [""] * len(old_ids)
    return lookup_authors(author_strings=old_strings, author_ids=old_ids)

def get_or_create_journal(name):
    """Resolve (and CREATE if missing) a journal item for NEW data -> QID."""
    if is_empty(name):
        return None
    for attempt in range(5):
        try:
            journal_item = ZBMathJournal(integrator=mc, name=name)
            if journal_item.exists():
                print(f"Journal {name} exists!")
                return journal_item.QID
            print(f"Creating journal {name}")
            return journal_item.create()
        except Exception as e:
            print(f"Exception: {e}, sleeping")
            print(traceback.format_exc())
            time.sleep(120)
    else:
        sys.exit("Uploading journal did not work after retries!")

def lookup_journal(name):
    """Resolve a journal item for OLD data WITHOUT creating it -> QID or None."""
    if is_empty(name):
        return None
    for attempt in range(5):
        try:
            journal_item = ZBMathJournal(integrator=mc, name=name)
            return journal_item.QID if journal_item.exists() else None
        except Exception as e:
            print(f"Exception: {e}, sleeping")
            print(traceback.format_exc())
            time.sleep(120)
    else:
        sys.exit("Looking up old journal did not work after retries!")

# ---- small parsers so OLD and NEW raw strings are normalised identically ----
_link_pattern = re.compile(r"^([a-z][a-z\d+.-]*):([^][<>\"\x00-\x20\x7F])+$")
_arxiv_prefix = "https://arxiv.org/abs/"

def parse_classifications(v):
    return v.strip().split(";") if not is_empty(v) else []

def parse_keywords(v):
    return [x.strip() for x in v.split(";")] if not is_empty(v) else []

def parse_year(v):
    return [f"+{v.strip()}-00-00T00:00:00Z"] if not is_empty(v) else []

def parse_source(v):
    if is_empty(v):
        return []
    m = re.search(r'\[(.*?)\]', v)
    return [m.group(1)] if m else []

def parse_simple(v):
    """doi / review_text / document_title style single string."""
    return [v.strip()] if not is_empty(v) else []

def parse_links(v):
    """Return (non_arxiv_links, arxiv_id) for a raw links string."""
    if is_empty(v):
        return [], None
    links = v.split(";")
    links = [x.strip() for x in links if (_link_pattern.match(x) and "http" in x)]
    arxiv_id = None
    remaining = []
    for l in links:
        if _arxiv_prefix in l:
            arxiv_id = l.removeprefix(_arxiv_prefix)
        else:
            remaining.append(l)
    return remaining, arxiv_id

def claim_scalar(claim):
    """Reduce an on-item claim to a value comparable with our normalised
    old/new scalars (QID for items, time string for time, text for
    monolingual, plain string otherwise)."""
    snak = claim.mainsnak
    datavalue = snak.datavalue or {}
    value = datavalue.get("value")
    if value is None:
        return None
    datatype = snak.datatype
    if datatype == "wikibase-item":
        return value.get("id")
    if datatype == "time":
        return value.get("time")
    if datatype == "monolingualtext":
        return value.get("text")
    if datatype == "quantity":
        return value.get("amount")
    # string, external-id, url, ...
    return value

# Properties listed here are treated as authoritative: the new data fully
# replaces whatever is on the item (old behaviour). Leave empty to merge
# everything surgically; add a key (e.g. "author") if for some property you
# would rather wipe and replace than preserve extra on-item values.
AUTHORITATIVE_KEYS = set()


# ---- test-batch + resume configuration -------------------------------------
# How many successful item writes to perform in this run. Set to a positive
# integer to run a small test batch then stop cleanly; leave as None (or set
# UPDATE_TEST_BATCH_SIZE=0) to process everything.
TEST_BATCH_SIZE = int(os.environ.get("UPDATE_TEST_BATCH_SIZE", "0")) or None

# Where the resume position is persisted. Progress is tracked on every run,
# whether or not a test batch limit is set.
PROGRESS_FILE = os.environ.get("UPDATE_PROGRESS_FILE", "update_progress.json")

# de_number to resume *after* on the very first run, before any progress has
# been saved (matches the original hard-coded starting point). Set to None to
# start from the first row. Once a progress file exists it takes precedence.
START_AFTER_DE_NUMBER = 1566567

def load_progress():
    """Return the de_number of the last successfully written item, or None."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f).get("last_de_number")
        except (json.JSONDecodeError, OSError):
            return None
    return None

def save_progress(de_number):
    """Persist the last successfully written de_number (atomic write)."""
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_de_number": int(de_number)}, f)
    os.replace(tmp, PROGRESS_FILE)


#author_ids will be used together with authors
resume_after = load_progress()
if resume_after is None:
    resume_after = START_AFTER_DE_NUMBER

found = resume_after is None  # nothing to skip -> start immediately
successful_writes = 0
for _, row in df.iterrows():
    old_cols = [x for x in list(df) if not x.endswith("_new") and x not in ["de_number","source", "zbl_id", "language"]]
    if not is_empty(row["zbl_id"]):
        if isinstance(row["zbl_id"], str):
            if "arXiv" in row["zbl_id"]:
                old_cols = ["document_title", "classifications","author","author_ids","publication_year","source"]

    if not found:
        if row["de_number"] == resume_after:
            found = True
        continue
    authors_done = False
    reviewers_done=False
    change_dict = {}      # key -> NEW normalised value(s)
    old_value_dict = {}   # key -> OLD normalised value(s), same comparable form
    for col in old_cols:
        old_val = row[col]
        new_val = row[col+"_new"]
        if not is_empty(new_val) and old_val!=new_val:
            # if col == "creation_date":
            #     change_dict[col] = f"{new_val.split('T')[0]}T00:00:00Z"
            if col == "document_title":
                change_dict[col] = new_val.strip()
                old_value_dict[col] = parse_simple(old_val)
            elif col == "classifications":
                change_dict[col] = parse_classifications(new_val)
                old_value_dict[col] = parse_classifications(old_val)
            # elif col == "language":
            #     change_dict[col] = new_val.strip()
            elif col == "keywords":
                change_dict[col] = parse_keywords(new_val)
                old_value_dict[col] = parse_keywords(old_val)
            elif col == "publication_year":
                change_dict[col] = parse_year(new_val)[0]
                old_value_dict[col] = parse_year(old_val)
            elif col == "source":
                parsed = parse_source(new_val)
                if not parsed:
                    continue
                change_dict[col] = parsed[0]
                old_value_dict[col] = parse_source(old_val)
            elif col == "serial":
                #I don't need to check the doi here because this means
                #that there definitely is a serial
                new_journal = new_val.split(";")[-1].strip()
                old_journal = old_val.split(";")[-1].strip() if not is_empty(old_val) else ""
                if new_journal == old_journal:
                    continue
                change_dict[col] = get_or_create_journal(new_journal)
                old_qid = lookup_journal(old_journal)
                old_value_dict[col] = [old_qid] if old_qid else []
            elif col == "links":
                new_links, new_arxiv = parse_links(new_val)
                old_links, old_arxiv = parse_links(old_val)
                if new_arxiv:
                    change_dict["arxiv_id"] = new_arxiv
                if new_links:
                    change_dict[col] = new_links
                    old_value_dict[col] = old_links
            elif col == "doi":
                new_val = new_val.strip()
                if is_empty(row["document_title"]) and is_empty(row["document_title_new"]):
                    document_title = get_info_from_doi(
                            doi=new_val, key="document_title"
                        )
                    if document_title:
                        # pure addition (title was empty old and new)
                        change_dict["document_title"] = document_title
                        old_value_dict["document_title"] = []
                if is_empty(row["serial"]) and is_empty(row["serial_new"]):
                    journal_string = get_info_from_doi(
                            doi=new_val, key="journal"
                        )
                    if journal_string:
                        journal = get_or_create_journal(journal_string)
                        # pure addition (serial was empty old and new)
                        change_dict["serial"] = journal
                        old_value_dict["serial"] = []
                change_dict[col] = new_val
                old_value_dict[col] = parse_simple(old_val)
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
                old_value_dict["author"] = _old_authors(row)
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
                old_value_dict["author"] = _old_authors(row)
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
                old_value_dict["reviewers"] = _old_reviewers(row)
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
                old_value_dict["reviewers"] = _old_reviewers(row)
                reviewers_done = True
            elif col == "review_text":
                change_dict[col] = new_val.strip()
                old_value_dict[col] = parse_simple(old_val)
    if change_dict:
        de_number = row["de_number"]
        hits = mc.search_entity_by_value("P1451", str(int(de_number)))
        qid = hits[0] if hits else None
        if not qid:
            print(f"No item for de number {de_number}")
            print(wtf)
        item = mc.item.get(qid)
        arxiv_switch = False
        stop_switch = False
        for key, val in change_dict.items():
            if key in ['creation_date','links', 'source', 'classifications', 'keywords', 'doi', 'publication_year', "arxiv_id"]:
                stop_switch = True

            # document title is also kept as a label
            if key == "document_title":
                item.labels.set(language="en", value=val)
            elif key == "arxiv_id":
                # not a claim on this item; handled via the linked arXiv item below
                arxiv_switch = True
                continue

            prop = property_dict[key]

            # normalise new/old values to comparable lists of scalars
            new_vals = val if isinstance(val, list) else [val]
            new_vals = [v for v in new_vals if not is_empty(v)]
            old_raw = old_value_dict.get(key, [])
            old_vals = old_raw if isinstance(old_raw, list) else [old_raw]
            old_vals = [v for v in old_vals if not is_empty(v)]

            existing_claims = item.claims.get(prop)

            if key in AUTHORITATIVE_KEYS:
                # old behaviour: new data fully replaces the property
                new_claims = [mc.get_claim(prop, v) for v in new_vals]
                new_claims = [c for c in new_claims if c]
                if new_claims:
                    item.add_claims(new_claims, ActionIfExists.REPLACE_ALL)
                continue

            new_set = set(new_vals)
            old_set = set(old_vals)

            # 1) Remove the OLD value(s) only if they are still on the item AND
            #    they are not part of the NEW data. Anything else on the item
            #    (e.g. values added elsewhere) is left untouched.
            for existing in existing_claims:
                scalar = claim_scalar(existing)
                if scalar in old_set and scalar not in new_set:
                    existing.remove()

            # 2) Add NEW value(s) that are not already present (after removals),
            #    so additions happen and nothing gets duplicated.
            present_after = {
                claim_scalar(c) for c in item.claims.get(prop) if not c.removed
            }
            for v in new_vals:
                if v in present_after:
                    continue
                claim = mc.get_claim(prop, v)
                if not claim:
                    continue
                # values verified absent -> plain append, never touches the
                # removed/old claims or unrelated siblings
                item.add_claims([claim], ActionIfExists.FORCE_APPEND)
                present_after.add(v)

        # single commit for all removals, additions and the label change
        item.write()

        # record progress on every successful write (independent of test batches)
        successful_writes += 1
        save_progress(de_number)

        if arxiv_switch:
            arxiv_papers = mc.search_entity_by_value(property_dict["arxiv_id"], change_dict["arxiv_id"])
            if arxiv_papers:
                arxiv_item = mc.item.get(arxiv_papers[0])
                claim = mc.claim.get(property_dict["preprint"],qid)
                arxiv_item.add_claims([claim])
                arxiv_item.write()
        if stop_switch:
            print(de_number)
            print(change_dict)
            print(a)

        if TEST_BATCH_SIZE and successful_writes >= TEST_BATCH_SIZE:
            print(f"Reached test batch limit ({TEST_BATCH_SIZE}); stopping at de_number {de_number}.")
            break
