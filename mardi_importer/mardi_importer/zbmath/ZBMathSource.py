import json
import os
import re
import sys
import time
import traceback
import xml.etree.ElementTree as ET

from datetime import datetime
from habanero import Crossref  # , RequestError
from requests.exceptions import HTTPError, ContentDecodingError
from sickle import Sickle

from mardi_importer.integrator import MardiIntegrator
from mardi_importer.importer import ADataSource
from .ZBMathPublication import ZBMathPublication
from .ZBMathAuthor import ZBMathAuthor
from .ZBMathJournal import ZBMathJournal
from .misc import get_tag, get_info_from_doi


class ZBMathSource(ADataSource):
    """Reads data from zb math API."""

    def __init__(
        self,
        out_dir,
        tags,
        from_date=None,
        until_date=None,
        raw_dump_path=None,
        processed_dump_path=None,
        split_id=None,
    ):  # , path
        """
        Args:
            out_dir (string): target directory for saved files
            tags (list): list of tags to extract from the zbMath response
            from_date (string, optional): earliest date from when to pull information
            until_date (string, optional): latest date from when to pull information
            raw_dump_path (string, optional): path where the raw data dump is located, in case it has previously been pulled
            processed_dump_path (string, optional): path to the processed dump file
            split_id (string, optional): zbMath id from where to start processing the raw dump, in case it aborted mid-processing
        """
        # load the list of swMath software
        # software_df = pd.read_csv(path)
        # self.software_list = software_df['Software'].tolist()
        if out_dir[-1] != "/":
            out_dir = out_dir + "/"
        self.out_dir = out_dir
        self.split_id = split_id
        if self.split_id:
            self.split_mode = True
        else:
            self.split_mode = False
        self.from_date = from_date
        self.until_date = until_date
        self.tags = tags
        self.integrator = MardiIntegrator()
        self.conflict_string = (
            "zbMATH Open Web Interface contents unavailable due to conflicting licenses"
        )
        self.raw_dump_path = raw_dump_path
        self.filepath = os.path.realpath(os.path.dirname(__file__))
        self.processed_dump_path = processed_dump_path
        self.namespace = "http://www.openarchives.org/OAI/2.0/"
        self.preview_namespace = "https://zbmath.org/OAI/2.0/oai_zb_preview/"
        self.tag_namespace = "https://zbmath.org/zbmath/elements/1.0/"
        self.conflict_text = "zbMATH Open Web Interface contents unavailable due to conflicting licenses."
        # dict for counting how often a doi was not found and which agency it was registered with
        self.unknown_doi_agency_dict = {"Crossref": [], "crossref": [], "nonsense": []}
        # tags that will not be found in doi query
        self.internal_tags = ["author_id", "source", "classifications", "links"]
        self.existing_authors = {}
        self.existing_journals = {}
        self.existing_publications = []

    def setup(self):
        """Create all necessary properties and entities for zbMath"""
        # Import entities from Wikidata
        filename = self.filepath + "/wikidata_entities.txt"
        self.integrator.import_entities(filename=filename)
        self.create_local_entities()
        self.de_number_prop = self.integrator.get_local_id_by_label(
            "zbMATH DE Number", "property"
        )
        self.keyword_prop = self.integrator.get_local_id_by_label(
            "zbMATH keyword string", "property"
        )

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
            for key, value in item_element["claims"].items():
                item.add_claim(key, value=value)
            if not item.exists():
                item.write()

    def pull(self):
        # self.write_data_dump()
        self.process_data()

    def write_data_dump(self):
        """
        Overrides abstract method.
        This method queries the zbMath API to get a data dump of all records,
        optionally between from_date and until_date
        """
        timestr = time.strftime("%Y%m%d-%H%M%S")
        self.raw_dump_path = self.out_dir + "raw_zbmath_data_dump" + timestr + ".txt"
        sickle = Sickle("https://oai.zbmath.org/v1")
        # date has to have format like 2012-12-12
        if self.from_date and self.until_date:
            records = sickle.ListRecords(
                **{
                    "metadataPrefix": "oai_zb_preview",
                    "from": self.from_date,
                    "until": self.until_date,
                }
            )
        elif self.from_date:
            records = sickle.ListRecords(
                **{"metadataPrefix": "oai_zb_preview", "from": self.from_date}
            )
        elif self.until_date:
            records = sickle.ListRecords(
                **{"metadataPrefix": "oai_zb_preview", "until": self.until_date}
            )
        else:
            records = sickle.ListRecords(metadataPrefix="oai_zb_preview")
        with open(self.raw_dump_path, "w+") as f:
            for rec in records:
                f.write(rec.raw + "\n")

    def process_data(self):
        """
        Overrides abstract method.
        Reads a raw zbMath data dump and processes it, then saves it as a csv.
        """
        if not (self.processed_dump_path and self.split_mode):
            timestr = time.strftime("%Y%m%d-%H%M%S")
            self.processed_dump_path = (
                self.out_dir + "zbmath_data_dump" + timestr + ".csv"
            )
        # def do_all(xml_file, out_file):
        with open(self.raw_dump_path) as infile:
            with open(self.processed_dump_path, "a") as outfile:
                # if we are not continuing with a pre-filled file
                if not self.split_mode:
                    outfile.write(
                        "de_number\t"
                        + "creation_date\t"
                        + ("\t").join(self.tags)
                        + "_text\treview_sign\treviewer_id\n"
                    )
                record_string = ""
                for line in infile:
                    record_string = record_string + line
                    if line.endswith("</record>\n"):
                        element = ET.fromstring(record_string)
                        if self.split_mode:
                            de_number = self.get_de_number(element)
                            # if the last processed id is found
                            if de_number == self.split_id:
                                # next iteration, continue with writing
                                self.split_mode = False
                                record_string = ""
                                continue
                            else:
                                # continue searching
                                record_string = ""
                                continue
                        record = self.parse_record(element)
                        if record:
                            outfile.write(
                                "\t".join(str(x) for x in record.values()) + "\n"
                            )
                        record_string = ""

    def parse_record(self, xml_record):
        """
        Parse xml record from zbMath API.

        Args:
            xml_record (xml element): record returned by zbMath API

        Returns:
            dict: dict of (tag,value) pairs extracted from xml_record
        """
        is_conflict = False
        new_entry = {}
        # zbMath identifier
        de_number = self.get_de_number(xml_record)
        creation_date = self.get_creation_date(xml_record)
        new_entry["de_number"] = de_number
        new_entry["creation_date"] = creation_date
        # read tags
        zb_preview = xml_record.find(
            get_tag("metadata", namespace=self.namespace)
        ).find(get_tag("zbmath", self.preview_namespace))
        if zb_preview:
            for tag in self.tags:
                value = zb_preview.find(get_tag(tag, self.tag_namespace))
                if value is not None:
                    if len(value):
                        if tag == "review":
                            for subtag in ["review_text", "review_sign", "reviewer_id"]:
                                subvalue = value.find(
                                    get_tag(subtag, self.tag_namespace)
                                )
                                if subvalue is not None:
                                    if len(subvalue):
                                        sys.exit(f"tag {subtag} has children")
                                    else:
                                        text = subvalue.text
                                        if subtag == "review_text":
                                            text = text.replace("\t", " ")
                                            text = text.replace("\n", " ")
                                        new_entry[subtag] = text
                                else:
                                    new_entry[subtag] = None
                            continue

                        # element has children
                        texts = []
                        for child in value:
                            texts.append(child.text)
                        texts = [t for t in texts if t is not None]
                        text = ";".join(
                            texts
                        )  # multiple values are rendered as a semicolon-separated string

                    else:
                        # element content is a simple text
                        text = zb_preview.find(get_tag(tag, self.tag_namespace)).text

                    text = text.replace("\n", " ")
                    new_entry[tag] = text
                # if tag is not found in zbMath return, we still want to get it from doi
                else:
                    new_entry[tag] = None
            # return record, even if incomplete
            return new_entry
        else:
            sys.exit("Error: zb_preview not found")

    def push(self):
        """Updates the MaRDI Wikibase entities corresponding to zbMath publications.
        It creates a :class:`mardi_importer.zbmath.ZBMathPublication` instance
        for each publication. Authors and journals are added, as well.
        """
        found = False
        with open(self.processed_dump_path, "r") as infile:
            in_header_line = True
            for line in infile:
                if in_header_line:
                    headers = line.strip().split("\t")
                    in_header_line = False
                    continue
                split_line = line.strip().split("\t")
                # formatting error: skip
                if len(split_line) != len(headers):
                    continue
                info_dict = dict(zip(headers, split_line))
                # this part is for continuing at a certain position if the import failed
                # if not found:
                #     if info_dict["de_number"].strip() != " ":
                #     if info_dict["document_title"] != "Unimodular supergravity":
                #         continue
                #     else:
                #         found = True
                #         continue
                if info_dict["document_title"] in self.existing_publications:
                    print(
                        f"A publication with the name {info_dict['document_title']} was already created in this run."
                    )
                    continue
                # if there is not title, don't add
                if self.conflict_string in info_dict["document_title"]:
                    if (
                        self.conflict_string not in info_dict["doi"]
                        and info_dict["doi"] != "None"
                    ):
                        document_title = get_info_from_doi(
                            doi=info_dict["doi"].strip(), key="document_title"
                        )
                        if not document_title:
                            print("No title from doi, uploading empty")
                        else:
                            print(f"Found document title {document_title} from doi")
                    else:
                        print("No doi found, uploading empty.")
                        document_title = None
                # only upload those where there was a conflict before
                else:
                    print(f"Skipping non-conflict paper {info_dict['document_title']}")
                    continue
                    document_title = info_dict["document_title"].strip()
                if not info_dict["zbl_id"] == "None":
                    zbl_id = info_dict["zbl_id"]
                else:
                    zbl_id = None

                if (
                    not self.conflict_string in info_dict["author_ids"]
                    and "None" not in info_dict["author_ids"]
                ):
                    author_ids = info_dict["author_ids"].split(";")
                    if (
                        self.conflict_string in info_dict["author"]
                        or "None" in info_dict["author"]
                    ):
                        author_strings = [None] * len(author_ids)
                    else:
                        author_strings = info_dict["author"].split(";")
                    authors = []
                    for a, a_id in zip(author_strings, author_ids):
                        if a:
                            a = a.strip()
                        a_id = a_id.strip()
                        if a_id in self.existing_authors:
                            authors.append(self.existing_authors[a_id])
                            print(f"Author with name {a} was already created this run.")
                        else:
                            for attempt in range(5):
                                try:
                                    author = ZBMathAuthor(
                                        integrator=self.integrator,
                                        name=a,
                                        zbmath_author_id=a_id,
                                    )
                                    local_author_id = author.create()
                                except Exception as e:
                                    print(f"Exception: {e}, sleeping")
                                    print(traceback.format_exc())
                                    time.sleep(120)
                                else:
                                    break
                            else:
                                sys.exit("Uploading author did not work after retries!")
                            authors.append(local_author_id)
                            self.existing_authors[a_id] = local_author_id
                else:
                    authors = []

                if (
                    self.conflict_string in info_dict["serial"]
                    or info_dict["serial"].strip() == "None"
                ):
                    if (
                        self.conflict_string not in info_dict["doi"]
                        and info_dict["doi"] != "None"
                    ):
                        journal_string = get_info_from_doi(
                            doi=info_dict["doi"].strip(), key="journal"
                        )
                    else:
                        journal_string = None
                else:
                    journal_string = info_dict["serial"].split(";")[-1].strip()
                if journal_string:
                    if journal_string in self.existing_journals:
                        journal = self.existing_journals[journal_string]
                        print(
                            f"Journal {journal_string} was already created in this run."
                        )
                    else:
                        for attempt in range(5):
                            try:
                                journal_item = ZBMathJournal(
                                    integrator=self.integrator, name=journal_string
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
                        self.existing_journals[journal_string] = journal
                else:
                    journal = None

                if not self.conflict_string in info_dict["language"]:
                    language = info_dict["language"].strip()
                else:
                    language = None

                if not self.conflict_string in info_dict["publication_year"]:
                    time_string = (
                        f"+{info_dict['publication_year'].strip()}-00-00T00:00:00Z"
                    )
                else:
                    time_string = None

                if not self.conflict_string in info_dict["links"]:
                    pattern = re.compile(r'^([a-z][a-z\d+.-]*):([^][<>\"\x00-\x20\x7F])+$')
                    links = info_dict["links"].split(";")
                    links = [ x.strip() for x in links if (pattern.match(x) and "http" in x)]
                else:
                    links = []

                if (
                    not self.conflict_string in info_dict["doi"]
                    and not "None" in info_dict["doi"]
                ):
                    doi = info_dict["doi"].strip()
                else:
                    doi = None

                if info_dict["creation_date"] != "0001-01-01T00:00:00":
                    # because there can be no hours etc
                    creation_date = (
                        f"{info_dict['creation_date'].split('T')[0]}T00:00:00Z"
                    )
                else:
                    creation_date = None

                if (
                    not self.conflict_string in info_dict["review_text"]
                    and info_dict["review_text"].strip() != "None"
                ):
                    review_text = info_dict["review_text"].strip()
                    if (
                        not self.conflict_string in info_dict["review_sign"]
                        and info_dict["review_sign"].strip() != "None"
                        and not self.conflict_string in info_dict["reviewer_id"]
                        and info_dict["reviewer_id"].strip() != "None"
                    ):
                        reviewer_id = info_dict["reviewer_id"].strip()
                        reviewer_name = (
                            info_dict["review_sign"]
                            .strip()
                            .split("/")[0]
                            .strip()
                            .split("(")[0]
                            .strip()
                        )
                        if reviewer_id in self.existing_authors:
                            reviewer = self.existing_authors[reviewer_id]
                            print(
                                f"Reviewer with name {a} was already created this run."
                            )
                        else:
                            for attempt in range(5):
                                try:
                                    reviewer_object = ZBMathAuthor(
                                        integrator=self.integrator,
                                        name=reviewer_name,
                                        zbmath_author_id=reviewer_id,
                                    )
                                    reviewer = reviewer_object.create()
                                except Exception as e:
                                    print(f"Exception: {e}, sleeping")
                                    print(traceback.format_exc())
                                    time.sleep(120)
                                else:
                                    break
                            else:
                                sys.exit(
                                    "Uploading reviewer did not work after retries!"
                                )
                            self.existing_authors[reviewer_id] = reviewer
                    else:
                        reviewer = None
                else:
                    review_text = None
                    reviewer = None

                if (
                    not self.conflict_string in info_dict["classifications"]
                    and info_dict["classifications"].strip() != "None"
                ):
                    classifications = info_dict["classifications"].strip().split(";")
                else:
                    classifications = None

                if info_dict["de_number"].strip() != "None":
                    de_number = info_dict["de_number"].strip()
                else:
                    de_number = None

                if (
                    not self.conflict_string in info_dict["keywords"]
                    and info_dict["keywords"].strip() != "None"
                ):
                    keywords = info_dict["keywords"].strip().split(";")
                    keywords = [x.strip() for x in keywords]
                else:
                    keywords = None
                for attempt in range(5):
                    try:
                        publication = ZBMathPublication(
                            integrator=self.integrator,
                            title=document_title,
                            doi=doi,
                            authors=authors,
                            journal=journal,
                            language=language,
                            time=time_string,
                            links=links,
                            creation_date=creation_date,
                            zbl_id=zbl_id,
                            review_text=review_text,
                            reviewer=reviewer,
                            classifications=classifications,
                            de_number=de_number,
                            keywords=keywords,
                            de_number_prop=self.de_number_prop,
                            keyword_prop=self.keyword_prop,
                        )
                        if publication.exists():
                            print(f"Publication {document_title} exists")
                            publication.update()
                        else:
                            print(f"Creating publication {document_title}")
                            publication.create()
                    except Exception as e:
                        print(f"Exception: {e}, sleeping")
                        print(traceback.format_exc())
                        time.sleep(120)
                    else:
                        break
                else:
                    sys.exit("Uploading publication did not work after retries!")
                # in case a publication is listed twice; this normally happens
                # within a distance of a few lines
                if document_title:
                    self.existing_publications.append(document_title)
                self.existing_publications = self.existing_publications[-100:]

    def get_de_number(self, xml_record):
        """
        Get zbMath id from xml record.

        Args:
            xml_record (xml element): record returned by zbMath API

        Returns:
            string: zbMath ID
        """
        de_number = (
            xml_record.find(get_tag("header", self.namespace))
            .find(get_tag("identifier", namespace=self.namespace))
            .text
        )
        de_number = de_number.split(":")[-1]
        return de_number

    def get_creation_date(self, xml_record):
        """
        Get creation date from xml record.

        Args:
            xml_record (xml element): record returned by zbMath API

        Returns:
            string: creation date
        """
        creation_date = (
            xml_record.find(get_tag("header", self.namespace))
            .find(get_tag("datestamp", namespace=self.namespace))
            .text
        )
        return creation_date
