#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from importer.Importer import ADataSource
import time
from sickle import Sickle
import xml.etree.ElementTree as ET
import sys
from .misc import get_tag, parse_doi_info
from habanero import Crossref  # , RequestError
from requests.exceptions import HTTPError, ContentDecodingError


class ZBMathSource(ADataSource):
    """Reads data from zb math API."""

    def __init__(
        self,
        out_dir,
        tags,
        from_date=None,
        until_date=None,
        raw_dump_path=None,
        split_id=None,
        processed_dump_path=None,
    ):  # , path
        """
        @param path: string path to a csv file with one software name per row and 2 columns 'swMATH work ID' and 'Software'
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
        self.raw_dump_path = raw_dump_path
        self.processed_dump_path = processed_dump_path
        self.namespace = "http://www.openarchives.org/OAI/2.0/"
        self.preview_namespace = "https://zbmath.org/OAI/2.0/oai_zb_preview/"
        self.tag_namespace = "https://zbmath.org/zbmath/elements/1.0/"
        self.conflict_text = "zbMATH Open Web Interface contents unavailable due to conflicting licenses."
        # dict for counting how often a doi was not found and which agency it was registered with
        self.unknown_doi_agency_dict = {"Crossref": [], "crossref": [], "nonsense": []}
        # tags that will not be found in doi query
        self.internal_tags = ["author_id", "source", "classifications", "links"]

    def write_data_dump(self):
        """
        Overrides abstract method.
        This method uses the zbmath API to:
            * get a data dump from zbMath and save to a file
            (* filter paper references related to the softwares in self.software_list.)
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
        Reads a zbMath data dump and processes it, then saves it as a csv.
        """
        if not (self.processed_dump_path and self.split_mode):
            timestr = time.strftime("%Y%m%d-%H%M%S")
            self.processed_dump_path = (
                self.out_dir + "zbmath_data_dump" + timestr + ".txt"
            )
        # def do_all(xml_file, out_file):
        with open(self.raw_dump_path) as infile:
            with open(self.processed_dump_path, "a") as outfile:
                # if we are not continuing with a pre-filled file
                if not self.split_mode:
                    outfile.write("zbmath_id" + (",").join(self.tags) + "\n")
                record_string = ""
                for line in infile:
                    record_string = record_string + line
                    if line.endswith("</record>\n"):
                        element = ET.fromstring(record_string)
                        if self.split_mode:
                            zb_id = self.get_zb_id(element)
                            # if the last processed id is found
                            if zb_id == self.split_id:
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
                                ",".join(str(x) for x in record.values()) + "\n"
                            )
                        record_string = ""

    def parse_record(self, xml_record):
        """
        parse xml record from zbMath
        """
        is_conflict = False
        new_entry = {}
        # zbMath identifier
        zb_id = self.get_zb_id(xml_record)
        new_entry["id"] = zb_id
        # read tags
        zb_preview = xml_record.find(
            get_tag("metadata", namespace=self.namespace)
        ).find(get_tag("zbmath", self.preview_namespace))
        if zb_preview:
            for tag in self.tags:
                value = zb_preview.find(get_tag(tag, self.tag_namespace))
                if value is not None:
                    if len(value):
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

                    if text.startswith(self.conflict_text):
                        is_conflict = True
                        text = None

                    new_entry[tag] = text
                # if tag is not found in zbMath return, we still want to get it from doi
                else:
                    new_entry[tag] = None
            # if there were tags without information
            if is_conflict:
                if "doi" in new_entry:
                    if new_entry["doi"]:
                        new_entry = self.get_info_from_doi(new_entry, zb_id)
            # return record, even if incomplete
            return new_entry
        else:
            sys.exit("Error: zb_preview not found")

    def get_info_from_doi(self, entry_dict, zb_id):
        cr = Crossref(mailto="pusch@zib.de")
        doi = entry_dict["doi"]
        try:
            work_info = cr.works(ids=doi)
        # if the doi is not found, there is a 404
        except HTTPError:
            try:
                agency = cr.registration_agency(doi)[0]
                if agency == "Crossref" or agency == "crossref":
                    # this can happen, seems to be a crossref problem
                    self.unknown_doi_agency_dict[agency].append(zb_id)
                    # return entry dict unchanged
                else:
                    try:
                        self.unknown_doi_agency_dict[agency].append(zb_id)
                    except KeyError:
                        # if the agency has not yet been included in the dict
                        self.unknown_doi_agency_dict[agency] = [zb_id]
                # return entry dict unchanged
                return entry_dict
            except HTTPError:
                # return entry_dict unchanged
                return entry_dict
        for key, val in entry_dict.items():
            if val is None and key not in self.internal_tags:
                entry_dict[key] = parse_doi_info(key, work_info["message"])
        return entry_dict

    def get_zb_id(self, xml_record):
        zb_id = (
            xml_record.find(get_tag("header", self.namespace))
            .find(get_tag("identifier", namespace=self.namespace))
            .text
        )
        return zb_id

    def write_error_ids(self):
        """
        Function for writing DOIs for which the Crossref API returned an error to a file, together with
        the organization they are registered with
        """
        timestr = time.strftime("%Y%m%d-%H%M%S")
        out_path = self.out_dir + "error_ids" + timestr + ".txt"
        with open(out_path, "w") as out_file:
            for k, val in self.unknown_doi_agency_dict.items():
                out_file.write(k + "," + ",".join(val) + "\n")

    def get_invalid_dois(self):
        with open(self.raw_dump_path) as infile:
            record_string = ""
            for line in infile:
                record_string = record_string + line
                if line.endswith("</record>\n"):
                    element = ET.fromstring(record_string)
                    zb_id = self.get_zb_id(element)
                    doi = (
                        element.find(get_tag("metadata", namespace=self.namespace))
                        .find(get_tag("zbmath", self.preview_namespace))
                        .find(get_tag("doi", self.tag_namespace))
                    )
                    zb_preview = element.find(
                        get_tag("metadata", namespace=self.namespace)
                    ).find(get_tag("zbmath", self.preview_namespace))
                    if zb_preview:
                        doi_xml = zb_preview.find(get_tag("doi", self.tag_namespace))
                        if doi_xml is not None:
                            doi = doi_xml.text
                        if doi is not None:
                            cr = Crossref(mailto="pusch@zib.de")
                            try:
                                cr.works(ids=doi)
                            # if the doi is not found, there is a 404
                            except HTTPError:
                                try:
                                    agency = cr.registration_agency(doi)[0]
                                    try:
                                        self.unknown_doi_agency_dict[agency].append(
                                            zb_id
                                        )
                                    except KeyError:
                                        # if the agency has not yet been included in the dict
                                        self.unknown_doi_agency_dict[agency] = [zb_id]
                                except HTTPError:
                                    self.unknown_doi_agency_dict["nonsense"].append(doi)
                            except ContentDecodingError as e:
                                print(doi)
                                print(e)
                                sys.exit("Content decoding error")
                    record_string = ""

    # read all dois from file;
    # see if available; maybe take less costly method for that if possible
    # look at agency
