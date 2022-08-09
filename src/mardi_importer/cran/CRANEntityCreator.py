#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikidata.EntityCreator import EntityCreator
from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
from mardi_importer.wikibase.WBItem import WBItem
from mardi_importer.wikibase.WBProperty import WBProperty
import configparser

class CRANEntityCreator(EntityCreator):

    def __init__(self, entity_list):
        config = configparser.ConfigParser()
        config.sections()
        config.read("/config/credentials.ini")
        username = config["default"]["username"]
        botpwd = config["default"]["password"]
        WIKIBASE_API = config["default"]["WIKIBASE_API"]
        self.wb_connection = WBAPIConnection(username, botpwd, WIKIBASE_API)
        super(CRANEntityCreator, self).__init__(entity_list)

    def create_entities(self):

        # Create Item for 'file LICENSE'
        item = WBItem("file LICENSE")
        item.add_description("Text file that contains license information")
        item.add_statement("WD_P31", "WD_Q207621")
        item.create()

        # Create Item for ACM License
        item = WBItem("ACM Software License Agreement")
        item.add_description(
            "Software License published by the Association for Computing Machinery"
        )
        item.add_statement("WD_P31", "WD_Q207621")
        item.add_statement(
            "WD_P2699",
            "https://www.acm.org/publications/policies/software-copyright-notice",
        )
        item.create()

        # Create Item for Unlimited License
        item = WBItem("Unlimited License")
        item.add_description("Unlimited Software License")
        item.add_statement("WD_P31", "WD_Q207621")
        item.create()

        # Create Wikidata QID property
        property = WBProperty("Wikidata QID")
        property.add_datatype("external-id")
        property.add_description(
            "Item identifier in Wikidata"
        )
        property.add_statement("WD_P1630", "https://www.wikidata.org/wiki/$1")
        wikidata_QID = property.create()

        # Version
        property = WBProperty("License version")
        property.add_datatype("string")
        property.add_description("License version identifier")
        property.create()

        # Create property for items about software
        item = WBItem("Property for items about software")
        item.add_description("attribute of a particular piece of software")
        property_software = item.create()

        # Create import property
        property = WBProperty("imports")
        property.add_datatype("wikibase-item")
        property.add_description(
            "Subject software imports object software"
        )
        property.add_statement("WD_P31", property_software)
        property.create()

        # Add property for items about software to 'depends on software'
        propertyp = WBProperty("digital object identifier")
        propertyp.add_statement("WD_P1630", "https://doi.org/$1")
        propertyp.update()

        # Add formatter URL property to DOI
        propertyp = WBProperty("depends on software")
        property.add_statement("WD_P31", property_software)
        propertyp.update()

        # Add formatter URL property to ORCID iD
        property = WBProperty("ORCID iD")
        property.add_statement("WD_P1630", "https://orcid.org/$1")
        property.update()

        # Add formatter URL property to ISSN
        property = WBProperty("ISSN")
        property.add_statement("WD_P1630", "https://www.worldcat.org/issn/$1")
        property.add_statement("WD_P1630", "http://www.issn.cc/$1")
        property.add_statement("WD_P1630", "https://portal.issn.org/resource/issn/$1")
        property.add_statement("WD_P1630", "https://api.crossref.org/journals/$1")
        property.update()

        # Add formatter URL property to ISBN-10
        property = WBProperty("ISBN-10")
        property.add_statement("WD_P1630", "https://wikidata.org/wiki/Special:BookSources/$1")
        property.update()

        # Add formatter URL property to ISBN-13
        property = WBProperty("ISBN-13")
        property.add_statement("WD_P1630", "https://wikidata.org/wiki/Special:BookSources/$1")
        property.update()

        # Create property: Related publication
        property = WBProperty("related publication")
        property.add_datatype("wikibase-item")
        property.add_description(
            "Reference publication in relation to a given entity"
        )
        property.create()

        license_QID_list  = ["Q28130012",
                             "Q27017232",
                             "Q616526",
                             "Q13785927",
                             "Q17817999",
                             "Q14624826",
                             "Q18517294",
                             "Q18491847",
                             "Q2353141",
                             "Q6938433",
                             "Q20007257",
                             "Q18199165",
                             "Q34179348",
                             "Q42553662",
                             "Q1052189",
                             "Q19216649",
                             "Q2477807",
                             "Q1281977",
                             "Q1376919",
                             "Q34236",
                             "Q1131681",
                             "Q7603",
                             "Q10513450",
                             "Q10513445",
                             "Q192897",
                             "Q23035974",
                             "Q18534390",
                             "Q18534393",
                             "Q6696468",
                             "Q334661",
                             "Q26737735",
                             "Q25428413",
                             "Q308915",
                             "Q26737738"]

        for license_QID in license_QID_list:
            license_local_ID = get_wbs_local_id(license_QID)
            licenseItem = WBItem(ID=license_local_ID)
            # Instance of: Software License
            licenseItem.add_statement("WD_P31","WD_Q207621")
            # Wikidata QID
            licenseItem.add_statement(wikidata_QID,license_QID)
            print(licenseItem.update())
