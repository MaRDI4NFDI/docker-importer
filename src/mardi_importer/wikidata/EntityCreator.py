#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:50:55 2022

@author: alvaro
"""
from mardi_importer.importer.Importer import AEntityCreator, ImporterException
from mardi_importer.wikibase.WBItem import WBItem
from mardi_importer.wikibase.WBProperty import WBProperty
import pandas as pd
import os
import mysql.connector as connection


class EntityCreator(AEntityCreator):
    """
    Creates entities in a local Wikibase by copying them from Wikidata.
    see: https://github.com/MaRDI4NFDI/portal-examples/blob/main/Import%20from%20zbMath/WB_wikidata_properties.ipynb
    """

    mapping_df = None  # data frame of entity id mappings Wikidata -> local wikibase

    def __init__(self, path):
        """
        Args:
            path (str): file with properties to import
        """
        self.entity_list = path

    def import_entities(self):
        """
        Overrides abstract method.
        This method calls WikibaseImport extension to:

        - Import the entities from Wikidata into the Wikibase
        - Map the entity ids from Wikidata into ids in the local Wikibase
        - Store the mappings in the local wiki database

        A prerequisite is that this container can read the extension folder on the local Wikibase
        therefore, the /var/www/html folder from the local Wikibase has to be shared. This is set in docker-compose.yml

        - Wikibase:/var/www/html is mounted read-only on /shared
        - The file with properties to import (@arg path) is copied to the Dockerfile in /config

        Returns:
            pandas dataframe
        """
        # call WikibaseImport from the Wikibase container to import the properties from Wikidata
        command = "php /var/www/html/extensions/WikibaseImport/maintenance/importEntities.php --file {} --do-not-recurse --conf /shared/LocalSettings.php".format(
            self.entity_list
        )
        return_code = os.system(command)
        if return_code != 0:
            raise ImporterException(
                "Error attempting to import {}".format(self.entity_list)
            )

        # get the DB connection settings passed in docker-compose
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"]
        db_host = os.environ["DB_HOST"]

        # read the mappings table from the wiki database
        mydb = connection.connect(
            host=db_host, database=db_name, user=db_user, passwd=db_pass, use_pure=True
        )
        try:
            query = "Select * from wbs_entity_mapping;"
            mapping_df = pd.read_sql(query, mydb)
            # DB returns bytearrays, decode to utf-8
            for col in mapping_df.columns:
                mapping_df[col] = mapping_df[col].apply(lambda x: x.decode("utf-8"))
        except Exception as e:
            raise ImporterException(
                "Error attempting to read mappings from database\n{}".format(e)
            )
        finally:
            mydb.close()

        return mapping_df

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
            "Identifier in Wikidata of the corresponding properties"
        )
        property.add_statement("WD_P1630", "https://www.wikidata.org/wiki/Property:$1")
        wikidata_QID = property.create()

        # Version
        property = WBProperty("License version")
        property.add_datatype("string")
        property.add_description("License version identifier")
        license_version = property.create()

        # item = WBItem('MIT')
        # item.add_statement(wikidata_QID,"Q334661")
        # item.update()
        # print(item.exists())
