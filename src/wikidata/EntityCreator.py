#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:50:55 2022

@author: alvaro
"""
from importer.Importer import AEntityCreator, ImporterException
import pandas as pd
import os
import mysql.connector as connection

class EntityCreator(AEntityCreator):
    """
    Creates entities in a local Wikibase by copying them from Wikidata.
    @see: https://github.com/MaRDI4NFDI/portal-examples/blob/main/Import%20from%20zbMath/WB_wikidata_properties.ipynb
    """
    mapping_df = None # data frame of entity id mappings Wikidata -> local wikibase
    
    def __init__(self, path):
        """
        @arg path: file with properties to import
        """
        self.entity_list = path
    
    def create_entities(self):
        """
        Overrides abstract method.
        This method calls WikibaseImport extention to:
            * Import the entities from Wikidata into the Wikibase
            * Map the entity ids from Wikidata into ids in the local Wikibase
            * Store the mappings in the local wiki database
        A prerequisite is that this container can read the extension folder on the local Wikibase
        therefore, the /var/www/html folder from the local Wikibase has to be shared. This is set in docker-compose.yml
            * Wikibase:/var/www/html is mounted read-only on /shared
            * The file with properties to import (@arg path) is copied to the Dockerfile in /config
        @returns: pandas dataframe
        """
        # call WikibaseImport from the Wikibase container to import the properties from Wikidata
        command = "php /shared/extensions/WikibaseImport/maintenance/importEntities.php --file {} --do-not-recurse".format(self.entity_list)
        return_code = os.system(command)
        if return_code != 0:
            raise ImporterException( 'Error attempting to import {}'.format(path) )

        # get the DB connection settings passed in docker-compose        
        db_user = os.environ['DB_USER']
        db_pass = os.environ['DB_PASS']
        db_name = os.environ['DB_NAME']
        db_host = os.environ['DB_HOST']
        
        # read the mappings table from the wiki database
        mydb = connection.connect(host=db_host, database=db_name, user=db_user, passwd=db_pass, use_pure=True)
        try:
            query = "Select * from wbs_entity_mapping;"
            mapping_df = pd.read_sql(query,mydb)
            # DB returns bytearrays, decode to utf-8
            for col in mapping_df.columns:
                mapping_df[col] = mapping_df[col].apply(lambda x: x.decode("utf-8"))
        except Exception as e:
            raise ImporterException( 'Error attempting to read mappings from database\n{}'.format(e) )
        finally:
            mydb.close()

        return mapping_df
    