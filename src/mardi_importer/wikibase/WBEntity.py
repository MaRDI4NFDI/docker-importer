#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection, WBAPIException
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import configparser
import logging
log = logging.getLogger('CRANlogger')

class WBEntity:
    """
    Wikibase parent class to provide base functionaliy to 
    :class:`mardi_importer.wikibase.WBItem` and
    :class:`mardi_importer.wikibase.WBProperty`. 
    Each entity must be instantiated with a label.

    Establishes a connection to the local Wikibase instance using
    :class:`mardi_importer.wikibase.WBAPIConnection`.

    Attributes:
        label (String): Entity label.
        description (String): Entity description.
        claims (List): Statements of the instantiated entity, i.e. 
            list of properties and values describing the entity.
        wb_connection: Connection to the Wikibase API.
    """
    def __init__(self, label=None, *, id=None):
        self.label = label
        self.description = ""
        self.claims = []

        config = configparser.ConfigParser()
        config.sections()
        config.read("/config/credentials.ini")
        username = config["default"]["username"]
        botpwd = config["default"]["password"]
        WIKIBASE_API = config["default"]["WIKIBASE_API"]
        self.wb_connection = WBAPIConnection(username, botpwd, WIKIBASE_API)
        self.id = self.get_ID(id)

    def add_description(self, description):
        """Adds a description to the instantiated entity.

        Args:
            description (String): Description of the entity

        Returns:
            WBEntity: Entity
        """
        self.description = description
        return self

    def get_description(self):
        """Returns the description for the instantiated entity.
        
        If a description is provided for the instantiated entity it is
        returned by the Wikibase API, otherwise **None** is returned.

        Returns:
          String: Entity description
        """
        params = {"action": "wbgetentities", "ids": self.id}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if "entities" in r1.json.keys():
            if len(r1.json["entities"][self.id]["descriptions"]) > 0:
                return r1.json["entities"][self.id]["descriptions"]["en"]["value"]
        return None

    def add_statement(self, property, value, **qualifiers):
        """Adds a statement to the instantiated entity.

        A statement consists of a property and its value. 
        Additional qualifiers can be added to the statement. 

        Args:
            Property (String): Property ID in the local wikibase instance.
              Wikidata ID can also be used using the prefix *WD_*.
            Value (String): Value corresponding to the indicated property, in the
              corresponding format (i.e. string, time, wikibase-entity, ...)
            Qualifiers: Qualifiers to the statement. These must be introduced
              following the format *property=value*, where in turn the property
              is introduced using its ID, prefixed with *WD_* if necessary.

        Returns:
            WBEntity: Entity
        """
        statement = self.return_statement(property, value)

        if qualifiers:
            qualifiers = self.process_qualifiers(qualifiers)
            statement["qualifiers"] = qualifiers

        if not self.statement_exists(property,value):
            self.claims.append(statement)

        return self

    def return_statement(self, property, value):
        """Creates the dictionary to insert a statement through the Wikibase API.

        The nomenclature of the returned dictionary depends on the data type of 
        the specific property that is inserted.
        
        Args:
          Property (String): Property to be inserted. The property has to be 
            defined using its property ID and with the prefix *WD_* if the ID 
            refers to the Wikidata ID.
          Value (String): The format of the value has to be consistent with the 
            data type of the inserted property. Supported formats in this method 
            are **string**, **url**, **wikibase-item**, **time** and **external-id**.

        Returns:
          Dict: 
            Dictionary containing the property and value to be
            inserted following the required *mainsnak* nomenclature
            for Wikibase.
        """
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        try:
            data_type = self.wb_connection.get_datatype(property)
        except WBAPIException as e:
            data_type = None
            log.error(f"Property not found: {str(e)}")
        if data_type == "string" or data_type == "url":
            return {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {"type": "string", "value": value},
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "wikibase-item":
            if value[0:3] == "WD_":
                value = get_wbs_local_id(value[3:])
            return {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {"entity-type": "item", "id": value},
                    },
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "time":
            return {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "time",
                        "value": {
                            "time": value,
                            "precision": 11,
                            "timezone": 0,
                            "before": 0,
                            "after": 0,
                            "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                        },
                    },
                },
                "type": "statement",
                "rank": "normal",
            }
        elif data_type == "external-id":
            return {
                "mainsnak": {
                    "snaktype": "value",
                    "property": property,
                    "datavalue": {
                        "type": "string",
                        "value": value,
                    },
                },
                "rank": "normal",
                "type": "statement",
            }
        return None
    
    def statement_exists(self, property, value):
        """Checks if the statement already exists in the instantiated entity.

        Args:
          Property (String): Property of the statement to be checked. The 
            property has to be defined using its property ID and with the 
            prefix *WD_* if the ID refers to the Wikidata ID.
          Value (String): The format of the value has to be consistent with the 
            data type of the inserted property. Supported formats in this method 
            are **string**, **url**, **wikibase-item**, **time** and **external-id**.

        Returns:
          Boolean:
            **True** if the statement already exists, **False** otherwise.
        """
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        values = self.get_value(property)
        if value[0:3] == "WD_":
            value = get_wbs_local_id(value[3:])
        for value_item in values:
            if value_item == value:
                return True
        return False

    def exists(self):
        """Abstract method for checking the existence of an entity.

        Specific methods are defined for *Items* and *Properties*.
        """
        pass

    def create(self):
        """Abstract method for creating entities in a local Wikibase.

        Method for creating Items or Properties is defined in the
        corresponding subclasses.
        """
        pass

    def update(self):
        """Updates an existing entity with description or additional statements.

        In order for the method to update the entity with a new statement, 
        this must have been previously added with :meth:`add_statement`.

        Returns:
            String: ID of the new entity
        """
        data = {}
        if len(self.description) > 0:
            data["descriptions"] = {"en": {"language": "en", "value": self.description}}
        if len(self.claims) > 0:
            data["claims"] = self.claims
        try:
            return self.wb_connection.edit_entity(self.id, data)
        except WBAPIException as e:
            log.error(f"Package update failed: {str(e)}")

    def remove_claim(self,claim_guid):
        """
        Removes a specific claim from the instantiated entity.

        Args:
          claim_guid (String):
            GUID corresponding to the claim to be deleted.
        """
        if type(claim_guid) is not list:
            claim_guid = [claim_guid]
        for guid in claim_guid:
            try:
                self.wb_connection.remove_claim(guid)
            except WBAPIException as e:
                log.error(f"Claim {claim_guid} could not be removed: {str(e)}")

    def update_claim(self, claim_guid, statement):
        """
        Change the information of a specific claim from the instantiated entity.

        Args:
          claim_guid (String):
            GUID corresponding to the claim to be deleted.
          statement (Dict):
            Dictionary following the *mainsnak* nomenclature that described
            the updated claim.
        """
        claim = {"id":claim_guid,
                 "type":"claim"}
        claim['mainsnak'] = statement['mainsnak']
        try:
            return self.wb_connection.edit_claim(claim)
        except WBAPIException as e:
            log.error(f"Claim {claim_guid} could not be edited: {str(e)}")

    def get_claim_guid(self, property):
        """Returns the claim GUID corresponding to a given property.

        Given a property ID, a list is returned with all the GUIDs corresponding
        to the claims for that property in the instantiated entity.

        Args:
          property (String): Property for which the claims GUID must be returned.
            The property must be introduced as a string given its ID. Wikidata IDs
            can be introduced using the prefix *WD_*.

        Returns:
          List:
            List of GUIDs corresponding to each claim in the given property.
        """
        params = {"action": "wbgetentities", "ids": self.id, "props": "claims"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        #data_type = self.wb_connection.get_datatype(property)
        values = []
        if "entities" in r1.json.keys():
            if property in r1.json["entities"][self.id]["claims"].keys():
                for statement in r1.json["entities"][self.id]["claims"][property]:
                    values.append(statement['id'])
        return values

    def get_ID(self, id):
        """Returns the ID of the instantiated entity through an SQL query to the 
        Wikibase tables.

        The method is called during the instantiation of an entity. If the entity
        is declared using a label, the method returns the ID for that entity, if
        it already exists. If the entity is declared already with an ID, the label
        corresponding to that ID is assigned to the corresponding attribute of the
        instantiated entity.

        Args:
          id (String): Optional

        Return:
          String:
            Entity ID
        """
        if id:
            self.id = id
            self.label = self.get_label_by_ID()
        else:
            self.id = self.SQL_exists()
        return self.id

    def get_label_by_ID(self):
        """Returns the label corresponding to a given entity ID.

        Returns:
          String:
            Entity label
        """
        params = {
            'action': 'wbsearchentities',
            'search': self.id,
            'language': 'en',
            'type': 'item',
            'limit': 1
        }
        r1 = self.wb_connection.session.post(self.wb_connection.WIKIBASE_API, data=params)
        r1.json = r1.json()
        if 'search' in r1.json.keys():
            if len(r1.json['search']) > 0:
                for matches in r1.json['search']:
                    return matches['label']
        return None

    def get_value(self, property):
        """Returns the value corresponding to the given property.

        Args:
          Property (String): Property for which the value must be returned.
            The property must be introduced as a string given its ID. Wikidata IDs
            can be introduced using the prefix *WD_*.
        
        Returns:
          String:
            Value assigned to the introduced property in the instantiated entity.
        """
        params = {"action": "wbgetentities", "ids": self.id, "props": "claims"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        try:
            data_type = self.wb_connection.get_datatype(property)
        except WBAPIException as e:
            data_type = None
            log.error(f"Property not found: {str(e)}")
        values = []
        if "entities" in r1.json.keys():
            if property in r1.json["entities"][self.id]["claims"].keys():
                for statement in r1.json["entities"][self.id]["claims"][property]:
                    if data_type == "string" or data_type == "url" or data_type == "external-id":
                        values.append(statement['mainsnak']['datavalue']['value'])
                    elif data_type == "wikibase-item":
                        value_id = statement['mainsnak']['datavalue']['value']['id']
                        values.append(value_id)
                    elif data_type == "time":
                        values.append(statement['mainsnak']['datavalue']['value']['time'][1:11])
        return values

    def process_qualifiers(self, qualifiers):
        """Processes the qualifiers that can optionally be added to each
        statement.

        Qualifiers are passed using the nomenclature *property=value*. 
        Properties are indicated using the ID in the local wikibase. The prefix
        *WD_* can be used to indicate that the ID refers to the Wikidata instance.

        Returns:
            dict: Qualifier dictionary following the Wikibase API nomenclature.
        """
        qualifier_dict = {}
        for key, value in qualifiers.items():
            if key[0:3] == "WD_":
                property_qualifier = get_wbs_local_id(key[3:])
            else:
                property_qualifier = key
            try:
                data_type_qualifier = self.wb_connection.get_datatype(property_qualifier)
            except WBAPIException as e:
                data_type_qualifier = None
                log.error(f"Property not found: {str(e)}")
            if data_type_qualifier == "string" or data_type_qualifier == "url":
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "string",
                        "datavalue": {"type": "string", "value": value},
                    }
                ]
            elif data_type_qualifier == "wikibase-item":
                if value[0:3] == "WD_":
                    value = get_wbs_local_id(value[3:])
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "wikibase-item",
                        "datavalue": {
                            "type": "wikibase-entityid",
                            "value": {"entity-type": "item", "id": value},
                        },
                    }
                ]
            elif data_type_qualifier == "time":
                qualifier_dict[property_qualifier] = [
                    {
                        "snaktype": "value",
                        "property": property_qualifier,
                        "datatype": "time",
                        "datavalue": {
                            "value": {
                                "time": value,
                                "timezone": 0,
                                "before": 0,
                                "after": 0,
                                "precision": 11,
                                "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                            },
                            "type": "time",
                        },
                    }
                ]
        return qualifier_dict
