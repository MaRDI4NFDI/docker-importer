#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import configparser


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
    def __init__(self, label):
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

    def add_description(self, description):
        """Adds a description to the instantiated entity.

        Args:
            description (String): Description of the entity.

        Returns:
            WBEntity: Entity
        """
        self.description = description
        return self

    def add_statement(self, property, value, **qualifiers):
        """Adds a statement to the instantiated entity.

        A statement consists of a property and its value. 
        Additional qualifiers can be added to the statement. 

        Args:
            Property (String): Property ID in the local wikibase instance.
              Wikidata ID can also be used using the prefix *WD_*.
            Value: Value corresponding to the indicated property, in the
              corresponding format (i.e. string, time, wikibase-entity, ...)
            Qualifiers: Qualifiers to the statement. These must be introduced
              following the format *property=value*, where in turn the property
              is introduced using its ID, prefixed with *WD_* if necessary.

        Returns:
            WBEntity: Entity
        """
        if qualifiers:
            qualifiers = self.process_qualifiers(qualifiers)
        if property[0:3] == "WD_":
            property = get_wbs_local_id(property[3:])
        data_type = self.get_datatype(property)
        if data_type == "string" or data_type == "url":
            statement = {
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
            statement = {
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
            statement = {
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
            statement = {
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

        if qualifiers:
            statement["qualifiers"] = qualifiers

        self.claims.append(statement)
        return self

    def create(self):
        """Abstract method for creating entities in a local Wikibase.

        Method for creating Items or Properties is defined in the
        corresponding subclasses.
        """
        pass

    def update(self):
        """Updates an existing entity with additional statements.

        In order for the method to update the entity with a new statement, 
        this must have been previously added with :meth:`add_statement`.

        Returns:
            String: ID of the new entity
        """
        qid = self.exists()
        data = {}
        data["claims"] = self.claims
        return self.wb_connection.edit_entity(qid, data)

    def exists(self):
        """Abstract method for checking the existence of an entity.
        """
        pass

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
            data_type_qualifier = self.get_datatype(property_qualifier)
            if data_type_qualifier == "string":
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

    def get_datatype(self, property):
        """Returns the required data type for a given existing Wikibase property.

        Args:
            ID (String): Property ID

        Returns:
            String: Data type
        """
        params = {"action": "wbgetentities", "ids": property, "props": "datatype"}
        r1 = self.wb_connection.session.post(
            self.wb_connection.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if "entities" in r1.json.keys():
            if len(r1.json["entities"]) > 0:
                return r1.json["entities"][property]["datatype"]
        return None
