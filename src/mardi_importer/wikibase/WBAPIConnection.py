#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import configparser
import re


class WBAPIConnection:
    """Establishes a connection to Wikibase through the bot.

    *(The bot with the required permissions needs to be created in 
    advance)*.

    Each instance contains a session to create and edit entities
    in the local Wikibase instance, i.e. the MaRDI knowledge graph.

    Attributes:
        Session: Bot session in MediaWiki.
        WIKIBASE_API: URL to the Wikibase API.

    Args:
        username (String): Bot username
        botpwd (String): Bot password
        WIKIBASE_API (String): Wikibase instance url
    """
    def __init__(self, username, botpwd, WIKIBASE_API):
        self.session = self.login(username, botpwd, WIKIBASE_API)
        self.WIKIBASE_API = WIKIBASE_API

    def login(self, username, botpwd, WIKIBASE_API):
        """
        Starts a new session to MediaWiki and logins using a bot account.

        Args:
            username (String): Bot username
            botpwd (String): Bot password
            WIKIBASE_API (String): Wikibase instance url

        Returns:
            requests.sessions.Session: Bot session
        """
        # create a new session
        session = requests.Session()

        # get login token
        r1 = session.get(
            WIKIBASE_API,
            params={
                "format": "json",
                "action": "query",
                "meta": "tokens",
                "type": "login",
            },
        )
        # login with bot account
        r2 = session.post(
            WIKIBASE_API,
            data={
                "format": "json",
                "action": "login",
                "lgname": username,
                "lgpassword": botpwd,
                "lgtoken": r1.json()["query"]["tokens"]["logintoken"],
            },
        )
        # raise when login failed
        if r2.json()["login"]["result"] != "Success":
            raise WBAPIException(r2.json()["login"])

        return session

    def get_csrf_token(self):
        """Gets a security (CSRF) token through the API.
        
        Returns:
            CSRF token
            """
        params1 = {"action": "query", "meta": "tokens", "type": "csrf"}
        r1 = self.session.get(self.WIKIBASE_API, params=params1)
        token = r1.json()["query"]["tokens"]["csrftoken"]

        return token

    def create_entity(self, entity, data):
        """
        Creates a Wikibase entity with the given data.

        Args:
            Entity (string): Entity type (i.e. Item or Property)
            Data (dict): Parameters of the new entity

        Returns:
            String: ID of the new entity
        """
        token = self.get_csrf_token()

        params = {
            "action": "wbeditentity",
            "format": "json",
            "new": entity,
            "data": json.dumps(data),
            "token": token,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()

        if ("error" in r1.json.keys()):
            for message in r1.json["error"]["messages"]:
                if message['name'] == "wikibase-validator-label-with-description-conflict":
                    for parameter in message['parameters']:
                        item_error = re.findall("\[\[.*?\]\]", parameter)
                        if item_error:
                            item = re.findall("Q\d+", item_error[0])
                            return item[0]
                elif message['name'] == "wikibase-validator-label-conflict":
                    for parameter in message['parameters']:
                        property_error = re.findall("\[\[.*?\]\]", parameter)
                        if property_error:
                            property = re.findall("P\d+", property_error[0])
                            return property[0]

        # raise when edit failed
        if "error" in r1.json.keys():
            raise WBAPIException(r1.json["error"])

        return r1.json["entity"]["id"]

    def edit_entity(self, qid, data):
        """
        Edits a Wikibase entity with the given data.

        Args:
            qid (string): ID of the entity to be modified
            Data (dict): Parameters to be modified

        Returns:
            String: ID of the edited entity
        """
        token = self.get_csrf_token()

        params = {
            "id": qid,
            "action": "wbeditentity",
            "format": "json",
            "data": json.dumps(data),
            "token": token,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()

        # raise when edit failed
        if "error" in r1.json.keys():
            raise WBAPIException(r1.json["error"])

        return r1.json["entity"]["id"]

    def edit_claim(self, claim):
        token = self.get_csrf_token()
        params = {
            "action": "wbsetclaim",
            "claim": json.dumps(claim),
            "token": token,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()

        if "error" in r1.json.keys():
            raise WBAPIException(r1.json["error"])

    def remove_claim(self, claim_guid):
        token = self.get_csrf_token()
        params = {
            "action": "wbremoveclaims",
            "claim": claim_guid,
            "token": token,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()

        if "error" in r1.json.keys():
            raise WBAPIException(r1.json["error"])

        return claim_guid

    def create_qualifier(self, claim_guid, property, value):
        token = self.get_csrf_token()
        params = {
            "action": "wbsetqualifier",
            "claim": claim_guid,
            "property": property,
            "value": value,
            "snaktype": "somevalue",
            "token": token,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()

        if "error" in r1.json.keys():
            raise WBAPIException(r1.json["error"])

        return claim_guid

    def read_entity_by_title(self, entity_type, title):
        """Reads the ID of an entity, given its label.

        Args:
            entity_type (string): Entity type (i.e. Item or Property).
            title: Label of the entity.

        Returns:
            String: ID of the edited entity
        """
        params = {
            "action": "wbsearchentities",
            "search": title,
            "language": "en",
            "type": entity_type,
            "limit": 10,
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()
        if "search" in r1.json.keys():
            if len(r1.json["search"]) > 0:
                for matches in r1.json["search"]:
                    if matches["label"] == title:
                        return matches["id"]
        return None

    def get_datatype(self, property):
        """Returns the required data type for a given existing Wikibase property.

        Args:
            ID (String): Property ID

        Returns:
            String: Data type
        """
        params = {"action": "wbgetentities", "ids": property, "props": "datatype"}
        r1 = self.session.post(
            self.WIKIBASE_API, data=params
        )
        r1.json = r1.json()
        if "entities" in r1.json.keys():
            if len(r1.json["entities"]) > 0:
                return r1.json["entities"][property]["datatype"]
        return None


class WBAPIException(BaseException):
    """Raised when the Wikibase API throws an error"""

    pass
