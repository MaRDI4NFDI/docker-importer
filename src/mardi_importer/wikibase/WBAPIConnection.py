#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json 
import configparser
import re

class WBAPIConnection:

    def __init__(self, username, botpwd, WIKIBASE_API):
        self.session = self.login(username, botpwd, WIKIBASE_API)
        self.WIKIBASE_API = WIKIBASE_API

    def login(self, username, botpwd, WIKIBASE_API):
        """
        Starts a new session and logins using a bot account.
        @username, @botpwd string: credentials of an existing bot user
        @returns requests.sessions.Session object
        """
        # create a new session
        session = requests.Session()

        # get login token
        r1 = session.get(WIKIBASE_API, params={
            'format': 'json',
            'action': 'query',
            'meta': 'tokens',
            'type': 'login'
        })
        # login with bot account
        r2 = session.post(WIKIBASE_API, data={
            'format': 'json',
            'action': 'login',
            'lgname': username,
            'lgpassword': botpwd,
            'lgtoken': r1.json()['query']['tokens']['logintoken'],
        })
        # raise when login failed
        if r2.json()['login']['result'] != 'Success':
            raise WBAPIException(r2.json()['login'])
            
        return session

    def get_csrf_token(self):
        """Gets a security (CSRF) token."""
        params1 = {
            "action": "query",
            "meta": "tokens",
            "type": "csrf"
        }
        r1 = self.session.get(self.WIKIBASE_API, params=params1)
        token = r1.json()['query']['tokens']['csrftoken']

        return token

    def create_entity(self, entity, data):
        """
        Creates a wikibase entity.
        @session requests.sessions.Session: session obtained from login 
        @data python dict: creation parameters of the entity
        @returns string: id of the new entity
        """
        token = self.get_csrf_token()
        
        params = {
            'action': 'wbeditentity',
            'format': 'json',
            'new': entity,
            'data': json.dumps(data),
            'token': token
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()
        
        if 'error' in r1.json.keys() and r1.json['error']['messages'][0]['name'] == 'wikibase-validator-label-with-description-conflict':
            item_error = re.findall('\[\[.*?\]\]',r1.json['error']['info'])
            item = re.findall('Q\d+',item_error[0])
            return item[0]
        elif 'error' in r1.json.keys() and r1.json['error']['messages'][0]['name'] == 'wikibase-validator-label-conflict':
            property_error = re.findall('\[\[.*?\]\]',r1.json['error']['info'])
            property = re.findall('P\d+',property_error[0])
            return property[0]
        # raise when edit failed
        if 'error' in r1.json.keys():
            raise WBAPIException(r1.json['error'])

        return r1.json['entity']['id']

    def edit_entity(self, qid, data):
        token = self.get_csrf_token()
        
        params = {
            'id': qid,
            'action': 'wbeditentity',
            'format': 'json',
            'data': json.dumps(data),
            'token': token
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()
        
        # raise when edit failed
        if 'error' in r1.json.keys():
            raise WBAPIException(r1.json['error'])

        return r1.json['entity']['id']

    def read_entity_by_title(self, entity_type, title):
        """Reads the Qid of an entity."""
        params = {
            'action': 'wbsearchentities',
            'search': title,
            'language': 'en',
            'type': entity_type,
            'limit': 10
        }
        r1 = self.session.post(self.WIKIBASE_API, data=params)
        r1.json = r1.json()
        if 'search' in r1.json.keys():
            if len(r1.json['search']) > 0:
                for matches in r1.json['search']:
                    if matches['label'] == title:
                        return matches['id']
        return None

class WBAPIException(BaseException):
    """Raised when the wikibase Open API throws an error"""
    pass