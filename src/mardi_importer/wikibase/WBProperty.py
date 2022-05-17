#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBEntity import WBEntity

class WBProperty(WBEntity):
    def __init__(self,label):
        self.datatype = ""
        super(WBProperty,self).__init__(label)

    def create(self):
        data = {'labels':{'en':{'language':'en','value':self.label}}}
        if len(self.description) > 0:
            data['descriptions'] = {'en':{'language':'en','value':self.description}}
        data['datatype'] = self.datatype
        data['claims'] = self.claims
        return self.wb_connection.create_entity('property',data)

    def exists(self):
        return self.wb_connection.read_entity_by_title('property', self.label)

    def add_datatype(self,datatype):
        self.datatype = datatype
        return self
