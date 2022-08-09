
from mardi_importer.wikibase.WBItem import WBItem
from mardi_importer.wikibase.SPARQLUtils import SPARQL_exists
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import pandas as pd
import re

class Author:
    def __init__(self, name):
        self.name = name
        self.orcid = ""
    
    def add_orcid(self,orcid):
        self.orcid = orcid

    def sparql_exists(self):
        human = get_wbs_local_id("Q5")
        orcid_id_property = get_wbs_local_id("P496")
        return SPARQL_exists(human,orcid_id_property,self.orcid)

    def create(self):
        item = WBItem(self.name)
        item.add_statement("WD_P31", "WD_Q5")
        if len(self.orcid) > 0:
            item.add_statement("WD_P496", self.orcid)
        return item.create()

    def compare_names(self, author):
        if self.name == author:
            return self.name
        author_1_vec = self.name.split()
        author_2_vec = author.split()
        if author_1_vec[0] == author_2_vec[0]:
            if len(author_1_vec) > 2:
                if author_1_vec[2] == author_2_vec[1]:
                    return self.name
            if len(author_2_vec) > 2:
                if author_1_vec[1] == author_2_vec[2]:
                    return author
            if len(author_1_vec) > 2 and len(author_2_vec) > 2:
                if author_1_vec[0] == author_2_vec[0] and author_1_vec[2] == author_2_vec[2] and author_1_vec[1][0] == author_2_vec[1][0] and len(author_1_vec[1]) <= 2 and len(author_2_vec[1]) <= 2:
                    return f"{author_1_vec[0]} {author_1_vec[1][0]}. {author_1_vec[2]}"
        return None
