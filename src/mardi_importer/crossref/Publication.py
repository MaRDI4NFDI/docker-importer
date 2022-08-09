#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from habanero import Crossref
from requests.exceptions import HTTPError
from mardi_importer.wikibase.WBItem import WBItem
from mardi_importer.crossref.Author import Author
from mardi_importer.wikibase.WBMapping import get_wbs_local_id
import pandas as pd
import re
import logging
log = logging.getLogger('CRANlogger')

class Publication:
    def __init__(self, doi):
        self.doi = doi
        self.title = ""
        self.author = {}
        self.journal = ""
        self.volume = ""
        self.issue = ""
        self.page = ""
        self.issn_print = ""
        self.issn_online = ""
        self.book = ""
        self.isbn = ""
        self.proceedings = ""
        self.proceedings_month = ""
        self.proceedings_year = ""
        self.publisher = ""
        self.day = "00"
        self.month = "00"
        self.year = ""
        self.crossref = False
        self.related_authors = []

    def pull(self):
        try:
            self.crossref = True
            cr = Crossref()
            response = cr.works(ids=self.doi)
        except HTTPError as e:
            log.warning(f"Publication wit doi: {self.doi} not found in Crossref: {str(e)}")
            return None
        else:
            if response['status'] == 'ok':
                metadata = response['message']
                if 'title' in metadata.keys():
                    if len(metadata['title']) > 0:
                        self.title = metadata['title'][0]
                if 'type' in metadata.keys():
                    if metadata['type'] == 'journal-article':
                        if 'container-title' in metadata.keys():
                            if len(metadata['container-title']) > 0:
                                self.journal = metadata['container-title'][0]
                        if 'volume' in metadata.keys():
                            self.volume = metadata['volume']
                        if 'issue' in metadata.keys():
                            self.issue = metadata['issue']
                        if 'page' in metadata.keys():
                            self.page = metadata['page']
                        if 'issn-type' in metadata.keys():
                            for issn in metadata['issn-type']:
                                if issn['type'] == "print":
                                    self.issn_print = issn['value']
                                elif issn['type'] == "electronic":
                                    self.issn_online = issn['value']
                    elif metadata['type'] == 'book':  
                        if 'title' in metadata.keys():
                            if len(metadata['title']) > 0:
                                self.book = metadata['title'][0]
                        if 'ISBN' in metadata.keys():
                            if len(metadata['ISBN']) > 0:
                                self.isbn = metadata['ISBN'][0]
                    elif metadata['type'] == 'proceedings-article':
                        if 'container-title' in metadata.keys():
                            if len(metadata['container-title']) > 0:
                                self.proceedings = metadata['container-title'][0]
                        if 'created' in metadata.keys():
                            if 'date-parts' in metadata['created'].keys():
                                if len(metadata['created']['date-parts'][0]) > 1:
                                    self.proceedings_month = str(metadata['created']['date-parts'][0][1])
                                    if len(self.proceedings_month) == 1:
                                        self.proceedings_month = "0" + self.proceedings_month
                                if len(metadata['created']['date-parts'][0]) > 1:
                                    self.proceedings_year = str(metadata['created']['date-parts'][0][0])   

                if 'publisher' in metadata.keys():    
                    self.publisher = metadata['publisher']

                if 'published' in metadata.keys():
                    if 'date-parts' in metadata['published'].keys():
                        if len(metadata['published']['date-parts'][0]) > 2:
                            self.day = str(metadata['published']['date-parts'][0][2])
                            if len(self.day) == 1:
                                self.day = "0" + self.day
                        if len(metadata['published']['date-parts'][0]) > 1:
                            self.month = str(metadata['published']['date-parts'][0][1])
                            if len(self.month) == 1:
                                self.month = "0" + self.month
                        self.year = str(metadata['published']['date-parts'][0][0])

                if 'author' in metadata.keys():
                    for author in metadata['author']:
                        if 'given' in author.keys() and 'family' in author.keys():
                            author_label = f"{author['given']} {author['family']}"
                            if 'ORCID' in author.keys():
                                orcid_id = re.findall("\d{4}-\d{4}-\d{4}-.{4}", author['ORCID'])[0]
                                self.author[author_label] = orcid_id
                            else:
                                self.author[author_label] = None

                return self
        return None

    def create(self):
        if (self.crossref == False):
            self.pull()

        item = WBItem(self.title)
        item.add_statement('WD_P31','WD_Q591041')
        item.add_statement('WD_P356',self.doi)
        if len(self.title) > 0:
            if len(self.journal) > 0:
                journal_id = self.preprocess_journal()
                item.add_statement('WD_P1433', journal_id)
                if len(self.volume) > 0:
                    item.add_statement('WD_P478', self.volume)
                if len(self.issue) > 0:
                    item.add_statement('WD_P433', self.issue)
                if len(self.page) > 0:
                    item.add_statement('WD_P304', self.page)
            elif len(self.book) > 0:
                book_id = self.preprocess_book()
                item.add_statement('WD_P1433', book_id)
            elif len(self.proceedings) > 0:
                proceedings_id = self.preprocess_proceedings()
                item.add_statement('WD_P1433', proceedings_id)
            if len(self.day) > 0:
                item.add_statement("WD_P577", f"+{self.year}-{self.month}-{self.day}T00:00:00Z")
            elif len(self.month) > 0:
                item.add_statement("WD_P577", f"+{self.year}-{self.month}-00T00:00:00Z")
            elif len(self.year) > 0:
                item.add_statement("WD_P577", f"+{self.year}-00-00T00:00:00Z")
            if len(self.author) > 0:
                author_ID = self.preprocess_authors()
                for author in author_ID:
                    item.add_statement("WD_P50", author)                
        publication_ID = item.create()

        if publication_ID:
            log.info(f"Publication created with ID {publication_ID}.")
            return publication_ID
        else:
            log.info(f"Publication could not be created.")
            return None

    def add_related_authors(self, author_ID):
        for author_qid in author_ID:
            self.related_authors.append(author_qid)

    def preprocess_authors(self):
        """Processes the author information of each Publication. This includes:

        - Searching if an author with the given ID already exists in the KG.
        - Alternatively, create WB Items for new authors.
            
        Returns:
          List: 
            Item IDs corresponding to each author.
        """
        author_ID = []
        for author, orcid in self.author.items():
            author_qid = None
            human = get_wbs_local_id("Q5")
            orcid_id = get_wbs_local_id("P496")
            if orcid: 
                author_qid = WBItem(author).instance_property_exists(human, orcid_id, orcid)
            if not author_qid:
                author_qid = self.check_related_authors(author, orcid)
            if not author_qid:
                author_item = Author(author)
                if orcid:
                    author_item.add_orcid(orcid)
                author_qid = author_item.create()
                self.related_authors.append(author_qid)
            author_ID.append(author_qid)
        return author_ID

    def check_related_authors(self, author, orcid):
        for related_author_id in self.related_authors:
            related_author = WBItem(ID=related_author_id)
            related_author_name = related_author.get_label_by_ID()
            orcid_id = get_wbs_local_id("P496")
            related_author_orcid = related_author.get_value(orcid_id)
            if len(related_author_orcid) > 0:
                related_author_orcid = related_author_orcid[0]
            else:
                related_author_orcid = None
            if Author(related_author_name).compare_names(author):
                if not related_author_orcid and orcid:
                    related_author.update_label(Author(related_author_name).compare_names(author))
                    related_author.add_statement(orcid_id, orcid)
                    related_author.update()
                return related_author_id
            if orcid != None and related_author_orcid == orcid:
                return related_author_id
        return None

    def preprocess_journal(self):
        item = WBItem(self.journal)
        journal_id = item.instance_exists('WD_Q5633421')
        if journal_id:
            return journal_id
        else:
            item.add_statement('WD_P31', 'WD_Q5633421')
            if len(self.issn_print) > 0:
                item.add_statement('WD_P236', self.issn_print, WD_P437="WD_Q1261026")
            if len(self.issn_online) > 0:
                item.add_statement('WD_P236', self.issn_online, WD_P437="WD_Q1714118")
            return item.create()

    def preprocess_book(self):
        item = WBItem(self.book)
        book_id = item.instance_exists('WD_Q571')
        if book_id:
            return book_id
        else:
            item.add_statement('WD_P31', 'WD_Q571')
            if len(self.isbn) == 13:
                item.add_statement('WD_P212', self.isbn)
            elif len(self.isbn) == 10:
                item.add_statement('WD_P957', self.isbn)
            return item.create()

    def preprocess_proceedings(self):
        item = WBItem(self.proceedings)
        proceedings_id = item.instance_exists('WD_Q1143604')
        if proceedings_id:
            return proceedings_id
        else:
            item.add_statement('WD_P31', 'WD_Q1143604')
            if len(self.proceedings_month) > 0:
                item.add_statement("WD_P577", f"+{self.proceedings_year}-{self.proceedings_month}-00T00:00:00Z")
            elif len(self.proceedings_year) > 0:
                item.add_statement("WD_P577", f"+{self.proceedings_year}-00-00T00:00:00Z")
            return item.create()
