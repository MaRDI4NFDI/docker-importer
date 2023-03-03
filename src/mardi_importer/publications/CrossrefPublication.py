#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import pandas as pd
import re

from habanero import Crossref
from requests.exceptions import HTTPError
from mardi_importer.publications.Author import Author
from wikibaseintegrator.wbi_enums import ActionIfExists

log = logging.getLogger('CRANlogger')

class CrossrefPublication:
    def __init__(self, integrator, doi, coauthors=[]):
        self.api = integrator
        self.doi = doi
        self.coauthors = coauthors
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
        self.day = ""
        self.month = ""
        self.year = ""
        self.__pull()

    def __pull(self):
        try:
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
                        title = metadata['title'][0]
                        groups = re.search("<([a-z]*)>(.*?)<\/\\1>", title)
                        while groups: 
                            title = title.replace(groups.group(0),groups.group(2))
                            groups = re.search("<([a-z]*)>(.*?)<\/\\1>", title)
                        title = " ".join(title.split())
                        self.title = title
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
                            author_label = f"{author['given'].title()} {author['family'].title()}"
                            if 'ORCID' in author.keys():
                                orcid_id = re.findall("\d{4}-\d{4}-\d{4}-.{4}", author['ORCID'])[0]
                                self.author[author_label] = orcid_id
                            else:
                                self.author[author_label] = None

                return self
        return None

    def create(self):
        item = self.api.item.new()
        if not self.title:
            publication_ID = None
        else:
            item.labels.set(language="en", value=self.title)
            if self.title:
                item.descriptions.set(
                    language="en", 
                    value="scientific article"
                )

            item.add_claim('wdt:P31','wd:Q13442814')
            item.add_claim('wdt:P356',self.doi)

            if len(self.title) > 0:
                if len(self.journal) > 0:
                    journal_id = self.__preprocess_journal()
                    item.add_claim('wdt:P1433', journal_id)
                    if len(self.volume) > 0:
                        item.add_claim('wdt:P478', self.volume)
                    if len(self.issue) > 0:
                        item.add_claim('wdt:P433', self.issue)
                    if len(self.page) > 0:
                        item.add_claim('wdt:P304', self.page)
                elif len(self.book) > 0:
                    book_id = self.__preprocess_book()
                    item.add_claim('wdt:P1433', book_id)
                elif len(self.proceedings) > 0:
                    proceedings_id = self.__preprocess_proceedings()
                    item.add_claim('wdt:P1433', proceedings_id)
                if len(self.day) > 0:
                    item.add_claim("wdt:P577", time=f"+{self.year}-{self.month}-{self.day}T00:00:00Z", precision=11)
                elif len(self.month) > 0:
                    item.add_claim("wdt:P577", time=f"+{self.year}-{self.month}-00T00:00:00Z", precision=10)
                elif len(self.year) > 0:
                    item.add_claim("wdt:P577", time=f"+{self.year}-00-00T00:00:00Z", precision=9)
                if len(self.author) > 0:
                    author_ID = self.__preprocess_authors()
                    claims = []
                    for author in author_ID:
                        claims.append(self.api.get_claim("wdt:P50", author))
                    item.add_claims(claims)

            publication_ID = item.write().id

        if publication_ID:
            log.info(f"Publication with DOI: {self.doi} created with ID {publication_ID}.")
            return publication_ID
        else:
            log.info(f"Publication with DOI: {self.doi} could not be created.")
            return None

    def __preprocess_authors(self):
        """Processes the author information of each Publication. This includes:

        - Searching if an author with the given ID already exists in the KG.
        - Alternatively, create items for new authors.
            
        Returns:
          List: 
            Wikidata QIDs corresponding to each author.
        """
        author_ID = []

        for name, orcid in self.author.items():
            author = Author(self.api, name, orcid, self.coauthors)
            author_qid = author.create()
            author_ID.append(author_qid)

        coauthors_ini = self.coauthors
        for author_qid in author_ID:
            if author_qid not in coauthors_ini:
                self.coauthors.append(author_qid)

        return author_ID

    def __preprocess_journal(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.journal)
        journal_id = item.is_instance_of('wd:Q5633421')
        if journal_id:
            return journal_id
        else:
            item.add_claim('wdt:P31', 'wd:Q5633421')
            claims = []
            if len(self.issn_print) > 0:
                qualifier = [self.api.get_claim('wdt:P437', 'wd:Q1261026')]
                claims.append(self.api.get_claim('wdt:P236', self.issn_print, qualifiers=qualifier))
            if len(self.issn_online) > 0:
                qualifier = [self.api.get_claim('wdt:P437', 'wd:Q1714118')]
                claims.append(self.api.get_claim('wdt:P236', self.issn_online, qualifiers=qualifier))
            return item.write().id

    def __preprocess_book(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.book)
        book_id = item.is_instance_of('wd:Q571')
        if book_id:
            return book_id
        else:
            item.add_claim('wdt:P31', 'wd:Q571')
            if len(self.isbn) == 13:
                item.add_claim('wdt:P212', self.isbn)
            elif len(self.isbn) == 10:
                item.add_claim('wdt:P957', self.isbn)
            return item.write().id

    def __preprocess_proceedings(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.proceedings)
        proceedings_id = item.is_instance_of('wd:Q1143604')
        if proceedings_id:
            return proceedings_id
        else:
            item.add_claim('wdt:P31', 'wd:Q1143604')
            if len(self.proceedings_month) > 0:
                item.add_claim("wdt:P577", time=f"+{self.proceedings_year}-{self.proceedings_month}-00T00:00:00Z")
            elif len(self.proceedings_year) > 0:
                item.add_claim("wdt:P577", time=f"+{self.proceedings_year}-00-00T00:00:00Z")
            return item.write().id
