import logging
import pandas as pd
import re

from habanero import Crossref
from requests.exceptions import HTTPError
from .Author import Author
from wikibaseintegrator.wbi_enums import ActionIfExists

log = logging.getLogger('CRANlogger')

class CrossrefPublication:
    def __init__(self, integrator, doi, coauthors=[]):
        self.api = integrator
        self.doi = doi
        self.coauthors = coauthors
        self.title = ""
        self.description = ""
        self.instance = ""
        self.author = {}
        self.journal = ""
        self.volume = ""
        self.issue = ""
        self.page = ""
        self.issn_print = ""
        self.issn_online = ""
        self.book = False
        self.book_chapter = False
        self.container_book = ""
        self.monograph = False
        self.isbn = ""
        self.proceedings = ""
        self.proceedings_month = ""
        self.proceedings_year = ""
        self.posted = False
        self.publisher = ""
        self.day = ""
        self.month = ""
        self.year = ""
        self.preprint = False
        self.identical = ""
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
                        self.instance = 'wd:Q13442814'
                        self.description = 'scientific article'
                        if 'relation' in metadata.keys():
                            if 'is-preprint-of' in metadata['relation'].keys():
                                self.description += " preprint"
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
                        self.instance = 'wd:Q571'
                        self.description = 'academic book'
                        self.book = True
                        if 'ISBN' in metadata.keys():
                            if len(metadata['ISBN']) > 0:
                                self.isbn = metadata['ISBN'][0]
                    elif metadata['type'] == 'monograph': 
                        self.instance = 'wd:Q193495'
                        self.description = 'scholarly monograph'
                        self.monograph = True
                        if 'ISBN' in metadata.keys():
                            if len(metadata['ISBN']) > 0:
                                self.isbn = metadata['ISBN'][0]
                    elif metadata['type'] == 'posted-content':
                        if 'subtype' in metadata.keys():
                            if metadata['subtype'] == 'preprint':
                                self.posted = True
                                self.instance = 'wd:Q13442814'
                                self.description = 'scientific article preprint'
                                self.preprint = True
                    elif metadata['type'] == 'proceedings-article':
                        self.instance = 'wd:Q23927052'
                        self.description = 'proceedings article'
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
                    elif metadata['type'] == 'book-chapter':
                        self.instance = 'wd:Q1980247'
                        self.description = 'book chapter'
                        self.book_chapter = True
                        if 'container-title' in metadata.keys():
                            if 'ISBN' in metadata.keys():
                                if len(metadata['ISBN']) > 0:
                                    self.isbn = metadata['ISBN'][0]
                            if len(metadata['container-title']) > 0:
                                book_title = metadata['container-title'][0]
                                self.container_book = self.__preprocess_book(book_title)
                    elif metadata['type'] == 'journal-issue':
                        self.instance = 'wd:Q28869365'
                        self.description = 'journal issue'
                    elif metadata['type'] == 'journal-volume':
                        self.instance = 'wd:Q1238720'
                        self.description = 'journal volume'
                    elif metadata['type'] == 'journal':
                        self.instance = 'wd:Q5633421'
                        self.description = 'scientific journal'
                    elif metadata['type'] == 'proceedings':
                        self.instance = 'wd:Q1143604'
                        self.description = 'conference proceedings'
                    elif metadata['type'] == 'dataset':
                        self.instance = 'wd:Q1172284'
                        self.description = 'dataset'
                    elif metadata['type'] == 'report':
                        self.instance = 'wd:Q10870555'
                        self.description = 'report'                    
                    elif metadata['type'] == 'edited-book':
                        self.instance = 'wd:Q571'
                        self.description = 'academic book'
                    elif metadata['type'] == 'reference-book':
                        self.instance = 'wd:Q571'
                        self.description = 'academic book'
                    elif metadata['type'] == 'book-series':
                        self.instance = 'wd:Q277759'
                        self.description = 'book series'
                    elif metadata['type'] == 'book-set':
                        self.instance = 'wd:Q28062188'
                        self.description = 'book set'
                    elif metadata['type'] == 'book-section':
                        self.instance = 'wd:Q1931107'
                        self.description = 'book section'
                    elif metadata['type'] == 'dissertation':
                        self.instance = 'wd:Q1385450'
                        self.description = 'dissertation'
                    # The following types are not associated with an instance or description
                    #['component', 'report-series', 'standard', 'standard-series',
                    # 'book-part', 'book-track', 'reference-entry', 'other', 'peer-review']

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
                        if self.year and self.book:
                            self.description += f" ({self.year})" 

                if 'author' in metadata.keys():
                    for author in metadata['author']:
                        if 'given' in author.keys() and 'family' in author.keys():
                            author_label = f"{author['given'].title()} {author['family'].title()}"
                            if 'ORCID' in author.keys():
                                orcid_id = re.findall("\d{4}-\d{4}-\d{4}-.{4}", author['ORCID'])[0]
                                self.author[author_label] = orcid_id
                            else:
                                self.author[author_label] = None

                if 'relation' in metadata.keys():
                    if 'is-preprint-of' in metadata['relation'].keys():
                        self.preprint = True
                    if 'is-identical-to' in metadata['relation'].keys():
                        identical_obj = metadata['relation']['is-identical-to'][0]
                        if 'id' in identical_obj.keys():
                            self.identical = identical_obj['id']

                return self
        return None

    def create(self):
        item = self.api.item.new()
        publication_ID = None
        if self.title:
            item.labels.set(language="en", value=self.title)
            
            if self.description: 
                item.descriptions.set(
                    language="en", 
                    value=self.description
                )

            if self.instance:
                publication_ID = item.is_instance_of_with_property(
                                self.instance, 
                                "wdt:P356",
                                self.doi
                            )
                item.add_claim('wdt:P31', self.instance)

            if self.identical:
                # Check if an identical crossref publication already exists
                existing_item = item.is_instance_of_with_property(
                                    self.instance, 
                                    "wdt:P356",
                                    self.identical
                                )
                if existing_item: return existing_item

            item.add_claim('wdt:P356', self.doi)

            if self.journal:
                journal_id = self.__preprocess_journal()
                item.add_claim('wdt:P1433', journal_id)
                if len(self.volume) > 0:
                    item.add_claim('wdt:P478', self.volume)
                if len(self.issue) > 0:
                    item.add_claim('wdt:P433', self.issue)
                if len(self.page) > 0:
                    item.add_claim('wdt:P304', self.page)
            elif (self.book or self.monograph) and self.isbn:
                if len(self.isbn) == 13:
                    isbn_prop_nr = 'wdt:P212'
                elif len(self.isbn) == 10:
                    isbn_prop_nr = 'wdt:P957'
                item.add_claim(isbn_prop_nr, self.isbn)
                publication_ID = item.is_instance_of_with_property(
                                self.instance, 
                                isbn_prop_nr,
                                self.isbn
                            )
            elif self.book_chapter:
                if len(self.container_book) > 0:
                    item.add_claim('wdt:P1433', self.container_book)
            elif self.proceedings:
                proceedings_id = self.__preprocess_proceedings()
                item.add_claim('wdt:P1433', proceedings_id)

            if len(self.day) > 0:
                item.add_claim("wdt:P577", f"+{self.year}-{self.month}-{self.day}T00:00:00Z", precision=11)
            elif len(self.month) > 0:
                item.add_claim("wdt:P577", f"+{self.year}-{self.month}-00T00:00:00Z", precision=10)
            elif len(self.year) > 0:
                item.add_claim("wdt:P577", f"+{self.year}-00-00T00:00:00Z", precision=9)

            if len(self.author) > 0:
                author_ID = self.__preprocess_authors()
                claims = []
                for author in author_ID:
                    claims.append(self.api.get_claim("wdt:P50", author))
                item.add_claims(claims)
            
            if not publication_ID:
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
        item.descriptions.set(
                language="en", 
                value="scientific journal"
            )
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

    def __preprocess_book(self, container_book):
        item = self.api.item.new()
        item.labels.set(language="en", value=container_book)
        item.descriptions.set(
                language="en", 
                value="academic book"
            )
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
                item.add_claim("wdt:P577", f"+{self.proceedings_year}-{self.proceedings_month}-00T00:00:00Z")
            elif len(self.proceedings_year) > 0:
                item.add_claim("wdt:P577", f"+{self.proceedings_year}-00-00T00:00:00Z")
            return item.write().id
