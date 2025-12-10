import re

from mardiclient import MardiClient
from dataclasses import dataclass, field
from habanero import Crossref
from httpx import HTTPStatusError
from typing import List, Optional

from mardi_importer import Importer
from mardi_importer.utils import Author

@dataclass
class CrossrefPublication():
    doi: str
    authors: List[Author] = field(default_factory=list)
    title: str = ""
    description: str = ""
    instance: str = ""
    journal: str = ""
    volume: str = ""
    issue: str = ""
    page: str = ""
    issn_print: str = ""
    issn_online: str = ""
    book: bool = False
    book_chapter: bool = False
    container_book: str = ""
    monograph: bool = False
    isbn: str = ""
    proceedings: str = ""
    proceedings_month: str = ""
    proceedings_year: str = ""
    posted: bool = False
    publisher: str = ""
    day: str = ""
    month: str = ""
    year: str = ""
    preprint: bool = False
    identical: str = ""
    QID: str = None
    api: Optional[MardiClient] = None

    def __post_init__(self):
        self.crossref_ok = False
        if self.api is None:
            self.api = Importer.get_api('crossref')
        item = self.api.item.new()
        item.labels.set(language="en", value=self.title)

        doi_id = "wdt:P356"
        QID_results = self.api.search_entity_by_value(doi_id, self.doi)
        if QID_results: self.QID = QID_results[0]

        if self.QID:
            # Get authors.
            item = self.api.item.get(self.QID)
            author_QID = item.get_value('wdt:P50')
            for QID in author_QID:
                author_item = self.api.item.get(entity_id=QID)
                name = str(author_item.labels.get('en'))
                orcid = author_item.get_value('wdt:P496')
                orcid = orcid[0] if orcid else None
                aliases = []
                if author_item.aliases.get('en'):
                    for alias in author_item.aliases.get('en'):
                        aliases.append(str(alias))
                author = Author(self.api,
                                name=name,
                                orcid=orcid,
                                _aliases=aliases,
                                _QID=QID)
                self.authors.append(author)
        else:
            try:
                cr = Crossref()
                response = cr.works(ids=self.doi)
            except HTTPStatusError as e:
                print(f"Publication with doi: {self.doi} not found in Crossref: {str(e)}")
                return None
            else:
                if response['status'] != 'ok':
                    return None
                self.crossref_ok = True
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
                    # ['component', 'report-series', 'standard', 'standard-series',
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
                                self.authors.append(Author(self.api, name=author_label, orcid=orcid_id))
                            else:
                                self.authors.append(Author(self.api, name=author_label))

                if 'relation' in metadata.keys():
                    if 'is-preprint-of' in metadata['relation'].keys():
                        self.preprint = True
                    if 'is-identical-to' in metadata['relation'].keys():
                        identical_obj = metadata['relation']['is-identical-to'][0]
                        if 'id' in identical_obj.keys():
                            self.identical = identical_obj['id']

    def create(self):
        if self.QID:
            return self.QID

        if not self.crossref_ok:
            print(f"Skipping creation, DOI {self.doi} not found in Crossref.")
            return None
        item = self.api.item.new()
        if self.title:
            item.labels.set(language="en", value=self.title)
            
            if self.description: 
                item.descriptions.set(
                    language="en", 
                    value=self.description
                )

            if self.instance:
                self.QID = item.is_instance_of_with_property(
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
                self.QID = item.is_instance_of_with_property(
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

            author_QID = self.__preprocess_authors()
            claims = []
            for author in author_QID:
                claims.append(self.api.get_claim("wdt:P50", author))
            item.add_claims(claims)
            
            if not self.QID:
                self.QID = item.write().id
        else:
            item.descriptions.set(
                language="en", 
                value="scientific article"
            )

            scholarly_article = 'wd:Q13442814'
            item.add_claim('wdt:P31', scholarly_article)
            item.add_claim('wdt:P356', self.doi)

            if not self.QID:
                self.QID = item.write().id


        if self.QID:
            print(f"Publication with DOI: {self.doi} created with ID {self.QID}.")
            return self.QID
        else:
            print(f"Publication with DOI: {self.doi} could not be created.")
            return None

    def __preprocess_authors(self) -> List[str]:
        """Processes the author information of each publication. 

        Create the author if it does not exist already as an 
        entity in wikibase.
            
        Returns:
          List[str]: 
            QIDs corresponding to each author.
        """
        author_QID = []
        for author in self.authors:
            if not author.QID:
                author.create()
            author_QID.append(author.QID)
        return author_QID

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