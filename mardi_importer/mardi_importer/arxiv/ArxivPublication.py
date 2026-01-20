import feedparser
import requests
import re
import logging

from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from feedparser.util import FeedParserDict
from typing import List, Optional

from mardiclient import MardiClient
from mardi_importer import Importer
from mardi_importer.utils import Author


def get_logger_safe(name: str = __name__) -> logging.Logger:
    try:
        from prefect.logging import get_run_logger
        from prefect.exceptions import MissingContextError

        try:
            return get_run_logger()
        except MissingContextError:
            return logging.getLogger(name)
    except ModuleNotFoundError:
        return logging.getLogger(name)


taxonomy = ["cs.AI", "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CL", "cs.CR", \
            "cs.CV", "cs.CY", "cs.DB", "cs.DC", "cs.DL", "cs.DM", "cs.DS", \
            "cs.ET", "cs.FL", "cs.GL", "cs.GR", "cs.GT", "cs.HC", "cs.IR", \
            "cs.IT", "cs.LG", "cs.LO", "cs.MA", "cs.MM", "cs.MS", "cs.NA", \
            "cs.NE", "cs.NI", "cs.OH", "cs.OS", "cs.PF", "cs.PL", "cs.RO", \
            "cs.SC", "cs.SD", "cs.SE", "cs.SI", "cs.SY", "econ.EM",\
            "econ.GN", "econ.TH", "eess.AS", "eess.IV", "eess.SP", \
            "eess.SY", "math.AC", "math.AG", "math.AP", "math.AT", \
            "math.CA", "math.CO", "math.CT", "math.CV", "math.DG", \
            "math.DS", "math.FA", "math.GM", "math.GN", "math.GR", \
            "math.GT", "math.HO", "math.IT", "math.KT", "math.LO", \
            "math.MG", "math.MP", "math.NA", "math.NT", "math.OA", \
            "math.OC", "math.PR", "math.QA", "math.RA", "math.RT", \
            "math.SG", "math.SP", "math.ST", "astro-ph.CO", "astro-ph.EP", \
            "astro-ph.GA", "astro-ph.HE", "astro-ph.IM", "astro-ph.SR", \
            "cond-mat.dis-nn", "cond-mat.mes-hall", "cond-mat.mtrl-sci", \
            "cond-mat.other", "cond-mat.quant-gas", "cond-mat.soft", \
            "cond-mat.stat-mech", "cond-mat.str-el", "cond-mat.supr-con", \
            "gr-qc", "hep-ex", "hep-lat", "hep-ph", "hep-th", "math-ph", \
            "nlin.AO", "nlin.CD", "nlin.CG", "nlin.PS", "nlin.SI", \
            "nucl-ex", "nucl-th", "physics.acc-ph", "physics.ao-ph", \
            "physics.app-ph", "physics.atm-clus", "physics.atom-ph", \
            "physics.bio-ph", "physics.chem-ph", "physics.class-ph", \
            "physics.comp-ph", "physics.data-an", "physics.ed-ph", \
            "physics.flu-dyn", "physics.gen-ph", "physics.geo-ph", \
            "physics.hist-ph", "physics.ins-det", "physics.med-ph", \
            "physics.optics", "physics.plasm-ph", "physics.pop-ph", \
            "physics.soc-ph", "physics.space-ph", "quant-ph", "q-bio.BM", \
            "q-bio.CB", "q-bio.GN", "q-bio.MN", "q-bio.NC", "q-bio.OT", \
            "q-bio.PE", "q-bio.QM", "q-bio.SC", "q-bio.TO", "q-fin.CP", \
            "q-fin.EC", "q-fin.GN", "q-fin.MF", "q-fin.PM", "q-fin.PR", \
            "q-fin.RM", "q-fin.ST", "q-fin.TR", "stat.AP", "stat.CO", \
            "stat.ME", "stat.ML", "stat.OT", "stat.TH"]

@dataclass
class Arxiv():
    arxiv_id: str
    _title: str = None
    _abstract: str = None
    _publication_date: str = None
    _authors: List[Author] = field(default_factory=list)
    _arxiv_classification: List[str] = field(default_factory=list)
    entry: FeedParserDict = None
    api: Optional[MardiClient] = None

    def __post_init__(self) -> None:
        self.entry = self.arxiv_api(self.arxiv_id)
        if self.api is None:
            self.api = Importer.get_api('arxiv')

    @property
    def title(self) -> str:
        """Get the title of the entry.

        Returns:
            str: The title of the entry.
        """
        if not self._title:
            title = self.entry.title
            self._title = title.replace('\n', ' ')
        return self._title

    @property
    def abstract(self) -> str:
        """Get the abstract of the entry.

        Returns:
            str: The abtract of the entry.
        """
        if not self._abstract:
            abstract = self.entry.summary
            self._abstract = abstract.replace('\n', ' ')
        return self._abstract

    @property
    def publication_date(self) -> str:
        """Get the publication date

        Returns:
            str: Publication date in format +YYYY-MM-DDT00:00:00Z
        """
        if not self._publication_date:
            time = self.entry.published
            if not time.startswith("+"):
                time = "+" + time
            time = time[0:11] + "T00:00:00Z"
            self._publication_date = time
        return self._publication_date

    @property
    def authors(self) -> List[Author]:
        """Get the list of authors for the entry

        Returns:
            List[Author]: 
                The list of authors for the entry, which can include
                an arXiv author ID, if found
        """
        if not self._authors:
            for author in self.entry.authors:
                author = self.disambiguate_autor(author.name)
                self._authors.append(author)
        return self._authors

    @property
    def arxiv_classification(self) -> List[str]:
        """Get the ArXiv classification for the entry.

        Returns:
            List[str]: ArXiv classifications for the entry.
        """
        if not self._arxiv_classification:
            for t in self.entry.tags:
                self._arxiv_classification.append(t['term'])
        return self._arxiv_classification

    def disambiguate_autor(self, name: str) -> Author:
        """Parses arXiv author pages to find a matching author.

        Given a name string, generates possible arxiv author id, which
        is then visited to find a matching author based on the listed
        publications.

        Args:
            name (str): Author's name as returned by the ArXiv API

        Returns:
            Author: Author object with the arXiv author ID, if found.
        """
        author_split = name.lower().split(' ')
        finish = False
        i = 1
        while not finish:
            arxiv_author_id = "_".join([author_split[-1], author_split[0][0], str(i)])
            i += 1
            headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
            }

            base_url = "https://arxiv.org/a/"
            req = requests.get(base_url + arxiv_author_id + ".html", headers=headers)
            soup = BeautifulSoup(req.content, 'html.parser')

            try:
                author_html = soup.find("div", id="content").find("h1").get_text(strip=True)
            except AttributeError:
                author_html = "Not Found"
            if author_html == "Not Found":
                finish = True
            else:
                author_html = author_html.replace('\'s articles on arXiv', '')
                author_initials = author_html.split(' ')
                author_initials = author_initials[0][0] + ". " + author_initials[-1]

                if author_html == name or author_initials == name:
                    articles = soup.find_all("div", class_="list-title")
                    for article in articles:
                        article = article.get_text()
                        article = article.replace('\n', '').replace('Title: ', '').replace('  ',' ')
                        if article == self.title:
                            finish = True
                            orcid = self.get_orcid(soup)
                            return Author(self.api,
                                          name=name,
                                          orcid=orcid, 
                                          arxiv_id=arxiv_author_id)
        return Author(self.api, name=name)

    @staticmethod
    def arxiv_api(arxiv_id: str) -> FeedParserDict:
        api_url = 'http://export.arxiv.org/api/query?id_list='
        response = requests.get(api_url + arxiv_id)
        feed = feedparser.parse(response.text)
        return feed.entries[0]

    @staticmethod
    def get_orcid(soup):
        links = soup.find_all("a")
        for link in links:
            orcid = re.search('https://orcid.org/(.{4}-.{4}-.{4}-.{4})', link['href'])
            if orcid:
                return orcid.groups()[0]
        return None

@dataclass
class ArxivPublication():
    arxiv_id: str
    api: Optional[MardiClient] = None
    metadata: Arxiv = None
    title: str = None
    QID: str = None
    authors: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.api is None:
            self.api = Importer.get_api('arxiv')
        if ' ' in self.arxiv_id:
            self.arxiv_id = self.arxiv_id.split(' ')[0]
        self.metadata = Arxiv(arxiv_id =self.arxiv_id)
        self.title = self.metadata.title
        self.authors = self.metadata.authors

        arxiv_id = 'wdt:P818'
        QID_results = self.api.search_entity_by_value(arxiv_id, self.arxiv_id)
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
                arxiv_author_id = author_item.get_value('wdt:P4594')
                arxiv_author_id = arxiv_author_id[0] if arxiv_author_id else None
                aliases = []
                if author_item.aliases.get('en'):
                    for alias in author_item.aliases.get('en'):
                        aliases.append(str(alias))
                author = Author(self.api,
                                name=name,
                                orcid=orcid,
                                arxiv_id=arxiv_author_id,
                                _aliases=aliases,
                                _QID=QID)
                self.authors.append(author)
    
    def create(self):

        log = get_logger_safe(__name__)
        log.debug("Start creating wiki item for arXiv publication")

        if self.QID:
            return self.QID

        item = self.api.item.new()
        if self.title != "Error":
            item.labels.set(language="en", value=self.title)
            if self.title:
                item.descriptions.set(
                    language="en", 
                    value="scientific article from arXiv"
                )

            # Instance of: scholary article
            item.add_claim('wdt:P31','wd:Q13442814')

            # Publication date
            item.add_claim('wdt:P577', self.metadata.publication_date)

            # Arxiv ID
            item.add_claim('wdt:P818', self.arxiv_id)

            # Arxiv classification
            category_claims = []
            pattern_msc = re.compile(r'\d\d(?:-(?:XX|\d\d)|[A-Z](?:xx|\d\d))')
            pattern_acm = re.compile(r'^[ABCDEFGHIJK]\.[0-9m](\.[0-9m])?$')

            for category in self.metadata.arxiv_classification:
                if pattern_msc.match(category):
                    # MSC ID
                    msc_categories = re.findall(pattern_msc, category)
                    for msc_cat in msc_categories:
                        claim = self.api.get_claim('wdt:P3285', msc_cat)
                        category_claims.append(claim)
                elif pattern_acm.match(category) or ";" in category:
                    # ACM Computing Classification System (1998)
                    # Not existing in wikidata
                    # https://cran.r-project.org/web/classifications/ACM.html
                    continue
                elif category in taxonomy:
                    # arXiv classification
                    claim = self.api.get_claim('wdt:P820', category)
                    category_claims.append(claim)
                
            if category_claims:
                log.debug(
                    "arxiv category_claims types=%s values=%s",
                    [type(c).__name__ for c in category_claims],
                    category_claims,
                )
                item.add_claims(category_claims)

            # Authors
            author_QID = self.__preprocess_authors()
            claims = []
            for author in author_QID:
                claims.append(self.api.get_claim("wdt:P50", author))
            log.debug(
                "arxiv author claims types=%s values=%s",
                [type(c).__name__ for c in claims],
                claims,
            )
            item.add_claims(claims)

            # DOI
            doi = '10.48550/arXiv.' + self.arxiv_id
            item.add_claim('wdt:P356', doi.upper())
                        
            self.QID = item.write().id

        if self.QID:
            print(f"arXiv preprint with arXiv id: {self.arxiv_id} created with ID {self.QID}.")
            return self.QID
        else:
            print(f"arXiv preprint with arXiv id: {self.arxiv_id} could not be created.")
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
