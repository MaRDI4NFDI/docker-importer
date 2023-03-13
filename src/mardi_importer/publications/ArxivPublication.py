import feedparser
import logging
import requests
import re
from bs4 import BeautifulSoup

from mardi_importer.publications.Author import Author
from wikibaseintegrator.wbi_enums import ActionIfExists

log = logging.getLogger('CRANlogger')

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

def arxiv_api(arxiv_id):
    api_url = 'http://export.arxiv.org/api/query?id_list='
    response = requests.get(api_url + arxiv_id)
    feed = feedparser.parse(response.text)
    return feed.entries[0]

class authorArxiv():
    def __init__(self, author, orcid = None, arxiv_author_id = None):
        self.name = author
        self.orcid = orcid
        self.arxiv_author_id = arxiv_author_id

class Arxiv():
    def __init__(self, arxiv_id):
        self.id = arxiv_id
        self.entry = arxiv_api(arxiv_id)
        self._title = ''
        self._abstract = ''
        self._publication_date = ''
        self._authors = []
        self._arxiv_classification = ''

    @property
    def title(self):
        if not self._title:
            title = self.entry.title
            self._title = title.replace('\n', ' ')
        return self._title

    @property
    def abstract(self):
        if not self._abstract:
            abstract = self.entry.summary
            self._abstract = abstract.replace('\n', ' ')
        return self._abstract

    @property
    def publication_date(self):
        if not self._publication_date:
            time = self.entry.published
            if not time.startswith("+"):
                time = "+" + time
            time = time[0:11] + "T00:00:00Z"
            self._publication_date = time
        return self._publication_date

    @property
    def authors(self):
        if not self._authors:
            for author in self.entry.authors:
                author = self.disambiguate_autor(author.name)
                self._authors.append(author)
        return self._authors

    @property
    def arxiv_classification(self):
        if not self._arxiv_classification:
            self._arxiv_classification = []
            for t in self.entry.tags:
                self._arxiv_classification.append(t['term'])
        return self._arxiv_classification

    def disambiguate_autor(self, author):
        author_split = author.lower().split(' ')
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
            req = requests.get(base_url + arxiv_author_id + ".html", headers)
            soup = BeautifulSoup(req.content, 'html.parser')

            author_html = soup.find("div", id="content").find("h1").get_text()
            if author_html == "Not Found":
                finish = True
            else:
                author_html = author_html.replace('\'s articles on arXiv', '')
                author_initials = author_html.split(' ')
                author_initials = author_initials[0][0] + ". " + author_initials[-1]

                if author_html == author or author_initials == author:
                    articles = soup.find_all("div", class_="list-title")
                    for article in articles:
                        article = article.get_text()
                        article = article.replace('\n', '').replace('Title: ', '').replace('  ',' ')
                        if article == self.title:
                            finish = True
                            orcid = self.get_orcid(soup)
                            return authorArxiv(author, orcid, arxiv_author_id)
        return authorArxiv(author)

    @staticmethod
    def get_orcid(soup):
        links = soup.find_all("a")
        for link in links:
            orcid = re.search('https://orcid.org/(.{4}-.{4}-.{4}-.{4})', link['href'])
            if orcid:
                return orcid.groups()[0]
        return None

class ArxivPublication():
    def __init__(self, integrator, arxiv_id, coauthors=[]):
        self.arxiv_id = arxiv_id
        self.metadata = Arxiv(arxiv_id)
        self.api = integrator
        self.coauthors = coauthors
        self._title = ''

    @property
    def title(self):
        if not self._title:
            self._title = self.metadata.title
        return self._title
    
    def create(self):
        publication_ID = None
        item = self.api.item.new()
        if self.metadata.title != "Error":
            item.labels.set(language="en", value=self.metadata.title)
            if self.metadata.title:
                item.descriptions.set(
                    language="en", 
                    value="scientific article from arXiv"
                )

            # Instance of: scholary article
            item.add_claim('wdt:P31','wd:Q13442814')

            # Publication date
            item.add_claim('wdt:P577', time=self.metadata.publication_date)
            #item.add_claim('wdt:P577', self.metadata.publication_date)

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
                item.add_claims(category_claims)

            # Authors
            author_claims = []
            author_list = []
            for author in self.metadata.authors:
                author_item = Author(self.api, 
                                    author.name, 
                                    author.orcid, 
                                    self.coauthors)
                author_id = author_item.create()
                author_list.append(author_id)

                if author.arxiv_author_id:
                    update_item = self.api.item.get(entity_id=author_id)
                    claim = self.api.get_claim('wdt:P818', author.arxiv_author_id)
                    update_item.claims.add(
                        claim,
                        ActionIfExists.APPEND_OR_REPLACE,
                    )
                    update_item.write()
                
                claim = self.api.get_claim('wdt:P50', author_id)
                author_claims.append(claim)
            item.add_claims(author_claims)

            coauthors_ini = self.coauthors
            for author_qid in author_list:
                if author_qid not in coauthors_ini:
                    self.coauthors.append(author_qid)

            # DOI
            doi = '10.48550/arXiv.' + self.arxiv_id
            item.add_claim('wdt:P356', doi)
                        
            publication_ID = item.write().id

        if publication_ID:
            log.info(f"arXiv preprint with arXiv id: {self.arxiv_id} created with ID {publication_ID}.")
            return publication_ID
        else:
            log.info(f"arXiv preprint with arXiv id: {self.arxiv_id} could not be created.")
            return None
        