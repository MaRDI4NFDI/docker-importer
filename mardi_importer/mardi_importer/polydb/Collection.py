import urllib.request, json, re

from mardi_importer.integrator import MardiIntegrator
from .Author import Author
from .ArxivPublication import ArxivPublication
from .CrossrefPublication import CrossrefPublication
from dataclasses import dataclass, field
from typing import List, Tuple, Union


@dataclass
class GenericReference():
    api: MardiIntegrator
    title: str
    authors: List[Author]
    attributes: str

    def create(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.title)
        item.descriptions.set(
                language="en",
                value="scientific article"
            )

        exists = item.is_instance_of('wd:Q13442814')
        if exists: return exists

        # Instance of: scholarly article
        item.add_claim('wdt:P31','wd:Q13442814')

        for author in self.authors:
            item.add_claim('wdt:P50', author.QID)

        conference = None
        if self.attributes == "KyotoCGGT2007, 2007/6/11-15":
            conference = self.api.import_entities('Q106338712')            
        elif self.attributes == "Kyoto RIMS Workshop on Computational Geometry and Discrete Mathematics, RIMS, Kyoto University, 2008/10/16":
            conf_item = self.api.item.new()
            conf_item.labels.set(language="en", value="Kyoto RIMS Workshop on Computational Geometry and Discrete Mathematics")
            conf_item.descriptions.set(
                language="en",
                value="academic conference"
            )
            conference = conf_item.exists()
            if not conference:
                conf_item.add_claim('wdt:P31', 'wd:Q2020153')
                conf_item.add_claim('wdt:P1476', 'Kyoto RIMS Workshop on Computational Geometry and Discrete Mathematics')
                conf_item.add_claim('wdt:P17', 'wd:Q17')
                conf_item.add_claim('wdt:P276', 'wd:Q34600')
                location = self.api.import_entities('Q840667')
                conf_item.add_claim('wdt:P276', location)
                conf_item.add_claim('wdt:P921', 'wd:Q874709')
                conf_item.add_claim('wdt:P921', 'wd:Q121416')
                conf_item.add_claim('wdt:P580', "+2007-10-16T00:00:00Z")
                conf_item.add_claim('wdt:P582', "+2007-10-18T00:00:00Z")
                conference = conf_item.write().id
        elif self.attributes == "PhD Thesis, Aarhus 2007":
            item.descriptions.set(
                language="en",
                value="doctoral thesis"
            )
            item.add_claim('wdt:P31', 'wd:Q187685')
            location = self.api.import_entities('Q924265')
            item.add_claim('wdt:P4101', location)
            item.add_claim('wdt:P577', "+2007-00-00T00:00:00Z")
        else:
            print('Following conference or attribute cannot be parsed')
            print(self.attributes)
            print('----------------------------------')
        
        if conference:
            item.add_claim('wdt:P5072', conference)

        return item.write().id        

@dataclass
class Collection():
    label: str
    api: MardiIntegrator = MardiIntegrator()
    authors: List[Author] = field(default_factory=list)
    author_pool: List[Author] = field(default_factory=list)
    maintainer: List[Author] = field(default_factory=list)
    contributor: List[Author] = field(default_factory=list)
    arxiv: List[ArxivPublication] = field(default_factory=list)
    crossref: List[CrossrefPublication] = field(default_factory=list)
    generic_references: List[GenericReference] = field(default_factory=list)
    data: List[Tuple[str, str]] = field(default_factory=list)

    def __post_init__(self):
        url = f"https://polydb.org/rest/current/collection/{self.label}"

        with urllib.request.urlopen(url) as url:
            json_data = json.load(url)

        if json_data:
            self.description = json_data.get('description')
            self.authors = self.parse_person(json_data.get('author'))
            self.maintainer = self.parse_person(json_data.get('maintainer'))
            self.contributor = self.parse_person(json_data.get('contributor'))
            self.parse_references(json_data)
            self.fill_author_pool()

        self.item = self.api.item.new()
        self.item.labels.set(language="en", value=self.label)

        # PolyDB QID (Instance of: collection)
        item = self.api.item.new()
        item.labels.set(language="en", value="polyDB collection")
        self.poly_db_qid = item.is_instance_of('wd:Q2668072')

        # Contributed by
        self.contributed_by_pid = self.api.get_local_id_by_label( 
                                                    'contributed by', 
                                                    'property')

    def exists(self):
        return self.item.is_instance_of(self.poly_db_qid)

    def create(self):
        self.item.descriptions.set(
            language="en",
            value=self.description
        )
        
        # Instance of: PolyDB Collection
        self.item.add_claim("wdt:P31", self.poly_db_qid)

        # Author
        for author in self.authors:
            self.item.add_claim("wdt:P50", author.QID)

        # Maintainer
        for maintainer in self.maintainer:
            self.item.add_claim("wdt:P126", maintainer.QID)

        # Contributor
        for contributor in self.contributor:
            self.item.add_claim(self.contributed_by_pid, contributor.QID)

        # Publications
        for publications in [self.arxiv, 
                             self.crossref, 
                             self.generic_references]:
            for publication in publications:
                pub_qid = publication.create()
                self.item.add_claim('wdt:P2860', pub_qid)

        # Data source
        for source, url in self.data:
            if source == "url":
                self.item.add_claim('wdt:P1325', url)
            elif source == "github":
                self.item.add_claim('wdt:P1324', url)

        self.item.write()

    def update(self):
        pass

    def parse_person(self, elements):
        if not elements: return []
        result = []
        for el in elements:
            person = Author(self.api, 
                            name=el.get('name'),
                            orcid="",
                            arxiv_id="", 
                            affiliation=el.get('affiliation'))
            result.append(person)
        return result

    def parse_references(self, json_data):
        list_references = []

        references = json_data.get('references')      
        if references: 
            list_references = self.parse_publications(references)

        webpage = json_data.get('webpage')
        if webpage: 
            list_references.extend(self.parse_webpage(webpage))

        for ref_type, ref in list_references:
            if ref_type == "arxiv":
                self.arxiv.append(ArxivPublication(self.api, ref))
            elif ref_type == "doi":
                self.crossref.append(CrossrefPublication(self.api, ref))
            elif ref_type == "reference":
                self.generic_references.append(ref)
            elif ref_type in ["url", "github"]:
                self.data.append((ref_type, ref))

    def parse_publications(self, references):
        result = []        
        for ref in references:
            bib = ref.get('bib')
            bib = bib.lower() if bib else ""
            if 'arxiv' in bib and bib != 'arxiv:<soon>':
                groups = re.search("arxiv[: ](\d{4}.\d{4,5})", bib)
                arxiv_id = groups.group(1)
                result.append(('arxiv', arxiv_id))
            else:
                links = ref.get('links')
                if links:
                    for link in links:
                        if link['type'] in ["arxiv", "arXiv"]:
                            arxiv_id = re.findall("\d{4}.\d{4,5}", link['link'])
                            if arxiv_id:
                                result.append(('arxiv', arxiv_id[0]))
                            else:
                                result.append(self.create_reference(ref))
                        elif link['type'] in ["journal", "doi"]:
                            doi = (re.findall(r"doi.org\/(.*)$", link['link']) or 
                                    re.findall(r"springer.com/article\/(.*)$", link['link']))
                            if doi:
                                result.append(('doi', doi[0]))
                            else:
                                result.append(self.create_reference(ref))
                        else:
                            result.append(self.create_reference(ref))
                else:
                    result.append(self.create_reference(ref))    
        return result

    def parse_webpage(self, elements):
        if not elements: return []
        result = []        
        for el in elements:
            address = el.get('address')
            if 'github' in address:
                result.append(('github', address))
            else:
                result.append(('url', address))
        return result

    def create_reference(self, ref):
        title = ref['title']
        if title == 'The complete enumeration of the 4-polytopes and 3-spheres with eight vertices':
            return ('doi', '10.2140/pjm.1985.117.1')
        elif title == 'Convex Polytopes':
            return ('doi', '10.1007/978-1-4613-0019-9')  
        elif title == 'Remote Computing Service via email':
            return ('url', 'http://www.ist.tugraz.at/staff/aichholzer/research/rp/rcs/info01poly/')
        elif title == 'Extremal Properties of 0/1-Polytopes of Dimension 5':
            return ('doi', '10.1007/978-3-0348-8438-9_5')
        elif title == 'Faces of Birkhoff Polytopes':
            return ('doi', '10.37236/4499')
        elif title == 'Computing tropical bitangents of smooth quartic curves in polymake':
            return ('arxiv', '2112.04447')
        else:
            authors = ref['authors'].split(', ')
            authors = [Author(self.api, name=author) for author in authors]
            attributes = ref['bib']
            reference = GenericReference(self.api, title, authors, attributes)
            return ('reference', reference)

    def fill_author_pool(self):
        self.author_pool.extend(self.authors)
        self.author_pool.extend(self.maintainer)
        self.author_pool.extend(self.contributor)
        for ref in self.arxiv:
            self.author_pool.extend(ref.authors)
        for ref in self.crossref:
            self.author_pool.extend(ref.authors)
        for ref in self.generic_references:
            self.author_pool.extend(ref.authors)

    def update_authors(self, polydb_authors):
        authors_list = [self.authors, self.contributor, self.maintainer]
        for lst in authors_list:
            for author in lst:
                author.pull_QID(polydb_authors)

        for publications in [self.arxiv, self.crossref, self.generic_references]:
            for publication in publications:
                for author in publication.authors:
                    author.pull_QID(polydb_authors)
