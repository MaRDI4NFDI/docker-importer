#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
from mardi_importer.polydb.ArxivPublication import ArxivPublication
from mardi_importer.polydb.CrossrefPublication import CrossrefPublication
from mardi_importer.polydb.Author import Author
from dataclasses import dataclass, field
from typing import List, Tuple, Union

import urllib.request, json, re

@dataclass
class GenericReference():
    api: MardiIntegrator
    title: str
    authors: List[Author]
    attributes: str

    def create(self):
        pass

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

    def exists(self):
        pass 

    def create(self):
        pass

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
        for ref in self.arxiv:
            self.author_pool.extend(ref.authors)
        for ref in self.crossref:
            self.author_pool.extend(ref.authors)
        for ref in self.generic_references:
            self.author_pool.extend(ref.authors)



