#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.integrator.Integrator import MardiIntegrator
from dataclasses import dataclass, field
from typing import List, Tuple, Union

import urllib.request, json, re

@dataclass
class Reference():
    title: str
    authors: List[str]
    attributes: str

@dataclass
class Collection():
    label: str
    api: MardiIntegrator = MardiIntegrator()
    authors: List[Tuple[str, str]] = field(default_factory=list)
    maintainer: List[Tuple[str, str]] = field(default_factory=list)
    contributor: List[Tuple[str, str]] = field(default_factory=list)
    references: List[Tuple[str, Union[str, Reference]]] = field(default_factory=list)

    def __post_init__(self):
        self.orcid_list = [('Frank Lutz', ''),
                        ('Andreas Paffenholz', '0000-0001-9718-523X'),
                        ('Constantin Fischer', ''),
                        ('Yoshitake Matsumoto', ''),
                        ('Sonoko Moriyama', '0000-0003-3358-7779'),
                        ('Hiroshi Imai,',''),
                        ('David Bremmer', ''),
                        ('Benjamin Schröter', '0000-0003-3153-5211'),
                        ('Simon Hampe', '0000-0003-4855-120X'),
                        ('', ''),
                        ('', ''),
                        ('', '')
                        ]
        url = f"https://polydb.org/rest/current/collection/{self.label}"

        with urllib.request.urlopen(url) as url:
            json_data = json.load(url)

        if json_data:
            self.description = json_data.get('description')
            self.authors = self.parse_person(json_data.get('author'))
            self.maintainer = self.parse_person(json_data.get('maintainer'))
            self.contributor = self.parse_person(json_data.get('contributor'))
            self.references = self.parse_references(json_data.get('references'))
            self.references.extend(self.parse_resources(json_data.get('webpage')))

    def exists(self):
        pass 

    def create(self):
        pass

    def update(self):
        pass

    def parse_person(self, elements):
        result = []
        if not elements: elements = []
        for el in elements:
            result.append((el.get('name'), el.get('affiliation')))
        return result

    def parse_references(self, elements):
        if not elements: return []
        result = []        
        for el in elements:
            bib = el.get('bib')
            bib = bib.lower() if bib else ""
            if 'arxiv' in bib and bib != 'arxiv:<soon>':
                groups = re.search("arxiv[: ](\d{4}.\d{4,5})", bib)
                arxiv_id = groups.group(1)
                result.append(('arxiv', arxiv_id))
            else:
                links = el.get('links')
                if links:
                    for link in links:
                        if link['type'] in ["arxiv", "arXiv"]:
                            arxiv_id = re.findall("\d{4}.\d{4,5}", link['link'])
                            if arxiv_id:
                                result.append(('arxiv', arxiv_id[0]))
                            else:
                                result.append(self.create_reference(el))
                        elif link['type'] in ["journal", "doi"]:
                            doi = (re.findall(r"doi.org\/(.*)$", link['link']) or 
                                    re.findall(r"springer.com/article\/(.*)$", link['link']))
                            if doi:
                                result.append(('doi', doi[0]))
                            else:
                                result.append(self.create_reference(el))
                        else:
                            result.append(self.create_reference(el))
                else:
                    result.append(self.create_reference(el))    
        return result

    def parse_resources(self, elements):
        if not elements: return []
        result = []        
        for el in elements:
            address = el.get('address')
            if 'github' in address:
                result.append(('github', address))
            else:
                result.append(('url', address))
        return result

    def create_reference(self, element):
        title = element['title']
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
            authors = element['authors'].split(', ')
            attributes = element['bib']
            ref = Reference(title, authors, attributes)
            return ('reference', ref)





