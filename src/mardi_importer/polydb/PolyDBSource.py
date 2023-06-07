#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.importer.Importer import ADataSource, ImporterException
from mardi_importer.integrator.Integrator import MardiIntegrator
from mardi_importer.polydb.Collection import Collection
from mardi_importer.polydb.Author import Author
from dataclasses import dataclass, field
from typing import List
import time
import json
import os
import logging
#log = logging.getLogger('CRANlogger')

@dataclass
class PolyDBSource(ADataSource):
    """Processes collection data from polyDB.org.

    Attributes:
        integrator (MardiIntegrator):
            API to wikibase
        collections (List[str]):
            List of current collections
    """
    update: bool = False
    integrator: MardiIntegrator = MardiIntegrator()
    collection_list: List[str] = field(default_factory=list)  
    author_pool: List[Author] = field(default_factory=list) 
    collections: List[Collection] = field(default_factory=list)

    def __post_init__(self):
        self.collection_list = ["Manifolds.DIM2_3",
                                "Matroids.SelfDual",
                                "Matroids.Small",
                                "Polytopes.Combinatorial.01Polytopes",
                                "Polytopes.Combinatorial.CombinatorialTypes",
                                "Polytopes.Combinatorial.FacesBirkhoffPolytope",
                                "Polytopes.Combinatorial.SmallSpheresDim4",
                                "Polytopes.Geometric.01Polytopes",
                                "Polytopes.Lattice.01Polytopes",
                                "Polytopes.Lattice.ExceptionalMaximalHollow",
                                "Polytopes.Lattice.FewLatticePoints3D",
                                "Polytopes.Lattice.NonSpanning3D",
                                "Polytopes.Lattice.Panoptigons",
                                "Polytopes.Lattice.Reflexive",
                                "Polytopes.Lattice.SmallVolume",
                                "Polytopes.Lattice.SmoothReflexive",
                                "Tropical.Cubics",
                                "Tropical.Polytropes",
                                "Tropical.QuarticCurves",
                                "Tropical.SchlaefliFan",
                                "Tropical.TOM"]
        author_tuples = [('Frank Lutz', '', '', 'wdt:Q51985'),
                         ('Constantin Fischer', '', '', 'wdt:Q51985'),
                         ('Sachi Hashimoto', '0000-0002-8936-5545', '', 'wdt:Q1912085'),
                         ('Bernd Sturmfels', '0000-0002-6642-1479', '', 'wdt:Q1912085'),
                         ('Raluca Vlad', '', '', 'wdt:Q49114'),
                         ('Yoshitake Matsumoto', '', '', 'wdt:Q7842'),
                         ('Hiroshi Imai', '', '', 'wdt:Q7842'),
                         ('David Bremner', '', '', 'wdt:Q1112515'),
                         ('Oswin Aichholzer', '0000-0002-2364-0583', '', 'wdt:Q689775'),
                         ('Hiroyuki Miyata', '', '', 'wdt:Q7842'),
                         ('Sonoko Moriyama', '0000-0003-3358-7779', '', 'wdt:Q1062129'),
                         ('Komei Fukuda', '', '', 'wdt:Q11942'),
                         ('Andreas Paffenholz', '0000-0001-9718-523X', '', 'wdt:Q310695'),
                         ('Moritz Firsching', '', '', 'wdt:Q153006'),
                         ('Monica Blanco', '', '', 'wdt:Q766962'),
                         ('Francisco Santos', '0000-0003-2120-9068', '', 'wdt:Q766962'),
                         ('Ayush Kumar Tewari', '0000-0002-3494-2691', '', ''),
                         ('Maximilian Kreuzer', '', '', ''),
                         ('Gabriele Balletti', '0000-0002-0536-0027', '', ''),
                         ('Andreas Kretschmer', '', '', 'wdt:Q655866'),
                         ('Benjamin Lorenz', '', '', 'wdt:Q51985'),
                         ('Mikkel Oebro', '', '', ''),
                         ('Michael Joswig', '0000-0002-4974-9659', '', 'wdt:Q51985'),
                         ('Lars Kastner', '0000-0001-9224-7761', '', 'wdt:Q51985'),
                         ('Ngoc Tran', '', 'tran_n_3', 'wdt:Q49213'),
                         ('Alheydis Geiger', '', '', 'wdt:Q153978'),
                         ('Marta Panizzut', '0000-0001-8631-6329', '', 'wdt:Q51985'),
                         ('Silke Horn', '', '', 'wdt:Q310695')]
        self.author_pool = [Author(self.integrator, name, orcid, arxiv_id, affiliation) 
                            for name, orcid, arxiv_id, affiliation in author_tuples]

        for author in self.author_pool:
            print(author)

    def setup(self):
        """Create all necessary properties for polyDB
        """
        filepath = os.path.realpath(os.path.dirname(__file__)) 


        filename = filepath + "/wikidata_entities.txt"
        self.integrator.import_entities(filename=filename)
        
        filename = filepath + "/new_entities.json"
        f = open(filename)
        entities = json.load(f)

        for prop_element in entities['properties']:
            prop = self.integrator.property.new()
            prop.labels.set(language='en', value=prop_element['label'])
            prop.descriptions.set(language='en', value=prop_element['description'])
            prop.datatype = prop_element['datatype']
            if not prop.exists(): prop.write()

        for item_element in entities['items']:
            item = self.integrator.item.new()
            item.labels.set(language='en', value=item_element['label'])
            item.descriptions.set(language='en', value=item_element['description'])
            for key, value in item_element['claims'].items():
                item.add_claim(key,value=value)
            if not item.exists(): item.write()

    def get_collection_list(self):
        pass

    def pull(self):
        if self.update:
            self.get_collection_list()
        for name in self.collection_list:
            new_col = Collection(name)
            self.collections.append(new_col)

            #for author in new_col.authors:
            #    print(author)
            #for contributor in new_col.authors:
            #    print(contributor)
            #for maintainer in new_col.authors:
            #    print(maintainer)
            for el in new_col.references:
                print(el)
            time.sleep(3)


    def push(self):
        for collection in self.collections:
            if not collection.exists():
                collection.create()
            if self.update:
                collection.update()

            # Get all authors
            # Get references with 'arxiv', 'doi'
            # Get authors from references into the pool
            # Then disambiguate them and create tuple with QIDs
            # Create publications & new tuple with QIDs?



