#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.importer.Importer import ADataSource, ImporterException
from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
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
                                "Matroids.Small"]
        #self.collection_list = ["Manifolds.DIM2_3",
        #                        "Matroids.SelfDual",
        #                        "Matroids.Small",
        #                        "Polytopes.Combinatorial.01Polytopes",
        #                        "Polytopes.Combinatorial.CombinatorialTypes",
        #                        "Polytopes.Combinatorial.FacesBirkhoffPolytope",
        #                        "Polytopes.Combinatorial.SmallSpheresDim4",
        #                        "Polytopes.Geometric.01Polytopes",
        #                        "Polytopes.Lattice.01Polytopes",
        #                        "Polytopes.Lattice.ExceptionalMaximalHollow",
        #                        "Polytopes.Lattice.FewLatticePoints3D",
        #                        "Polytopes.Lattice.NonSpanning3D",
        #                        "Polytopes.Lattice.Panoptigons",
        #                        "Polytopes.Lattice.Reflexive",
        #                        "Polytopes.Lattice.SmallVolume",
        #                        "Polytopes.Lattice.SmoothReflexive",
        #                        "Tropical.Cubics",
        #                        "Tropical.Polytropes",
        #                        "Tropical.QuarticCurves",
        #                        "Tropical.SchlaefliFan",
        #                        "Tropical.TOM"]

        # author_tuple = (name, orcid, arxiv_id, affiliation_qid, wikidata_qid)
        author_tuples = [('Frank Lutz', '', '', 'wd:Q51985', 'wd:Q102201447'),
                         ('Constantin Fischer', '', '', 'wd:Q51985', ''),
                         ('Sachi Hashimoto', '0000-0002-8936-5545', '', 'wd:Q1912085', ''),
                         ('Bernd Sturmfels', '0000-0002-6642-1479', '', 'wd:Q1912085', 'wd:Q73000'),
                         ('Raluca Vlad', '', '', 'wd:Q49114', ''),
                         ('Yoshitake Matsumoto', '', '', 'wd:Q7842', ''),
                         ('Hiroshi Imai', '', '', 'wd:Q7842', ''),
                         ('David Bremner', '', '', 'wd:Q1112515', ''),
                         ('Oswin Aichholzer', '0000-0002-2364-0583', '', 'wd:Q689775', 'wd:Q102333743'),
                         ('Hiroyuki Miyata', '', '', 'wd:Q7842', ''),
                         ('Sonoko Moriyama', '0000-0003-3358-7779', '', 'wd:Q1062129', ''),
                         ('Komei Fukuda', '', '', 'wd:Q11942', 'wd:Q98644582'),
                         ('Andreas Paffenholz', '0000-0001-9718-523X', '', 'wd:Q310695', 'wd:Q102307516'),
                         ('Moritz Firsching', '', '', 'wd:Q153006', 'wd:Q102507664'),
                         ('Monica Blanco', '', '', 'wd:Q766962', 'wd:Q102689582'),
                         ('Francisco Santos', '0000-0003-2120-9068', '', 'wd:Q766962', 'wd:Q61677726'),
                         ('Ayush Kumar Tewari', '0000-0002-3494-2691', '', '', ''),
                         ('Maximilian Kreuzer', '', '', '', ''),
                         ('Gabriele Balletti', '0000-0002-0536-0027', '', '', 'wd:Q103013875'),
                         ('Andreas Kretschmer', '', '', 'wd:Q655866', ''),
                         ('Benjamin Lorenz', '', '', 'wd:Q51985', ''),
                         ('Mikkel Oebro', '', '', '', ''),
                         ('Michael Joswig', '0000-0002-4974-9659', '', 'wd:Q51985', 'wd:Q16930854'),
                         ('Lars Kastner', '0000-0001-9224-7761', '', 'wd:Q51985', ''),
                         ('Ngoc Tran', '', 'tran_n_3', 'wd:Q49213', ''),
                         ('Alheydis Geiger', '', '', 'wd:Q153978', ''),
                         ('Marta Panizzut', '0000-0001-8631-6329', '', 'wd:Q51985', 'wd:Q102782302'),
                         ('Silke Horn', '', '', 'wd:Q310695', 'wd:Q102398539')]

        for name, orcid, arxiv_id, affiliation, qid in author_tuples:
            author = Author(self.integrator, name, orcid, arxiv_id, affiliation)
            if qid:
                qid = qid.split(':')[1]
                author.qid = self.integrator.import_entities(qid)
            self.author_pool.append(author)
                

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
            print(name)
            self.collections.append(Collection(name))
            time.sleep(3)


    def push(self):

        #for collection in self.collections:
        #    if not collection.exists():
        #        collection.create()
        #    if self.update:
        #        collection.update()

        for collection in self.collections:
            for author in collection.authors:
                self.author_pool.extend(collection.author_pool)

        Author.disambiguate_authors(self.author_pool)



