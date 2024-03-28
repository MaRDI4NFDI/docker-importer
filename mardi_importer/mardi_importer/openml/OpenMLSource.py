from mardi_importer.importer import ADataSource
import openml
from mardi_importer.integrator import MardiIntegrator
from .OpenMLDataset import OpenMLDataset
import os
import json
from itertools import zip_longest
import pickle

class OpenMLSource(ADataSource):
    def __init__(self):
        self.integrator = MardiIntegrator()
        self.filepath = os.path.realpath(os.path.dirname(__file__))
    def setup(self):
        """Create all necessary properties and entities for zbMath"""
        # Import entities from Wikidata
        filename = self.filepath + "/wikidata_entities.txt"
        self.integrator.import_entities(filename=filename)
        self.create_local_entities()
        # self.de_number_prop = self.integrator.get_local_id_by_label(
        #     "zbMATH DE Number", "property"
        # )
        # self.keyword_prop = self.integrator.get_local_id_by_label(
        #     "zbMATH keyword string", "property"
        # )

    def create_local_entities(self):
        filename = self.filepath + "/new_entities.json"
        f = open(filename)
        entities = json.load(f)

        for prop_element in entities["properties"]:
            prop = self.integrator.property.new()
            prop.labels.set(language="en", value=prop_element["label"])
            prop.descriptions.set(language="en", value=prop_element["description"])
            prop.datatype = prop_element["datatype"]
            if not prop.exists():
                prop.write()

        for item_element in entities["items"]:
            item = self.integrator.item.new()
            item.labels.set(language="en", value=item_element["label"])
            item.descriptions.set(language="en", value=item_element["description"])
            if "claims" in item_element:
                for key, value in item_element["claims"].items():
                    item.add_claim(key, value=value)
            if not item.exists():
                item.write()

    def pull(self):
        dataset_dict = {"name": [], "dataset_id": [], "version": [], "creators": [],
                        "collection_date": [], "upload_date": [], 
                        "license": [], "url":[], "default_target_attribute":[], "row_id_attribute":[],
                        "tags":[], "original_data_url":[], "paper_url":[],
                        "md5_checksum": [], "num_binary_features":[],
                        "num_classes":[], "num_features":[], "num_instances":[], "num_instances_missing_vals":[],
                        "num_missing_vals":[], "num_numeric_features":[], "num_symbolic_features":[], 
                        "format":[]}
        dataset_df = openml.datasets.list_datasets(output_format="dataframe")
        did_list = dataset_df["did"].unique()
        for did in did_list:
            try:
                ds = openml.datasets.get_dataset(int(did), download_data=False, download_qualities=True, download_features_meta_data=False)
            except Exception as e:
                ds = openml.datasets.get_dataset(int(did), download_data=False, download_qualities=False, download_features_meta_data=False)
            dataset_dict["name"].append(ds.name)
            dataset_dict["dataset_id"].append(did)
            dataset_dict["version"].append(ds.version)
            dataset_dict["creators"].append(ds.creator)
            dataset_dict["collection_date"].append(ds.collection_date)
            dataset_dict["upload_date"].append(ds.upload_date)
            dataset_dict["license"].append(ds.licence)
            dataset_dict["url"].append(ds.url)
            dataset_dict["default_target_attribute"].append(ds.default_target_attribute)
            dataset_dict["row_id_attribute"].append(ds.row_id_attribute)
            dataset_dict["tags"].append(ds.tag)
            dataset_dict["original_data_url"].append(ds.original_data_url)
            dataset_dict["paper_url"].append(ds.paper_url)
            dataset_dict["md5_checksum"].append(ds.md5_checksum)
            try:
                qualities = ds.qualities
            except:
                dataset_dict["num_binary_features"].append(None)
                dataset_dict["num_classes"].append(None)
                dataset_dict["num_features"].append(None)
                dataset_dict["num_instances"].append(None)
                dataset_dict["num_instances_missing_vals"].append(None)
                dataset_dict["num_missing_vals"].append(None)
                dataset_dict["num_numeric_features"].append(None)
                dataset_dict["num_symbolic_features"].append(None)
                continue
            if 'NumberOfBinaryFeatures' in ds.qualities:
                dataset_dict["num_binary_features"].append(ds.qualities['NumberOfBinaryFeatures'])
            else:
                dataset_dict["num_binary_features"].append(None)
            if  'NumberOfClasses' in ds.qualities:
                dataset_dict["num_classes"].append(ds.qualities['NumberOfClasses'])
            else:
                dataset_dict["num_classes"].append(None)
            if  'NumberOfFeatures' in ds.qualities:
                dataset_dict["num_features"].append(ds.qualities['NumberOfFeatures'])
            else:
                dataset_dict["num_features"].append(None)
            if  'NumberOfInstances' in ds.qualities:
                dataset_dict["num_instances"].append(ds.qualities['NumberOfInstances'])
            else:
                dataset_dict["num_instances"].append(None)
            if  'NumberOfInstancesWithMissingValues' in ds.qualities:
                dataset_dict["num_instances_missing_vals"].append(ds.qualities['NumberOfInstancesWithMissingValues'])
            else:
                dataset_dict["num_instances_missing_vals"].append(None)
            if  'NumberOfMissingValues' in ds.qualities:
                dataset_dict["num_missing_vals"].append(ds.qualities['NumberOfMissingValues'])
            else:
                dataset_dict["num_missing_vals"].append(None)
            if  'NumberOfNumericFeatures' in ds.qualities:
                dataset_dict["num_numeric_features"].append(ds.qualities['NumberOfNumericFeatures'])
            else:
                dataset_dict["num_numeric_features"].append(None)
            if  'NumberOfSymbolicFeatures' in ds.qualities:
                dataset_dict["num_symbolic_features"].append(ds.qualities['NumberOfSymbolicFeatures'])
            else:
                dataset_dict["num_symbolic_features"].append(None)
        with open('dataset_dict.p', 'wb') as handle:
            pickle.dump(dataset_dict, handle)
        return(dataset_dict)
    
    def push(self):
        with open('/data/dataset_dict.p', 'rb') as handle:
            dataset_dict = pickle.load(handle)
        # dataset_dict = {'name': ['kr-vs-kp'],
        #             'description': ['Author: Alen Shapiro\nSource: [UCI](https://archive.ics.uci.edu/ml/datasets/Chess+(King-Rook+vs.+King-Pawn))\nPlease cite: [UCI citation policy](https://archive.ics.uci.edu/ml/citation_policy.html)\n\n1. Title: Chess End-Game -- King+Rook versus King+Pawn on a7\n(usually abbreviated KRKPA7). The pawn on a7 means it is one square\naway from queening. It is the King+Rook\'s side (white) to move.\n\n2. Sources:\n(a) Database originally generated and described by Alen Shapiro.\n(b) Donor/Coder: Rob Holte (holte@uottawa.bitnet). The database\nwas supplied to Holte by Peter Clark of the Turing Institute\nin Glasgow (pete@turing.ac.uk).\n(c) Date: 1 August 1989\n\n3. Past Usage:\n- Alen D. Shapiro (1983,1987), "Structured Induction in Expert Systems",\nAddison-Wesley. This book is based on Shapiro\'s Ph.D. thesis (1983)\nat the University of Edinburgh entitled "The Role of Structured\nInduction in Expert Systems".\n- Stephen Muggleton (1987), "Structuring Knowledge by Asking Questions",\npp.218-229 in "Progress in Machine Learning", edited by I. Bratko\nand Nada Lavrac, Sigma Press, Wilmslow, England SK9 5BB.\n- Robert C. Holte, Liane Acker, and Bruce W. Porter (1989),\n"Concept Learning and the Problem of Small Disjuncts",\nProceedings of IJCAI. Also available as technical report AI89-106,\nComputer Sciences Department, University of Texas at Austin,\nAustin, Texas 78712.\n\n4. Relevant Information:\nThe dataset format is described below. Note: the format of this\ndatabase was modified on 2/26/90 to conform with the format of all\nthe other databases in the UCI repository of machine learning databases.\n\n5. Number of Instances: 3196 total\n\n6. Number of Attributes: 36\n\n7. Attribute Summaries:\nClasses (2): -- White-can-win ("won") and White-cannot-win ("nowin").\nI believe that White is deemed to be unable to win if the Black pawn\ncan safely advance.\nAttributes: see Shapiro\'s book.\n\n8. Missing Attributes: -- none\n\n9. Class Distribution:\nIn 1669 of the positions (52%), White can win.\nIn 1527 of the positions (48%), White cannot win.\n\nThe format for instances in this database is a sequence of 37 attribute values.\nEach instance is a board-descriptions for this chess endgame. The first\n36 attributes describe the board. The last (37th) attribute is the\nclassification: "win" or "nowin". There are 0 missing values.\nA typical board-description is\n\nf,f,f,f,f,f,f,f,f,f,f,f,l,f,n,f,f,t,f,f,f,f,f,f,f,t,f,f,f,f,f,f,f,t,t,n,won\n\nThe names of the features do not appear in the board-descriptions.\nInstead, each feature correponds to a particular position in the\nfeature-value list. For example, the head of this list is the value\nfor the feature "bkblk". The following is the list of features, in\nthe order in which their values appear in the feature-value list:\n\n[bkblk,bknwy,bkon8,bkona,bkspr,bkxbq,bkxcr,bkxwp,blxwp,bxqsq,cntxt,dsopp,dwipd,\nhdchk,katri,mulch,qxmsq,r2ar8,reskd,reskr,rimmx,rkxwp,rxmsq,simpl,skach,skewr,\nskrxp,spcop,stlmt,thrsk,wkcti,wkna8,wknck,wkovl,wkpos,wtoeg]\n\nIn the file, there is one instance (board position) per line.\n\n\nNum Instances: 3196\nNum Attributes: 37\nNum Continuous: 0 (Int 0 / Real 0)\nNum Discrete: 37\nMissing values: 0 / 0.0%'],
        #             'dataset_id': [3],
        #             'version': [1],
        #             'creators': ['Alen Shapiro'],
        #             'contributors': ['Rob Holte'],
        #             'collection_date': ['1989-08-01'],
        #             'upload_date': ['2014-04-06T23:19:28'],
        #             'license': ['CC0'],
        #             'url': ['https://api.openml.org/data/v1/download/3/kr-vs-kp.arff'],
        #             'default_target_attribute': ['class'],
        #             'row_id_attribute': [None],
        #             'tags': [['Machine Learning',
        #                 'Mathematics',
        #                 'mythbusting_1',
        #                 'OpenML-CC18',
        #                 'OpenML100',
        #                 'study_1',
        #                 'study_123',
        #                 'study_14',
        #                 'study_144',
        #                 'uci']],
        #             'original_data_url': ['https://archive.ics.uci.edu/ml/datasets/Chess+(King-Rook+vs.+King-Pawn)'],
        #             'paper_url': ['https://dl.acm.org/doi/abs/10.5555/32231'],
        #             'md5_checksum': ['ad6eb32b7492524d4382a40e23cdbb8e'],
        #             'features': [{0: ["0 - bkblk (nominal)"],
        #                         1: ["1 - bknwy (nominal)"],
        #                         2: ["2 - bkon8 (nominal)"],
        #                         3: ["3 - bkona (nominal)"],
        #                         36: ["36 - class (nominal)"]}],
        #             'num_binary_features': [35.0],
        #             'num_classes': [2.0],
        #             'num_features': [37.0],
        #             'num_instances': [3196.0],
        #             'num_instances_missing_vals': [0.0],
        #             'num_missing_vals': [0.0],
        #             'num_numeric_features': [0.0],
        #             'num_symbolic_features': [37.0],
        #             'format': ['ARFF']}
        for items in zip_longest(*[dataset_dict[key] for key in dataset_dict], fillvalue=None):
            lookup_dict = dict(zip(dataset_dict.keys(), items))
            dataset = OpenMLDataset(
                integrator = self.integrator,
                **lookup_dict
            )
            if not dataset.exists():
                dataset.create()
            else:
                dataset.update()