from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, merge_items
from mardi_importer.integrator.MardiIntegrator import MardiIntegrator
from mardi_importer.integrator.MardiEntities import MardiItemEntity

from mardi_importer.zenodo.Community import Community

from dataclasses import dataclass, field
from typing import List

#{"identifier": "AA2-16", "relation": "isPartOf", "resource_type": "other", "scheme": "other"}

@dataclass
class Project:
    api: MardiIntegrator
    community : Community
    project_id : str
    project_title : str = None
    project_url : str = None
    instance_of : str = None
    QID : str = None

    def __post_init__(self):

        item = self.api.item.new()
        item.labels.set(language="en", value=self.project_id)


    def get_project_details(self):
        # manual setting up of project details from https://mathplus.de/research-2/application-areas/ and https://mathplus.de/research-2/emerging-fields/
    
        ###############################
        ###### Application Areas ######
        ###############################

        application_areas_url = "https://mathplus.de/research-2/application-areas/aa1-mechanisms-of-life/"
        application_areas_IDs = ["AA1-6", "AA1-10", "AA1-14", "AA1-15", "AA1-17", "AA1-18", "AA1-19", "AA1-20", 
        "AA2-7", "AA2-8", "AA2-12","AA2-13", "AA2-16", "AA2-17", "AA2-18", "AA2-19" , "AA2-20",
        "AA3-9", "AA3-12", "AA3-13", "AA3-15", "AA3-16", "AA3-17", "AA3-18",
        "AA4-2", "AA4-7", "AA4-8", "AA4-9", "AA4-10", "AA4-11", "AA4-12", "AA4-13", "AA4-14",
        "AA5-2", "AA5-4", "AA5-5", "AA5-6", "AA5-8", "AA5-9", "AA5-10", "AA5-11"]


        ###### Mechanisms of Life #####
        if self.project_id == "AA1-6":
            self.project_title = "Data-Driven Modeling from Atoms to Cells"
        if self.project_id == "AA1-10":
            self.project_title = "New Methods for Inhibiting Sars-Cov2 Cell Entry"
        if self.project_id == "AA1-14":
            self.project_title = "Development of an Ion-Channel Model-Framework for In-vitro Assisted Interpretation of Current Voltage Relations"
        if self.project.id == "AA1-15":
            self.project_title = "Math-Powered Drug-Design"
        if self.project_id == "AA1-17":
            self.project_title = "Beyond Attractors: Understanding the Transient and Modular Behaviour of Boolean Networks"
        if self.project_id == "AA1-18":
            self.project_title = "Synchronization and Geometric Structures of Stochastic Biochemical Oscillators"
        if self.project_id == "AA1-19":
            self.project_title = "Drug Candidates as Pareto Optima in Chemical Space"
        if self.project_id == "AA1-20":
            self.project_title = "Geometric Learning for Single-Cell RNA Velocity Modeling"
        ##### Nano and Quantum Technologies #####
        if self.project_id == "AA2-7":
            self.project_title = "Sparse Deep Neuronal Networks for the Design of Solar Energy Materials"
        if self.project_id == "AA2-8":
            self.project_title = "Deep Backflow for Accurate Solution of the Electronic Schrödinger Equation"
        if self.project_id == "AA2-12":
            self.project_title = "Nonlinear Electrokinetics in Anisotropic Microfluids – Analysis, Simulation, and Optimal Control"
        if self.project_id == "AA2-13":
            self.project_title = "Data-Driven Stochastic Modeling of Semiconductor Lasers"
        if self.project_id == "AA2-16":
            self.project_title = "Tailored Entangled Photon Sources for Quantum Technology"
        if self.project_id == "AA2-17":
            self.project_title = "Coherent Transport of Semiconductor Spin-Qubits: Modeling, Simulation and Optimal Control"
        if self.project_id == "AA2-18":
            self.project_title = "Pareto-Optimal Control of Quantum Thermal Devices with Deep Reinforcement Learning"
        if self.project_id == "AA2-19":
            self.project_title = "Entanglement Detection via Frank-Wolfe Algorithms"
        if self.project_id == "AA2-20":
            self.project_title = "Coarse-Graining Electrons in Quantum Systems"
        ##### Next generation networks #####
        if self.project_id == "AA3-9":
            self.project_title = "Information Design for Bayesian Networks"
        if self.project_id == "AA3-12":
            self.project_title = "On the Expressivity of Neural Networks"
        if self.project_id == "AA3-13":
            self.project_title = "Placing Steiner Points in Constrained Tetrahedrelizations"
        if self.project_id == "AA3-15":
            self.project_title = "Convex Solver Adaptivity for Mixed-Integer Optimization"
        if self.project_id == "AA3-16":
            self.project_title = "Likelihood Geometry of Max-Linear Bayesian Networks"
        if self.project_id == "AA3-17":
            self.project_title = "Efficient Algorithms for Quickest Transshipment Problems"
        if self.project_id == "AA3-18":
            self.project_title = "Evolution Processes for Populations and Economic Agents"
        ##### Energy Transition #####
        if self.project_id == "AA4-2":
            self.project_title = "Optimal Control in Energy Markets Using Rough Analysis and Deep Networks"
        if self.project_id == "AA4-7":
            self.project_title = "Decision-Making for Energy Network Dynamics"
        if self.project_id == "AA4-8":
            self.project_title = "Recovery of Battery Ageing Dynamics with Multiple Timescales"
        if self.project_id == "AA4-9":
            self.project_title = "Volatile Electricity Markets and Battery Storage: A Model-Based Approach for Optimal Control"
        if self.project_id == "AA4-19":
            self.project_title = "Modelling and Optimization of Weakly Coupled Minigrids under Uncertainty"
        if self.project_id == "AA4-11":
            self.project_title = "Using Mathematical Programming to Enhance Multiobjective Learning"
        if self.project_id == "AA4-12":
            self.project_title = "Advanced Modeling, Simulation, and Optimization of Large Scale Multi-Energy Systems"
        if self.project_id == "AA4-13":
            self.project_title = "Equilibria for Distributed Multi-Modal Energy Systems under Uncertainty"
        if self.project_id == "AA4-14":
            self.project_title = "Data-Driven Prediction of the Band-Gap for Perovskites"
        ##### Variational Problems in Data-Driven Applications #####
        if self.project_id == "AA5-2":
            self.project_title = "Robust Multilevel Training of Artificial Neural Networks"
        if self.project_id == "AA5-4":
            self.project_title = "Bayesian Optimization and Inference for Deep Networks"
        if self.project_id == "AA5-5":
            self.project_title = "Wasserstein Gradient Flows for Generalised Transport in Bayesian Inversion"
        if self.project_id == "AA5-6":
            self.project_title = "Convolutional Proximal Neural Networks for Solving Inverse Problems"
        if self.project_id == "AA5-8":
            self.project_title = "Convolutional Brenier Generative Networks"
        if self.project_id == "AA5-9":
            self.project_title = "LEAN on Me: Transforming Mathematics through Formal Verification, Improved Tactics, and Machine Learning"
        if self.project_id == "AA5-10":
            self.project_title = "Robust Data-Driven Reduced-Order Models for Cardiovascular Imaging of Turbulent Flows"
        if self.project_id == "AA5-11":
            self.project_title = "Data-Adaptive Discretization of Inverse Problems"

        #############################
        ###### Emerging Fields ######
        #############################

        emerging_fields_url = "https://mathplus.de/research-2/emerging-fields/"
        emerging_fields_IDs = ["EF1-10", "EF1-11", "EF1-12", "EF1-16", "EF1-19", "EF1-20", "EF1-21", "EF1-23", "EF1-24",
        ]

        ##### Extracting Dynamical Laws from Complex Data #####
        if self.project_id == "EF1-10":
            self.project_title = "Kernel Ensemble Kalman Filter and Inference"
        if self.project_id == "EF1-11":
            self.project_title = "Quantum Advantages in Machine Learning"
        if self.project_id == "EF1-12":
            self.project_title = "Learning Extremal Structures in Combinatorics"
        if self.project_id == "EF1-16":
            self.project_title = "Quiver Representations in Big Data and Machine Learning"
        if self.project_id == "EF1-19":
            self.project_title = "Machine Learning Enhanced Filtering Methods for Inverse Problems"
        if self.project_id == "EF1-20":
            self.project_title = "Uncertainty Quantification and Design of Experiment for Data-Driven Control"
        if self.project_id == "EF1-21":
            self.project_title = "Scaling up Flag Algebras in Combinatorics"
        if self.project_id == "EF1-23":
            self.project_title = "On a Frank-Wolfe Approach for Abs-Smooth Optimization"
        if self.project_id == "EF1-24":
            self.project_title = "Expanding Merlin-Arthur Classifiers Interpretable Neural Networks through Interactive Proof Systems"
        
        

        if self.project_id in application_areas_IDs:
            self.project_url = application_areas_url + self.project_id
        if self.project_id in emerging_fields_IDs:
            self.project_url = emerging_fields_url + self.project_id

    def get_project_ids(id_category = "all"):

        application_areas_IDs = ["AA1-6", "AA1-10", "AA1-14", "AA1-15", "AA1-17", "AA1-18", "AA1-19", "AA1-20", 
        "AA2-7", "AA2-8", "AA2-12","AA2-13", "AA2-16", "AA2-17", "AA2-18", "AA2-19" , "AA2-20",
        "AA3-9", "AA3-12", "AA3-13", "AA3-15", "AA3-16", "AA3-17", "AA3-18",
        "AA4-2", "AA4-7", "AA4-8", "AA4-9", "AA4-10", "AA4-11", "AA4-12", "AA4-13", "AA4-14",
        "AA5-2", "AA5-4", "AA5-5", "AA5-6", "AA5-8", "AA5-9", "AA5-10", "AA5-11"]

        emerging_fields_IDs = ["EF1-10", "EF1-11", "EF1-12", "EF1-16", "EF1-19", "EF1-20", "EF1-21", "EF1-23", "EF1-24",
        ]

        all_IDs = application_areas_IDs + emerging_fields_IDs

        if id_category == "EA":
            return emerging_fields_IDs
        else:
            if id_category == "AA":
                return application_areas_IDs
            else:
                return all_IDs

    def exists(self):
        
        if self.QID:
            return self.QID

        QID_results = self.api.get_local_id_by_label(self.project_id, "item")
        if QID_results: 
            self.QID = QID_results[0]

        if self.QID:
            print(f"Internal project exists with QID {self.QID}")
        return self.QID


    def create(self):

        if self.QID:
            return self.QID

        if not self.exists(): 
            item = self.api.item.new()
        else:
            print("Project already exists")
            item = self.api.item.get(entity_id=self.QID)

        if self.project_id:
            item.labels.set(language="en", value = self.project_id)
            descr = "Project " + self.project_id 
            if self.community.community_title:
                descr = descr + " in " + self.community.community_title
            if self.project_title:
                descr = descr + " ("+ self.project_title + ")"
            item.descriptions.set(language="en", value = descr)

        if self.community:
            prop_nr = self.api.get_local_id_by_label("community", "property")
            item.add_claim(prop_nr, self.community.QID)

        # instance of scientific project
        item_nr = self.api.get_local_id_by_label("scientific project", "item")[0]
        item.add_claim("wdt:P31", item_nr)

        self.QID = item.write().id

        if self.QID:
            print(f"Internal project with project id: {self.project_id} created with ID {self.QID}.")
            return self.QID
        else:
            print(f"Internal project with project id: {self.project_id} could not be created.")
            return None



