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

        ###### Mechanisms of Life #####
        if self.project_id == "AA1-1":
            self.project_title = "The Spatio-Temporal Modelling of Mechanisms Underlying Pain Relief via the µ-Opioid Receptor"
        if self.project_id == "AA1-2":
            self.project_title = "Learning Transition Manifolds and Effective Dynamics of Biomolecules"
        if self.project_id == "AA1-3":
            self.project_title = "Stochastic Analysis of Particle Systems: Langevin Dynamics and the Dean-Kawasaki Model"
        if self.project_id == "AA1-4":
            self.project_title = "Algebraic Methods for Investigating Cell Fate Decisions"
        if self.project_id == "AA1-5":
            self.project_title = "Space-Time Stochastic Models for Neurotransmission Processes"
        if self.project_id == "AA1-6":
            self.project_title = "Data-Driven Modeling from Atoms to Cells"
        if self.project_id == "AA1-7":
            self.project_title = "Yeast Mating in Space and Time"
        if self.project_id == "AA1-8":
            self.project_title = "Random Bifurcations in Chemical Reaction Networks"
        if self.project_id == "AA1-9":
            self.project_title = "Polyhedral Geometry of Virus Capsids"
        if self.project_id == "AA1-10":
            self.project_title = "New Methods for Inhibiting Sars-Cov2 Cell Entry"
        if self.project_id == "AA1-11":
            self.project_title = "Receptor Dynamics and Interactions in Complex Geometries: An Inverse Problem in Particle-Based Reaction-Diffusion Theory"
        if self.project_id == "AA1-12":
            self.project_title = "Mathematical Modelling of Cellular Self-Organization on Stimuli Responsive Extracellular Matrix"
        if self.project_id == "AA1-13":
            self.project_title = "Predicting the Clinical Course of COVID-19 Patients"
        if self.project_id == "AA1-14":
            self.project_title = "Development of an Ion-Channel Model-Framework for In-vitro Assisted Interpretation of Current Voltage Relations"
        if self.project_id == "AA1-15":
            self.project_title = "Math-Powered Drug-Design"
        if self.project_id == "AA1-16":
            self.project_title = "Spatial Dynamics of Cell Signalling Explored with Agent-Based Modeling"
        if self.project_id == "AA1-17":
            self.project_title = "Beyond Attractors: Understanding the Transient and Modular Behaviour of Boolean Networks"
        if self.project_id == "AA1-18":
            self.project_title = "Synchronization and Geometric Structures of Stochastic Biochemical Oscillators"
        if self.project_id == "AA1-19":
            self.project_title = "Drug Candidates as Pareto Optima in Chemical Space"
        if self.project_id == "AA1-20":
            self.project_title = "Geometric Learning for Single-Cell RNA Velocity Modeling"
        ##### Nano and Quantum Technologies #####
        if self.project_id == "AA2-1":
            self.project_title = "Hybrid Models for the Electrothermal Behavior of Organic Semiconductor Devices"
        if self.project_id == "AA2-2":
            self.project_title = "Excitonic Energy Transport in Conjugated Polymer Chains"
        if self.project_id == "AA2-3":
            self.project_title = "Quantum-Classical Simulation of Quantum Dot Nanolasers"
        if self.project_id == "AA2-4":
            self.project_title = "Modeling and Analysis of Suspension Flows"
        if self.project_id == "AA2-5":
            self.project_title = "Data-Driven Electronic Structure Calculations for Nanostructures"
        if self.project_id == "AA2-6":
            self.project_title = "Modeling and Simulation of Multi-Material Electrocatalysis"
        if self.project_id == "AA2-7":
            self.project_title = "Sparse Deep Neuronal Networks for the Design of Solar Energy Materials"
        if self.project_id == "AA2-8":
            self.project_title = "Deep Backflow for Accurate Solution of the Electronic Schrödinger Equation"
        if self.project_id == "AA2-9":
            self.project_title = "Variational Methods for Viscoelastic Flows and Gelation"
        if self.project_id == "AA2-10":
            self.project_title = "Electro-Mechanical Coupling for Semiconductor Devices"
        if self.project_id == "AA2-11":
            self.project_title = "Multiscale Analysis of Exciton-Phonon Dynamics"
        if self.project_id == "AA2-12":
            self.project_title = "Nonlinear Electrokinetics in Anisotropic Microfluids – Analysis, Simulation, and Optimal Control"
        if self.project_id == "AA2-13":
            self.project_title = "Data-Driven Stochastic Modeling of Semiconductor Lasers"
        if self.project_id == "AA2-14":
            self.project_title = "Chiral Light-Matter Interaction for Quantum Photonic Devices"
        if self.project_id == "AA2-15":
            self.project_title = "Random Alloy Fluctuations in Semiconductors"
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
        if self.project_id == "AA2-21":
            self.project_title = "Strain Engineering for Functional Heterostructures: Aspects of Elasticity"
        ##### Next generation networks #####
        if self.project_id == "AA3-1":
            self.project_title = "Proximity of LP and IP Solutions"
        if self.project_id == "AA3-2":
            self.project_title = "Nash Flows over Time in Transport and Evacuation Simulation"
        if self.project_id == "AA3-3":
            self.project_title = "Discrete-Continuous Shortest Path Problems in Flight Planning"
        if self.project_id == "AA3-4":
            self.project_title = "Flow-Preserving Graph Contractions with Applications to Logistics Networks"
        if self.project_id == "AA3-5":
            self.project_title = "Tropical Mechanism Design"
        if self.project_id == "AA3-6":
            self.project_title = "Stochastic Scheduling with Restricted Adaptivity"
        if self.project_id == "AA3-7":
            self.project_title = "Beyond the Worst-Case: Data-Dependent Rates in Learning and Optimization"
        if self.project_id == "AA3-8":
            self.project_title = "The Tropical Geometry of Periodic Timetables"
        if self.project_id == "AA3-9":
            self.project_title = "Information Design for Bayesian Networks"
        if self.project_id == "AA3-12":
            self.project_title = "On the Expressivity of Neural Networks"
        if self.project_id == "AA3-13":
            self.project_title = "Placing Steiner Points in Constrained Tetrahedrelizations"
        if self.project_id == "AA3-14":
            self.poject_title = "The Structure within Disordered Cellular Materials"
        if self.project_id == "AA3-15":
            self.project_title = "Convex Solver Adaptivity for Mixed-Integer Optimization"
        if self.project_id == "AA3-16":
            self.project_title = "Likelihood Geometry of Max-Linear Bayesian Networks"
        if self.project_id == "AA3-17":
            self.project_title = "Efficient Algorithms for Quickest Transshipment Problems"
        if self.project_id == "AA3-18":
            self.project_title = "Evolution Processes for Populations and Economic Agents"
        if self.project_id == "AA3-19":
            self.project_title = "The Value of Distributional Information"
        ##### Energy Transition #####
        if self.project_id == "AA4-1":
            self.project_title = "PDAEs with Uncertainties for the Analysis, Simulation and Optimization of Energy Networks"
        if self.project_id == "AA4-2":
            self.project_title = "Optimal Control in Energy Markets Using Rough Analysis and Deep Networks"
        if self.project_id == "AA4-3":
            self.project_title = "Equilibria for Energy Markets with Transport"
        if self.project_id == "AA4-4":
            self.project_title = "Stochastic Modeling of Intraday Electricity Markets"  
        if self.project_id == "AA4-5":
            self.project_title = "Energy-Based Modeling, Simulation, and Optimization of Power Systems under Uncertainty"
        if self.project_id == "AA4-6":
            self.project_title = "Simulation and Optimization of Integrated Solar Fuels and Photovoltaics Devices"  
        if self.project_id == "AA4-7":
            self.project_title = "Decision-Making for Energy Network Dynamics"
        if self.project_id == "AA4-8":
            self.project_title = "Recovery of Battery Ageing Dynamics with Multiple Timescales"
        if self.project_id == "AA4-9":
            self.project_title = "Volatile Electricity Markets and Battery Storage: A Model-Based Approach for Optimal Control"
        if self.project_id == "AA4-10":
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
        if self.project_id == "AA5-1":
            self.project_title = "Sparsity and Sample-Size Efficiency in Structured Learning"
        if self.project_id == "AA5-2":
            self.project_title = "Robust Multilevel Training of Artificial Neural Networks"
        if self.project_id == "AA5-3":
            self.project_title = "Manifold-Valued Graph Neural Networks"
        if self.project_id == "AA5-4":
            self.project_title = "Bayesian Optimization and Inference for Deep Networks"
        if self.project_id == "AA5-5":
            self.project_title = "Wasserstein Gradient Flows for Generalised Transport in Bayesian Inversion"
        if self.project_id == "AA5-6":
            self.project_title = "Convolutional Proximal Neural Networks for Solving Inverse Problems"
        if self.project_id == "AA5-7":
            self.project_title = "Integrated Learning and Variational Methods for Quantitative Dynamic Imaging"
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

        ##### Extracting Dynamical Laws from Complex Data #####
        if self.project_id == "EF1-1":
            self.project_title = "Quantifying Uncertainties in Explainable AI"
        if self.project_id == "EF1-2":
            self.project_title = "Quantum Kinetics"
        if self.project_id == "EF1-3":
            self.project_title = "Approximate Convex Hulls With Bounded Complexity"
        if self.project_id == "EF1-4":
            self.project_title = "Extracting Dynamical Laws by Deep Neural Networks: A Theoretical Perspective"
        if self.project_id == "EF1-5":
            self.project_title = "On Robustness of Deep Neural Networks"
        if self.project_id == "EF1-6":
            self.project_title = "Graph Embedding for Analyzing the Microbiome"
        if self.project_id == "EF1-7":
            self.project_title = "Quantum Machine Learning"
        if self.project_id == "EF1-8":
            self.project_title = " Incorporating Locality into Fast(er) Learning"
        if self.project_id == "EF1-9":
            self.project_title = "Adaptive Algorithms through Machine Learning: Exploiting Interactions in Integer Programming"    
        if self.project_id == "EF1-10":
            self.project_title = "Kernel Ensemble Kalman Filter and Inference"
        if self.project_id == "EF1-11":
            self.project_title = "Quantum Advantages in Machine Learning"
        if self.project_id == "EF1-12":
            self.project_title = "Learning Extremal Structures in Combinatorics"
        if self.project_id == "EF1-13":
            self.project_title = "Stochastic and Rough Aspects in Deep Neural Networks"
        if self.project_id == "EF1-16":
            self.project_title = "Quiver Representations in Big Data and Machine Learning"
        if self.project_id == "EF1-17":
            self.project_title = "Data-Driven Robust Model Predictive Control under Distribution Shift"
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
        ##### Multi-agent social systems #####
        if self.project_id == "EF45-1":
            self.project_title = "Informing Opinion Dynamics Models with Online Social Network Data"
        if self.project_id == "EF45-2":
            self.project_title = "Effective Stochastic Simulation of Adaptive AB models"
        if self.project_id == "EF45-3":
            self.project_title = "Data Transmission in Dynamical Random Networks"
        if self.project_id == "EF45-4":
            self.project_title = "Hybrid Models for Large Scale Infection Spread Simulations"
        if self.project_id == "EF45-5":
            self.project_title = "A New Approach to Metastability in Multi-Agent Systems"
        if self.project_id == "EF4-5":
            self.project_title = "An Agent-Based Understanding of Green Growth"
        if self.project_id == "EF4-7":
            self.project_title = "The Impact of Dormancy on the Evolutionary, Ecological and Pathogenic Properties of Microbial Populations"
        if self.project_id == "EF4-8":
            self.project_title = "Concentration Effects and Collective Variables in Agent-Based Systems"
        if self.project_id == "EF4-10":
            self.project_title = "Coherent Movements in Co-evolving Agent–Message Systems"
        if self.project_id == "EF4-12":
            self.project_title = "Agent-Based Models of SARS-CoV2 Transmission: Multilevel Identification and Network-Based Reduction"
        if self.poject_id == "EF4-13":
            self.project_title = "Modeling Infection Spreading and Counter-Measures in a Pandemic Situation Using Coupled Models"
        if self.project_id == "EF5-6":
            self.project_title = "Evolution Models for Historical Networks"
        ##### Decision Support in the Public Sector #####
        if self.project_id == "EF6-1":
            self.project_title = "Heterogeneous Data Integration to Infer SARS-CoV-2 Variant-Specific Immunity for Risk Assessment and Vaccine Design"
        if self.project_id == "EF6-2":
            self.project_title = "Deliberation Processes in Citizens’ Assemblies: An Optimal Experimental Design Perspective"
        if self.project_id == "EF6-3":
            self.project_title = "Labour in an Agent-Based Understanding of Green Growth"
   

        if self.project_id in get_project_ids(id_category = "AA"):
            self.project_url = application_areas_url + self.project_id
        if self.project_id in get_project_ids(id_category = "EA"):
            self.project_url = emerging_fields_url + self.project_id

    def get_project_ids(id_category = "all"):

        application_areas_IDs = ["AA1-" + str(i) for i in range(1,21)] + ["AA2-" + str(i) for i in range(1,22)] + ["AA3-" + str(i) for i in range(1,20)] + ["AA4-" + str(i) for i in range(1,15)] + ["AA5-" + str(i) for i in range(1,12)]

        emerging_fields_IDs = ["EF1-" + str(i) for i in range(1,14)] + ["EF1-" + str(i) for i in [16,17,19,20,21,23,24]] + ["EF45-" + str(i) for i in range(1,6)] + ["EF4-" + str(i) for i in [5,7,8,10,12,13]] + ["EF5-6"] + ["EF6-1", "EF6-2", "EF6-3"]

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

        self.get_project_details()

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



