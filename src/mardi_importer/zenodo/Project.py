from mardiclient import MardiClient
from mardi_importer.zenodo.Community import Community

from dataclasses import dataclass, field
from typing import Dict

class Project:

    ###############################
    ###### Application Areas ######
    ###############################
    application_areas: Dict[str, str] = {
        ###### Mechanisms of Life #####
        "AA1-1": "The Spatio-Temporal Modelling of Mechanisms Underlying Pain Relief via the µ-Opioid Receptor",
        "AA1-2": "Learning Transition Manifolds and Effective Dynamics of Biomolecules",
        "AA1-3": "Stochastic Analysis of Particle Systems: Langevin Dynamics and the Dean-Kawasaki Model",
        "AA1-4": "Algebraic Methods for Investigating Cell Fate Decisions",
        "AA1-5": "Space-Time Stochastic Models for Neurotransmission Processes",
        "AA1-6": "Data-Driven Modeling from Atoms to Cells",
        "AA1-7": "Yeast Mating in Space and Time",
        "AA1-8": "Random Bifurcations in Chemical Reaction Networks",
        "AA1-9": "Polyhedral Geometry of Virus Capsids",
        "AA1-10": "New Methods for Inhibiting Sars-Cov2 Cell Entry",
        "AA1-11": "Receptor Dynamics and Interactions in Complex Geometries: An Inverse Problem in Particle-Based Reaction-Diffusion Theory",
        "AA1-12": "Mathematical Modelling of Cellular Self-Organization on Stimuli Responsive Extracellular Matrix",
        "AA1-13": "Predicting the Clinical Course of COVID-19 Patients",
        "AA1-14": "Development of an Ion-Channel Model-Framework for In-vitro Assisted Interpretation of Current Voltage Relations",
        "AA1-15": "Math-Powered Drug-Design",
        "AA1-16": "Spatial Dynamics of Cell Signalling Explored with Agent-Based Modeling",
        "AA1-17": "Beyond Attractors: Understanding the Transient and Modular Behaviour of Boolean Networks",
        "AA1-18": "Synchronization and Geometric Structures of Stochastic Biochemical Oscillators",
        "AA1-19": "Drug Candidates as Pareto Optima in Chemical Space",
        "AA1-20": "Geometric Learning for Single-Cell RNA Velocity Modeling",
        ##### Nano and Quantum Technologies #####
        "AA2-1": "Hybrid Models for the Electrothermal Behavior of Organic Semiconductor Devices",
        "AA2-2": "Excitonic Energy Transport in Conjugated Polymer Chains",
        "AA2-3": "Quantum-Classical Simulation of Quantum Dot Nanolasers",
        "AA2-4": "Modeling and Analysis of Suspension Flows",
        "AA2-5": "Data-Driven Electronic Structure Calculations for Nanostructures",
        "AA2-6": "Modeling and Simulation of Multi-Material Electrocatalysis",
        "AA2-7": "Sparse Deep Neuronal Networks for the Design of Solar Energy Materials",
        "AA2-8": "Deep Backflow for Accurate Solution of the Electronic Schrödinger Equation",
        "AA2-9": "Variational Methods for Viscoelastic Flows and Gelation",
        "AA2-10": "Electro-Mechanical Coupling for Semiconductor Devices",
        "AA2-11": "Multiscale Analysis of Exciton-Phonon Dynamics",
        "AA2-12": "Nonlinear Electrokinetics in Anisotropic Microfluids - Analysis, Simulation, and Optimal Control",
        "AA2-13": "Data-Driven Stochastic Modeling of Semiconductor Lasers",
        "AA2-14": "Chiral Light-Matter Interaction for Quantum Photonic Devices",
        "AA2-15": "Random Alloy Fluctuations in Semiconductors",
        "AA2-16": "Tailored Entangled Photon Sources for Quantum Technology",
        "AA2-17": "Coherent Transport of Semiconductor Spin-Qubits: Modeling, Simulation and Optimal Control",
        "AA2-18": "Pareto-Optimal Control of Quantum Thermal Devices with Deep Reinforcement Learning",
        "AA2-19": "Entanglement Detection via Frank-Wolfe Algorithms",
        "AA2-20": "Coarse-Graining Electrons in Quantum Systems",
        "AA2-21": "Strain Engineering for Functional Heterostructures: Aspects of Elasticity",
        ##### Next generation networks #####
        "AA3-1": "Proximity of LP and IP Solutions",
        "AA3-2": "Nash Flows over Time in Transport and Evacuation Simulation",
        "AA3-3": "Discrete-Continuous Shortest Path Problems in Flight Planning",
        "AA3-4": "Flow-Preserving Graph Contractions with Applications to Logistics Networks",
        "AA3-5": "Tropical Mechanism Design",
        "AA3-6": "Stochastic Scheduling with Restricted Adaptivity",
        "AA3-7": "Beyond the Worst-Case: Data-Dependent Rates in Learning and Optimization",
        "AA3-8": "The Tropical Geometry of Periodic Timetables",
        "AA3-9": "Information Design for Bayesian Networks",
        "AA3-12": "On the Expressivity of Neural Networks",
        "AA3-13": "Placing Steiner Points in Constrained Tetrahedrelizations",
        "AA3-14": "The Structure within Disordered Cellular Materials",
        "AA3-15": "Convex Solver Adaptivity for Mixed-Integer Optimization",
        "AA3-16": "Likelihood Geometry of Max-Linear Bayesian Networks",
        "AA3-17": "Efficient Algorithms for Quickest Transshipment Problems",
        "AA3-18": "Evolution Processes for Populations and Economic Agents",
        "AA3-19": "The Value of Distributional Information",
        ##### Energy Transition #####
        "AA4-1": "PDAEs with Uncertainties for the Analysis, Simulation and Optimization of Energy Networks",
        "AA4-2": "Optimal Control in Energy Markets Using Rough Analysis and Deep Networks",
        "AA4-3": "Equilibria for Energy Markets with Transport",
        "AA4-4": "Stochastic Modeling of Intraday Electricity Markets",
        "AA4-5": "Energy-Based Modeling, Simulation, and Optimization of Power Systems under Uncertainty",
        "AA4-6": "Simulation and Optimization of Integrated Solar Fuels and Photovoltaics Devices",
        "AA4-7": "Decision-Making for Energy Network Dynamics",
        "AA4-8": "Recovery of Battery Ageing Dynamics with Multiple Timescales",
        "AA4-9": "Volatile Electricity Markets and Battery Storage: A Model-Based Approach for Optimal Control",
        "AA4-10": "Modelling and Optimization of Weakly Coupled Minigrids under Uncertainty",
        "AA4-11": "Using Mathematical Programming to Enhance Multiobjective Learning",
        "AA4-12": "Advanced Modeling, Simulation, and Optimization of Large Scale Multi-Energy Systems",
        "AA4-13": "Equilibria for Distributed Multi-Modal Energy Systems under Uncertainty",
        "AA4-14": "Data-Driven Prediction of the Band-Gap for Perovskites",
        ##### Variational Problems in Data-Driven Applications #####
        "AA5-1": "Sparsity and Sample-Size Efficiency in Structured Learning",
        "AA5-2": "Robust Multilevel Training of Artificial Neural Networks",
        "AA5-3": "Manifold-Valued Graph Neural Networks",
        "AA5-4": "Bayesian Optimization and Inference for Deep Networks",
        "AA5-5": "Wasserstein Gradient Flows for Generalised Transport in Bayesian Inversion",
        "AA5-6": "Convolutional Proximal Neural Networks for Solving Inverse Problems",
        "AA5-7": "Integrated Learning and Variational Methods for Quantitative Dynamic Imaging",
        "AA5-8": "Convolutional Brenier Generative Networks",
        "AA5-9": "LEAN on Me: Transforming Mathematics through Formal Verification, Improved Tactics, and Machine Learning",
        "AA5-10": "Robust Data-Driven Reduced-Order Models for Cardiovascular Imaging of Turbulent Flows",
        "AA5-11": "Data-Adaptive Discretization of Inverse Problems",
    }
    
    #############################
    ###### Emerging Fields ######
    #############################
    emerging_fields: Dict[str, str] = {
        ##### Extracting Dynamical Laws from Complex Data #####
        "EF1-1": "Quantifying Uncertainties in Explainable AI",
        "EF1-2": "Quantum Kinetics",
        "EF1-3": "Approximate Convex Hulls With Bounded Complexity",
        "EF1-4": "Extracting Dynamical Laws by Deep Neural Networks: A Theoretical Perspective",
        "EF1-5": "On Robustness of Deep Neural Networks",
        "EF1-6": "Graph Embedding for Analyzing the Microbiome",
        "EF1-7": "Quantum Machine Learning",
        "EF1-8": "Incorporating Locality into Fast(er) Learning",
        "EF1-9": "Adaptive Algorithms through Machine Learning: Exploiting Interactions in Integer Programming",   
        "EF1-10": "Kernel Ensemble Kalman Filter and Inference",
        "EF1-11": "Quantum Advantages in Machine Learning",
        "EF1-12": "Learning Extremal Structures in Combinatorics",
        "EF1-13": "Stochastic and Rough Aspects in Deep Neural Networks",
        "EF1-16": "Quiver Representations in Big Data and Machine Learning",
        "EF1-17": "Data-Driven Robust Model Predictive Control under Distribution Shift",
        "EF1-19": "Machine Learning Enhanced Filtering Methods for Inverse Problems",
        "EF1-20": "Uncertainty Quantification and Design of Experiment for Data-Driven Control",
        "EF1-21": "Scaling up Flag Algebras in Combinatorics",
        "EF1-23": "On a Frank-Wolfe Approach for Abs-Smooth Optimization",
        "EF1-24": "Expanding Merlin-Arthur Classifiers Interpretable Neural Networks through Interactive Proof Systems",
        ##### Multi-agent social systems #####
        "EF45-1": "Informing Opinion Dynamics Models with Online Social Network Data",
        "EF45-2": "Effective Stochastic Simulation of Adaptive AB models",
        "EF45-3": "Data Transmission in Dynamical Random Networks",
        "EF45-4": "Hybrid Models for Large Scale Infection Spread Simulations",
        "EF45-5": "A New Approach to Metastability in Multi-Agent Systems",
        "EF4-5": "An Agent-Based Understanding of Green Growth",
        "EF4-7": "The Impact of Dormancy on the Evolutionary, Ecological and Pathogenic Properties of Microbial Populations",
        "EF4-8": "Concentration Effects and Collective Variables in Agent-Based Systems",
        "EF4-10": "Coherent Movements in Co-evolving Agent-Message Systems",
        "EF4-12": "Agent-Based Models of SARS-CoV2 Transmission: Multilevel Identification and Network-Based Reduction",
        "EF4-13": "Modeling Infection Spreading and Counter-Measures in a Pandemic Situation Using Coupled Models",
        "EF5-6": "Evolution Models for Historical Networks",
        ##### Decision Support in the Public Sector #####
        "EF6-1": "Heterogeneous Data Integration to Infer SARS-CoV-2 Variant-Specific Immunity for Risk Assessment and Vaccine Design",
        "EF6-2": "Deliberation Processes in Citizens' Assemblies: An Optimal Experimental Design Perspective",
        "EF6-3": "Labour in an Agent-Based Understanding of Green Growth",
    }

    application_areas_url = "https://mathplus.de/research-2/application-areas/aa1-mechanisms-of-life/"
    emerging_fields_url = "https://mathplus.de/research-2/emerging-fields/"

    def __init__(self, api, community, project_id):

        self.api = api
        self.community = community
        self.project_id = project_id
        self.QID : str = None       
   
        if project_id in self.__class__.application_areas.keys():
            self.project_title = self.__class__.application_areas[project_id]
            self.project_url = self.__class__.application_areas_url + project_id
        elif project_id in self.__class__.emerging_fields.keys():
            self.project_title = self.__class__.emerging_fields[project_id]
            self.project_url = self.__class__.emerging_fields_url + project_id

    @classmethod
    def get_project_ids(cls):
        return list(cls.application_areas.keys()) + list(cls.emerging_fields.keys())

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



