import json

def get_individuals():
    filename = 'MaRDIModelOntology'
    data_properties = [
        'isDeterministic',
        'isTimeContinuous',
        'isDynamic',
        'isSpaceContinuous',
        'isLinear',
        'isDimensionless',
        'isMathematicalConstant',
        'isPhysicalConstant',
        'isChemicalConstant'
    ]
    object_properties = [
        'usedBy',
        'specializedBy',
        'approximatedBy',
        'discretizedBy',
        'linearizedBy',
        'modeledBy',
        'nondimensionalizedBy',
        'assumes',
    ]
    identifiers = ['arxivID', 'qudtID', 'wikidataID', 'mardiID', 'doiID']
    onto = get_ontology(f'{filename}.owl').load()
    individuals = {}

    # Iterate over all named individuals in the ontology
    for ind in onto.individuals():

        individual = {}
        name = str(ind).replace(f'{onto.name}.', '')

        # RDF type
        rdf_type = str(ind.is_a[0]).replace(f'{onto.name}.', '')
        individual['rdfType'] = rdf_type
        
        # Extract the english label
        individual['label'] = next((str(c) for c in (ind.label or []) if c.lang == 'en'), None)

        # Extract the english description
        individual['description'] = [str(c) for c in (ind.description or []) if c.lang == 'en']

        # Extract the english comment
        individual['longDescription'] = [str(c) for c in (ind.comment or []) if c.lang == 'en']

        # Extract aliases
        individual['aliases'] = list(map(str, ind.altLabel)) if ind.altLabel else []

        # See also URL
        if ind.seeAlso:
            individual['seeAlso'] = ind.seeAlso

        # Identifiers
        for id_name in identifiers:
            if getattr(ind, id_name):
                if len(getattr(ind, id_name)) > 1 and id_name == 'doiID' and rdf_type != 'Publication':
                    individual['publications'] = []
                    for doi in getattr(ind, id_name):
                        individual['publications'].append(doi)
                elif id_name == 'doiID' and rdf_type != 'Publication':
                    individual['publications'] = [getattr(ind, id_name)[0]]
                else:
                    individual[id_name] = getattr(ind, id_name)[0]

        # Extract Data Properties
        for data_prop in data_properties:        
            if getattr(ind, data_prop):
                value = getattr(ind, data_prop)[0]
                individual[data_prop] = value

        # Extract definingFormulation
        if getattr(ind, "definingFormulation"):
            defining_formulation = str(ind.definingFormulation[0])   
            individual['definingFormulation'] = defining_formulation
            
        # Extract inDefiningFormulation
        if getattr(ind, "inDefiningFormulation"):
            in_defining_formulations = [str(f) for f in getattr(ind, "inDefiningFormulation")]
            individual['inDefiningFormulation'] = in_defining_formulations

        # Object properties
        for prop in object_properties:
            if getattr(ind, prop):
                individual[prop] = []
                for attr in getattr(ind, prop):
                    individual[prop].append(str(attr).replace(f'{onto.name}.', ''))

        # Contains
        contains = []
        contains_mapping = {
            'contains': None,
            'containsConstraintCondition': 'constraint_condition',
            'containsInitialCondition': 'initial_condition',
            'containsFinalCondition': 'final_condition',
            'containsBoundaryCondition': 'boundary_condition',
            'containsCouplingCondition': 'coupling_condition',
            'containsInput': 'input',
            'containsOutput': 'output',
            'containsParameter': 'parameter',
            'containsObjective': 'objective',
            'containsConstant': 'constant'
        }
        for prop, role in contains_mapping.items():
            contains = add_attributes(ind, contains, prop, role)
        if len(contains) > 0:
            individual['contains'] = contains

        # Described by
        described_by = []
        described_mapping = {
            'describedAsStudiedBy': 'study',
            'describedAsInventedBy': 'invention',
            'describedAsDocumentedBy': 'document',
            'describedAsSurveyedBy': 'survey'
        }
        for prop, role in described_mapping.items():
            contains = add_attributes(ind, described_by, prop, role)
        if len(described_by) > 0:
            individual['describedBy'] = described_by

        individuals[name] = individual

    return individuals

individuals = get_individuals()

with open('individuals.json', 'w') as f:
    json.dump(individuals, f)