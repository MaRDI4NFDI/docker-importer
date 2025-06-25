def has_rdf_type(onto):

    allowed_rdf_types = [
        'ComputationalTask',
        'MathematicalFormulation',
        'MathematicalModel',
        'Publication',
        'Quantity',
        'QuantityKind',
        'ResearchField', 
        'ResearchProblem'
    ]
    
    for ind in onto.individuals():
        name = str(ind).replace(f'{onto.name}.', '')
        rdf_type = str(ind.is_a[0]).replace(f'{onto.name}.', '')
        if rdf_type not in allowed_rdf_types:
            print(f'- {name} does not have a valid RDF type')
            
def has_description(onto):
    for ind in onto.individuals():
        name = str(ind).replace(f'{onto.name}.', '')
        if not ind.description:
            print(f'- {name} has no description annotation')
        elif not isinstance(ind.description[0], locstr):
            print(f'- {name} description is missing the xml:lang attribute')
        elif not any(d.lang == 'en' for d in ind.description):
            print(f'- {name} has no english description, i.e. missing xml:lang="en" attribute')

def has_label(onto):
    for ind in onto.individuals():
        name = str(ind).replace(f'{onto.name}.', '')
        if not ind.label:
            print(f'- {name} has no label annotation')
        elif not isinstance(ind.label[0], locstr):
            print(f'- {name} label is missing the xml:lang attribute')
        elif not any(d.lang == 'en' for d in ind.label):
            print(f'- {name} has no english label, i.e. missing xml:lang="en" attribute')
    


#has_rdf_type(onto)
has_label(onto)
#has_description(onto)