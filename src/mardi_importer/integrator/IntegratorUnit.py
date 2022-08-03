from cProfile import label
from urllib.parse import ParseResultBytes


class IntegratorUnit:
    """What this is supposed to be:
    Unit for a specific Wiki Item
    """

    def __init__(self, labels, descriptions, aliases, entity_type, claims, wikidata_id) -> None:
        self.labels = labels
        self.descriptions = descriptions
        self.aliases = aliases
        self.entity_type = entity_type
        self.claims = claims 
        self.wikidata_id = wikidata_id
        self.imported = False
        self.local_id = None


   # def check_local_instance(self):
    #    """Checks local wikibase instance to see if this Unit
    #    already exists;
    #    """
     #   wikibase_integrator
    #    pass

  #  def create(self):
  #      """Creates new entry set imported from wikidata"""
 #       #write returns something, does it give the new id??
 #       pass

   # def update(self):
  #      """Updates existing entity set imported from wikidata?"""
  #      pass
