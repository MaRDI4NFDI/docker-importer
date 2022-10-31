from cProfile import label
from urllib.parse import ParseResultBytes


class IntegratorUnit:
    """
    Unit for a specific Wiki Item
    """

    def __init__(
        self, labels, descriptions, aliases, entity_type, claims, wikidata_id, datatype
    ) -> None:
        self.labels = labels
        self.descriptions = descriptions
        self.aliases = aliases
        self.entity_type = entity_type
        self.claims = claims
        self.wikidata_id = wikidata_id
        self.datatype = datatype
        self.imported = False
        self.local_id = None
