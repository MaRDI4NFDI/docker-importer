import sys

class OpenMLPublication:
    """ Class to manage OpenML publications in the local Wikibase instance.
    If there is already an item with this doi or arxiv id, it gets fetched.
    Attributes:
        integrator:
            MardiIntegrator instance
        identifier:
            arxiv id or doi
        identifier_type:
            'arxiv' or 'doi'
    """

    def __init__(
        self,
        integrator,
        identifier,
        identifier_type,
    ):
        self.api = integrator
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.item = self.api.item.new()
    
    def exists(self):
        """Checks if there is an item with that identifier in the local wikibase instance.
        Returns:
            String: Entity ID
        """
        if self.identifier_type == "doi":
            QID_list = self.api.search_entity_by_value(
                    "wdt:P356", self.identifier
                )
        elif self.identifier_type == "arxiv":
            QID_list = self.api.search_entity_by_value(
                    "wdt:P818", self.identifier
                )
        else:
            sys.exit(f"Invalid identifier type {self.identifier_type}")
        if not QID_list:
            self.QID = None
        else:
            self.QID = QID_list[0]
        if self.QID:
            print(f"Publication with qid {self.QID} exists")
        return(self.QID)

    def create(self):
        self.item.add_claim("wdt:P31", "wd:Q13442814")
        if self.identifier_type == "doi":
            self.item.add_claim("wdt:P356", self.identifier)
        elif self.identifier_type == "arxiv":
            self.item.add_claim("wdt:P356", self.identifier)
        profile_prop = self.api.get_local_id_by_label("MaRDI profile type", "property")
        profile_target = self.api.get_local_id_by_label("Publication", "item")[0]
        self.item.add_claim(profile_prop, profile_target)
        self.item.descriptions.set(language="en", value=f"scientific article about an OpenML dataset")
        publication_id = self.item.write().id
        print(f"Publication with the qid {publication_id} has been created.")
        return publication_id

