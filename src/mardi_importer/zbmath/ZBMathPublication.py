from mardi_importer.zbmath.misc import check_attribute

class ZBMathPublication:
    """ Class to manage zbMath publication items in the local Wikibase instance.

    Attributes: 

    title:
        Title of the paper
    """
    def __init__(self, integrator, title, doi, authors, journal):
        self.api = integrator
        self.title = title
        self.QID = None
        self.item = self.init_item()
        self.doi = doi
        self.authors = authors
        self.journal = journal
        self.conflict_string = "zbMATH Open Web Interface contents unavailable due to conflicting licenses"

    def create(self):
        # Instance of: scholary article
        self.item.add_claim('wdt:P31','wd:Q13442814')
        #title
        self.item.add_claim('wdt:P1476', text=self.title, language="en")
        if check_attribute(self.doi, self.conflict_string):
            self.item.add_claim('wdt:P356', self.doi)
        author_claims = []
        for author in self.authors:
            claim = self.api.get_claim('wdt:P50', author)
            author_claims.append(claim)
        self.item.add_claims(author_claims)
        #published in journal
        #claim = self.api.get_claim('wdt:P1433', self.journal)
        self.item.add_claim('wdt:P1433', self.journal)
        publication_id = self.item.write().id
        return(publication_id)

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.title)
        # item.descriptions.set(
        #     language="en", 
        #     value=self.description
        # )
        return item  

    def exists(self):
        """Checks if a WB item corresponding to the publication already exists.
        Searches for a WB item with the package label in the SQL Wikibase
        tables and returns **True** if a matching result is found.
        It uses for that the :meth:`mardi_importer.wikibase.WBItem.instance_exists()` 
        method.
        Returns: 
          String: Entity ID
        """
        if self.QID: return self.QID
        self.QID = self.item.is_instance_of('wd:Q13442814')
        self.item.id = self.QID
        print("Publication exists")
        return self.QID