from mardi_importer.zbmath.misc import check_attribute

class ZBMathPublication:
    """ Class to manage zbMath publication items in the local Wikibase instance.

    Attributes: 

    title:
        Title of the paper
    """
    def __init__(self, integrator, title, doi, authors, journal, language, 
                 time,
                 links, creation_date, zbl_id):
        self.api = integrator
        self.title = title
        self.zbl_id = zbl_id
        self.QID = None
        self.language =language
        self.item = self.init_item()
        self.doi = doi
        self.authors = authors
        self.journal = journal
        self.time=time
        self.creation_date=creation_date
        self.links=links
        self.conflict_string = "zbMATH Open Web Interface contents unavailable due to conflicting licenses"


    def init_item(self):
        item = self.api.item.new()
        if self.language == "German":
            item.labels.set(language="de", value=self.title)
        else:
            item.labels.set(language="en", value=self.title)
        # item.descriptions.set(
        #     language="en", 
        #     value=self.description
        # )
        return item

    def create(self):
        # Instance of: scholary article
        self.item.add_claim('wdt:P31','wd:Q13442814')
        self.insert_claims()
        publication_id = self.item.write().id
        return(publication_id)
  

    def insert_claims(self):
        #title
        self.item.add_claim('wdt:P1476', text=self.title, language="en")
        #zbmath document id
        self.item.add_claim('wdt:P894', self.zbl_id)
        if check_attribute(self.doi, self.conflict_string):
            self.item.add_claim('wdt:P356', self.doi)
        author_claims = []
        for author in self.authors:
            claim = self.api.get_claim('wdt:P50', author)
            author_claims.append(claim)
        self.item.add_claims(author_claims)
        #published in journal
        #claim = self.api.get_claim('wdt:P1433', self.journal)
        if self.journal: 
            self.item.add_claim('wdt:P1433', self.journal)
        if self.creation_date:
            claim = self.api.get_claim('wdt:P577', time=self.creation_date)
            self.item.add_claims([claim])
        elif self.time:
            claim = self.api.get_claim('wdt:P577', time=self.time)
            self.item.add_claims([claim])

        if self.links:
            link_claims = []
            for link in self.links:
                claim = self.api.get_claim('wdt:P953', link)
                link_claims.append(claim)
            self.item.add_claims(link_claims)

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

    def update(self):
        """
        Check authors, journal, creation_date, time, links
        """
        self.item = self.api.item.get(entity_id=self.QID)
        
        self.insert_claims()
        self.item.write()

        if self.QID:
            print(f"Package with ID {self.QID} has been updated.")
            return self.QID
        else:
            print(f"Package could not be updated.")
            return None