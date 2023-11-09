class ZBMathPublication:
    """Class to manage zbMath publication items in the local Wikibase instance.
    Attributes:
        integrator:
            MardiIntegrator instance
        title:
            publication title
        doi:
            DOI
        authors:
            list of local author ids
        journal:
            local journal id
        language:
            publication language
        time:
            time of publication
        links:
            list of links to publication
        creation_date:
            creation date of entry
        zbl_id:
            zbl_id
        review_text:
            review text
        reviewer:
         zbmath author ID of reviewer
        classifications:
            mathematics subject classification ids
        de_number:
            zbmath de number
        keywords:
            zbmath topic keywords
        de_number_prop:
            local property ID of zbmath de number
        keyword_prop:
            local property ID of keyword property number
    """

    def __init__(
        self,
        integrator,
        title,
        doi,
        authors,
        journal,
        language,
        time,
        links,
        creation_date,
        zbl_id,
        review_text,
        reviewer,
        classifications,
        de_number,
        keywords,
        de_number_prop,
        keyword_prop,
    ):
        self.api = integrator
        self.title = title
        self.zbl_id = zbl_id
        self.QID = None
        self.language = language
        self.doi = doi
        self.authors = authors
        self.journal = journal
        self.time = time
        self.creation_date = creation_date
        self.links = links
        self.review_text = review_text
        self.reviewer = reviewer
        self.classifications = classifications
        self.de_number = de_number
        self.keywords = keywords
        self.de_number_prop = de_number_prop
        self.keyword_prop = keyword_prop
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.title)
        item.descriptions.set(language="en", value=f"scientific article; zbMATH DE number {self.de_number}")
        return item

    def create(self):
        # Instance of: scholary article
        self.item.add_claim("wdt:P31", "wd:Q13442814")
        self.insert_claims()
        publication_id = self.item.write().id
        return publication_id

    def insert_claims(self):
        # title
        if self.title:
            self.item.add_claim("wdt:P1476", self.title, language="en")
        # zbmath document id
        if self.zbl_id:
            self.item.add_claim("wdt:P894", self.zbl_id)
        if self.doi:
            self.item.add_claim("wdt:P356", self.doi)
        author_claims = []
        for author in self.authors:
            claim = self.api.get_claim("wdt:P50", author)
            author_claims.append(claim)
        self.item.add_claims(author_claims)
        if self.journal:
            self.item.add_claim("wdt:P1433", self.journal)
        if self.creation_date:
            claim = self.api.get_claim("wdt:P577", self.creation_date)
            self.item.add_claims([claim])
        elif self.time:
            claim = self.api.get_claim("wdt:P577", self.time)
            self.item.add_claims([claim])
        if self.links:
            link_claims = []
            for link in self.links:
                claim = self.api.get_claim("wdt:P953", link)
                link_claims.append(claim)
            self.item.add_claims(link_claims)
        if self.review_text:
            prop_nr = self.api.get_local_id_by_label("review text", "property")
            self.item.add_claim(prop_nr, self.review_text)
        if self.reviewer:
            self.item.add_claim("wdt:P4032", self.reviewer)
        if self.classifications:
            classification_claims = []
            for c in self.classifications:
                claim = self.api.get_claim("wdt:P3285", c)
                classification_claims.append(claim)
            self.item.add_claims(classification_claims)
        if self.de_number:
            self.item.add_claim(self.de_number_prop, self.de_number)
        if self.keywords:
            kw_claims = []
            for k in self.keywords:
                print(self.keyword_prop)
                print(k)
                claim = self.api.get_claim(self.keyword_prop, k)
                kw_claims.append(claim)
            self.item.add_claims(kw_claims)

    def exists(self):
        """Checks if a WB item corresponding to the publication already exists.
        Searches for a WB item with the package label in the SQL Wikibase
        tables and returns **True** if a matching result is found.
        It uses for that the :meth:`mardi_importer.wikibase.WBItem.instance_exists()`
        method.
        Returns:
          String: Entity ID
        """
        if self.QID:
            return self.QID
        # instance of scholarly article
        if self.title:
            self.QID = self.item.is_instance_of_with_property(
                "wd:Q13442814", "P1451", self.de_number
            )
        else:
            QID_list = self.api.search_entity_by_value(
                self.de_number_prop, self.de_number
            )
            if not QID_list:
                self.QID = None
            else:
                # should not be more than one
                self.QID = QID_list[0]
        return self.QID

    def update(self):
        """
        Update existing item.
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
