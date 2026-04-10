from .misc import search_item_by_property
from mardi_importer import Importer

license_dict = {
    "http://arxiv.org/licenses/nonexclusive-distrib/1.0/": "Q6830561",
    "https://arxiv.org/licenses/nonexclusive-distrib/1.0/": "Q6830561",
    "https://creativecommons.org": "Q56978",
    "https://creativecommons.org/": "Q56978",
    "https://creativecommons.org/Licenses/by-nc-nd/4.0/": "Q6830565",
    "https://creativecommons.org/Licenses/by/4.0/": "Q57056",
    "https://creativecommons.org/licences/by/4.0/": "Q57056",
    "https://creativecommons.org/licenses/": "Q56978",
    "https://creativecommons.org/licenses/by-nc-by/4.0/": "Q57074",  # malformed, likely CC BY-NC 4.0
    "https://creativecommons.org/licenses/by-nc-nd/3.0/": "Q6830563",
    "https://creativecommons.org/licenses/by-nc-nd/4.0/": "Q6830565",
    "https://creativecommons.org/licenses/by-nc-sa/3.0/": "Q57076",
    "https://creativecommons.org/licenses/by-nc-sa/4.0/": "Q57078",
    "https://creativecommons.org/licenses/by-nc/2.0/": "Q6830583",
    "https://creativecommons.org/licenses/by-nc/2.5/": "Q6830578",  # Q26874140 not in portal
    "https://creativecommons.org/licenses/by-nc/2/": "Q6830583",  # malformed, likely CC BY-NC 2.0
    "https://creativecommons.org/licenses/by-nc/3.0/": "Q57072",
    "https://creativecommons.org/licenses/by-nc/4.0/": "Q57074",
    "https://creativecommons.org/licenses/by-nd/3.0/": "Q6830595",
    "https://creativecommons.org/licenses/by-nd/4.0/": "Q6830589",
    "https://creativecommons.org/licenses/by-sa/3.0/": "Q57029",
    "https://creativecommons.org/licenses/by-sa/4.0/": "Q57038",
    "https://creativecommons.org/licenses/by/1.0/": "Q6830600",
    "https://creativecommons.org/licenses/by/2.0/": "Q6830597",
    "https://creativecommons.org/licenses/by/2/": "Q6830597",  # malformed, likely CC BY 2.0
    "https://creativecommons.org/licenses/by/3.0/": "Q57050",
    "https://creativecommons.org/licenses/by/4.0/": "Q57056",
    "https://creativecommons.org/licenses/by/4.0/legalcode": "Q57056",
    "https://creativecommons.org/licenses/by/4/": "Q57056",  # malformed, likely CC BY 4.0
    "https://creativecommons.org/licenses/by/4:0/": "Q57056",  # malformed, likely CC BY 4.0
    "https://creativecommons.org/licenses/by/nc/4.0/": "Q57074",  # malformed, likely CC BY-NC 4.0
    "https://creativecommons.org/licenses/ny/4.0/": "Q57056",  # malformed, likely CC BY 4.0
    "https://creativecommons.org/publicdomain/mark/1.0/": "Q56990",
    "https://creativecommons.org/publicdomain/zero/1.0/": "Q56468",
    "http://creativecommons.org/publicdomain/zero/1.0/": "Q56468",
    "http://creativecommons.org/licenses/by/4.0/": "Q57056",
    "http://creativecommons.org/licenses/by/3.0/": "Q57050",
    "http://creativecommons.org/licenses/by-nc-sa/3.0/": "Q57076",
    "http://creativecommons.org/licenses/publicdomain/": "Q56990",
    "http://creativecommons.org/licenses/by-sa/4.0/": "Q57038",
    "http://creativecommons.org/licenses/by-nc-nd/4.0/": "Q6830565",
    "http://creativecommons.org/licenses/by-nc-sa/4.0/": "Q57078",
}


class ZBMathPublication:
    """Class to manage zbMath publication items in the local Wikibase instance.
    Attributes:
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
        arxiv_id:
            arxiv_id
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
        label_id_dict:
            dict mapping labels to ids for frequently searched items and properties
    """

    def __init__(
        self,
        title,
        doi,
        authors,
        author_name_strings,
        journal,
        language,
        time,
        links,
        creation_date,
        zbl_id,
        arxiv_id,
        review_text,
        reviewer,
        classifications,
        de_number,
        keywords,
        label_id_dict,
        licenses
    ):
        self.title = title
        self.zbl_id = zbl_id
        self.arxiv_id = arxiv_id
        self.QID = None
        self.language = language
        self.doi = doi
        if self.doi:
            self.doi = self.doi.upper()
        self.authors = authors
        self.author_name_strings = author_name_strings
        self.journal = journal
        self.time = time
        self.creation_date = creation_date
        self.links = links
        self.review_text = review_text
        self.reviewer = reviewer
        self.classifications = classifications
        self.de_number = de_number
        self.keywords = keywords
        self.label_id_dict = label_id_dict
        self.licenses = licenses
        self.api = Importer.get_api('zbmath')
        self.item = self.init_item()

    def __post_init__(self):
        if self.api is None:
            self.api = Importer.get_api('zbmath')

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.title)
        item.descriptions.set(
            language="en",
            value=f"scientific article; zbMATH DE number {self.de_number}",
        )
        return item

    def create(self):
        # Instance of: scholary article
        self.item.add_claim("wdt:P31", "wd:Q13442814")
        self.insert_claims()
        publication_id = self.item.write().id
         # If arxiv_id exists, find the arXiv item and link it back
        if self.arxiv_id:
            qid_list = self.api.search_entity_by_value("P21", self.arxiv_id)
            for qid in qid_list:
                arxiv_item = self.api.item.get(entity_id=qid)
                arxiv_item.add_claim("P1676", publication_id)  # is preprint of
                arxiv_item.write()
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
        author_string_claims = []
        for author_string in self.author_name_strings:
            claim = self.api.get_claim("P43", author_string)
            author_string_claims.append(claim)
        self.item.add_claims(author_string_claims)
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
            prop_nr = self.label_id_dict["review_prop"]
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
            self.item.add_claim(self.label_id_dict["de_number_prop"], self.de_number)
        if self.licenses:
            license_claims = []
            for l in self.licenses:
                license_qid = license_dict.get(l)
                if not license_qid:
                    continue
                claim = self.api.get_claim("P163", license_qid)
                license_claims.append(claim)
            if license_claims:
                self.item.add_claims(license_claims)
        if self.keywords:
            kw_claims = []
            for k in self.keywords:
                claim = self.api.get_claim(self.label_id_dict["keyword_prop"], k)
                kw_claims.append(claim)
            self.item.add_claims(kw_claims)
        profile_prop = self.label_id_dict["mardi_profile_type_prop"]
        profile_target = self.label_id_dict["mardi_publication_profile_item"]
        self.item.add_claim(profile_prop, profile_target)

    def is_arxiv(self):
        if self.zbl_id:
            if "arXiv" in self.zbl_id:
                return True
        return False


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
        self.QID = search_item_by_property(property_id = self.label_id_dict["de_number_prop"], value=self.de_number)
        if not self.QID:
            if self.arxiv_id:
                QID_list = self.api.search_entity_by_value("wdt:P818", self.arxiv_id)
                self.QID = QID_list[0] if QID_list else None
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
