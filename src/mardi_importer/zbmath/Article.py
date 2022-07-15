from mardi_importer.wikibase.WBItem import WBItem


class Article:
    """Class to manage Article items in the local Wikibase instance.

    Attributes:


    """

    def __init__(
        self,
        label,
        zbmath_id,
        authors,
        title,
        journal,
        journal_edition,
        journal_pages,
        classifications,
        language,
        url,
        keywords,
        doi,
        publication_year,
    ):
        self.label = label
        self.zbmath_id = zbmath_id
        self.authors = authors
        self.title = title
        self.journal = journal
        self.journal_edition = journal_edition
        self.journal_pages = journal_pages
        self.classifications = classifications
        self.language = language
        self.url = url
        self.keywords = keywords
        self.doi = doi
        self.publication_year = publication_year

    def exists_in_wikidata(self):
        """
        Checks if a WB item corresponding to the article exists in Wikidat; if yes, it gets imported.
        Searches by label.
        """
        pass

    def exists(self):
        """
        Checks if a WB item corresponding to the article already exists.

        Searches for a WB item with the article label and returns **True**
        if a matching result is found.
        """
        item = WBItem(self.label)
        return not not item.SQL_exists()

    def import_item(self):
        """
        Imports article item from wikidata
        """
        pass

    def create(self):
        """Creates a WB item with the imported article metadata from zbMath.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the
        corresponding new item.
        """
        # Create publisher Item with label
        item = WBItem(self.label)

        # instance of: scholarly article
        item.add_statement("?", "?")

        # zbmath  id: self.zbmath_id
        item.add_statement("?", self.zbmath_id)

        for author in self.authors:
            # author: author
            item.add_statement("?", author)

        # title:
        item.add_statement("?", self.title)

        # published in:
        item.add_statement("?", self.journal)

        # volume:
        item.add_statement("?", self.journal_edition)

        # page(s)
        item.add_statement("?", self.journal_pages)

        for classification in self.classifications:
            # Mathematics Subject Classification ID
            item.add_statement("?", classification)

        # language of work or name
        item.add_statement("?", self.language)

        # url
        item.add_statement("?", self.url)

        # main subject
        for keyword in self.keywords:
            item.add_statement("?", keyword)

        # doi
        item.add_statement("?", self.doi)

        # publication year
        item.add_statement("?", self.publication_year)
