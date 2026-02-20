from mardi_importer import Importer

class ZBMathJournal:
    """Class to manage zbMath journal items in the local Wikibase instance.

    Attributes:
        api:
            MardiClient instance
        name:
            journal name

    """

    def __init__(self, name):
        self.name = name.strip()
        self.QID = None
        self.api = Importer.get_api('zbmath')
        self.item = self.init_item()

    def __post_init__(self):
        if self.api is None:
            self.api = Importer.get_api('zbmath')

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        item.descriptions.set(language="en", value="scientific journal")
        return item

    def create(self):
        # instance of: scientific journal
        self.item.add_claim("wdt:P31", "wd:Q5633421")
        journal_id = self.item.write().id
        return journal_id

    def exists(self):
        if self.QID:
            return self.QID
        # instance of: scientific journal
        self.QID = self.item.is_instance_of("wd:Q5633421")
        return self.QID

    def update(self):
        # doesnt have anything to update
        pass
