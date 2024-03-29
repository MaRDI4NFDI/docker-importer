class ZBMathJournal:
    """Class to manage zbMath journal items in the local Wikibase instance.

    Attributes:
        integrator:
            MardiIntegrator instance
        name:
            journal name

    """

    def __init__(self, integrator, name):
        self.api = integrator
        self.name = name.strip()
        self.QID = None
        self.item = self.init_item()

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
