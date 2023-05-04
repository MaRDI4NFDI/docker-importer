class ZBMathJournal:
    def __init__(self, integrator, name):
        self.api = integrator
        self.name = name.strip()
        self.QID = None
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        return item  


    def create(self):
        #instance of: scientific journal
        self.item.add_claim("wdt:P31", "wd:Q5633421")
        journal_id = self.item.write().id
        return(journal_id)

    def exists(self):
        if self.QID: return self.QID
        self.QID = self.item.is_instance_of('wd:Q5633421')
        self.item.id = self.QID
        return self.QID

    def update(self):
        #doesnt have anything to update
        pass