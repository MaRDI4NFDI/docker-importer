class ZBMathAuthor:
    """
    Class to merge zbMath author items in the local wikibase instance
    """
    def __init__(self, integrator, name, zbmath_author_id):
        self.api = integrator
        self.name = name.strip()
        self.QID = None
        self.zbmath_author_id = zbmath_author_id.strip()
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        return item  


    def create(self):
        #instance of: human
        self.item.add_claim("wdt:P31", "wd:Q5")
        self.item.add_claim('wdt:P1556', self.zbmath_author_id)
        author_id = self.item.write().id
        return(author_id)

    def exists(self):
        if self.QID: return self.QID
        self.QID = self.item.is_instance_of_with_property('wd:Q5', 'wdt:P1556', self.zbmath_author_id)
        self.item.id = self.QID
        return self.QID

    def update(self):
        #author does not have an update function, 
        #because it has no attributes that could be updated
        pass
    