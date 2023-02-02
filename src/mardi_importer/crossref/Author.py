
class Author:
    def __init__(self, name, integrator):
        self.name = name
        self.orcid = ""
        self.api = integrator
        self.item = self.init_item()
        
    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        item.add_claim("wdt:P31", "wd:Q5")
        return item 

    def add_orcid(self,orcid):
        self.item.add_claim('wdt:P496', orcid)

    def create(self):
        self.item.write()
        return self.item.id

    def compare_names(self, author):
        if self.name == author:
            return self.name
        author_1_vec = self.name.split()
        author_2_vec = author.split()
        if author_1_vec[0] == author_2_vec[0]:
            if len(author_1_vec) > 2:
                if author_1_vec[2] == author_2_vec[1]:
                    return self.name
            if len(author_2_vec) > 2:
                if author_1_vec[1] == author_2_vec[2]:
                    return author
            if len(author_1_vec) > 2 and len(author_2_vec) > 2:
                if author_1_vec[0] == author_2_vec[0] and author_1_vec[2] == author_2_vec[2] and author_1_vec[1][0] == author_2_vec[1][0] and len(author_1_vec[1]) <= 2 and len(author_2_vec[1]) <= 2:
                    return f"{author_1_vec[0]} {author_1_vec[1][0]}. {author_1_vec[2]}"
        return None
