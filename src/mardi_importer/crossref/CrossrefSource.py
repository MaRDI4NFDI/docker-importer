from mardi_importer.base import ADataSource
from .CrossrefPublication import CrossrefPublication

class CrossrefSource(ADataSource):
    def __init__(
        self,
        user: str,
        password: str
    ):
        super().__init__(user, password)

    def setup(self):
        """Create all necessary properties and entities for CRAN
        """
        # Import entities from Wikidata
        self.import_wikidata_entities("/wikidata_entities.txt")

    def pull():
        print('Not implemented')

    def push():
        print('Not implemented')

    def new_publication(self, doi: str) -> 'CrossrefPublication':
        return CrossrefPublication(doi)
