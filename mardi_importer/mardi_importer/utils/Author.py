from mardiclient import MardiClient, MardiItem
from mardi_importer.wikidata import WikidataImporter

from dataclasses import dataclass, field
from typing import List
from nameparser import HumanName
from nameparser.config import CONSTANTS

CONSTANTS.titles.remove("Mahdi")
CONSTANTS.titles.remove("Bodhisattva")


@dataclass
class Author:
    api: MardiClient
    name: str
    orcid: str = None
    arxiv_id: str = None
    affiliation: str = None
    _aliases: List[str] = field(default_factory=list)
    _QID: str = None
    _item: MardiItem = None
    wdi: WikidataImporter = None

    def __post_init__(self):
        self.name = self.parse_name(self.name)
        if self.wdi is None:
            self.wdi = WikidataImporter()

    @staticmethod
    def parse_name(name: str) -> str:
        parsed_name = HumanName(name)
        parsed_name.capitalize(force=True)
        return str(parsed_name)

    def __eq__(self, other):
        if self.orcid and self.orcid == other.orcid:
            return True
        self_name = HumanName(self.name)
        other_name = HumanName(other.name)
        if self_name.first == other_name.first and self_name.last == other_name.last:
            return True
        if self_name.first.lower().replace("-", "") == other_name.first.lower().replace(
            "-", ""
        ) and self_name.last.lower().replace(
            "-", ""
        ) == other_name.last.lower().replace("-", ""):
            return True
        if (
            len(self_name.first) == 2
            and self_name.first[0] == other_name.first[0]
            and self_name.last == other_name.last
        ):
            return True
        if (
            len(other_name.first) == 2
            and self_name.first[0] == other_name.first[0]
            and self_name.last == other_name.last
        ):
            return True
        if (
            self_name.first == other_name.first
            and len(self_name.last) == 2
            and self_name.last[0] == other_name.last[0]
        ):
            return True
        if (
            self_name.first == other_name.first
            and len(other_name.last) == 2
            and self_name.last[0] == other_name.last[0]
        ):
            return True
        return False

    def __add__(self, other):
        self_name = HumanName(self.name)
        other_name = HumanName(other.name)

        name_attributes = ["first", "middle", "last"]
        name_values = [
            getattr(self_name, attr)
            if len(getattr(self_name, attr)) >= len(getattr(other_name, attr))
            else getattr(other_name, attr)
            for attr in name_attributes
        ]
        long_name = " ".join([s for s in name_values if s])

        aliases = []
        if self.name != long_name:
            aliases.append(self.name)
        if other.name != long_name:
            aliases.append(other.name)
        aliases += [x for x in self.aliases if x != long_name]
        aliases += [x for x in other.aliases if (x != long_name and x not in aliases)]

        orcid = self.orcid if self.orcid else other.orcid
        arxiv_id = self.arxiv_id if self.arxiv_id else other.arxiv_id
        affiliation = self.affiliation if self.affiliation else other.affiliation
        QID = self.QID if self.QID else other.QID

        if QID:
            item = self.api.item.get(entity_id=QID)
            item.labels.set(language="en", value=long_name)
            item.aliases.set(language="en", values=aliases)
            item.write()

        return Author(
            self.api,
            name=long_name,
            orcid=orcid,
            arxiv_id=arxiv_id,
            affiliation=affiliation,
            _aliases=aliases,
            _QID=QID,
        )

    def __repr__(self):
        rep = f"Author: {self.name}, ORCID: {self.orcid}, arXiv: {self.arxiv_id}, QID: {self.QID}, {self.aliases}"
        return rep

    @property
    def aliases(self) -> List[str]:
        if self._aliases:
            return self._aliases

        aliases = []
        if self.QID:
            item = self.api.item.get(entity_id=self.QID)
            if item.aliases.get("en"):
                for alias in item.aliases.get("en"):
                    aliases.append(str(alias))
                self._aliases = aliases
        return aliases

    @property
    def QID(self) -> str:
        if self._QID:
            return self._QID

        if not self.orcid:
            return None

        results = self.api.search_entity_by_value("wdt:P496", self.orcid)
        if results:
            self._QID = results[0]
            return self._QID

    @classmethod
    def disambiguate_authors(cls, authors):
        # Return empty input immediately
        if not authors:
            return []
        disambiguated_authors = [authors[0]]
        for author in authors:
            if author not in disambiguated_authors:
                disambiguated_authors.append(author)
            else:
                index = disambiguated_authors.index(author)
                disambiguated_authors[index] += author
        for author in disambiguated_authors:
            author.create()
        return disambiguated_authors

    def pull_QID(self, author_pool):
        if self in author_pool:
            index = author_pool.index(self)
            self._QID = author_pool[index].QID

    def create(self):
        if self.QID:
            # Update orcid and arxiv_id if given
            if self.orcid or self.arxiv_id:
                update_item = self.api.item.get(entity_id=self.QID)
                current_orcid = update_item.get_value("wdt:P496")
                current_arxiv_id = update_item.get_value("wdt:P4594")
                if not current_orcid or not current_arxiv_id:
                    if not current_orcid and self.orcid:
                        update_item.add_claim("wdt:P496", self.orcid)
                    if not current_arxiv_id and self.arxiv_id:
                        update_item.add_claim("wdt:P4594", self.arxiv_id)
                    update_item.write()
            return self.QID

        teams = {
            "r foundation": "Q111430684",
            "the r foundation": "Q111430684",
            "r core team": "Q116739338",
            "the r core team": "Q116739338",
            "cran team": "Q116739332",
            "microsoft corporation": "Q2283",
        }

        if self.name.lower() in teams.keys():
            self._QID = self.wdi.query("local_id", teams[self.name.lower()])
            return self.QID

        self._item = self.api.item.new()
        self._item.labels.set(language="en", value=self.name)
        self._item.aliases.set(language="en", values=self.aliases)

        # Instance of human
        human = "wd:Q5"
        self._item.add_claim("wdt:P31", human)

        # Orcid ID
        if self.orcid:
            self._item.add_claim("ORCID iD", self.orcid)

        if self.QID:
            self._item = self.api.item.get(self.QID)

        # ArXiv ID
        if self.arxiv_id:
            self._item.add_claim("wdt:P4594", self.arxiv_id)

        # MaRDI profile
        self._item.add_claim("MaRDI profile type", "MaRDI person profile")

        self._QID = self._item.write().id
        return self.QID
