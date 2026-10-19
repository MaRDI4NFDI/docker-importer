from mardiclient import MardiClient
from mardi_importer import Importer
from mardi_importer.wikidata import WikidataImporter
from mardi_importer.utils.Author import Author
from .Community import Community
from .Project import Project

import logging
import urllib.request, json, re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from wikibaseintegrator.datatypes import ExternalID, String, Time
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import execute_sparql_query

log = logging.getLogger(__name__)

CLEANR = re.compile(
    "<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});"
)  # used to parse out html tags

ZENODO_API = "https://zenodo.org/api"
# Record IDs kept in P227 per item, newest versions first.
MAX_VERSIONS = 20


def zenodo_get(path: str) -> dict:
    with urllib.request.urlopen(f"{ZENODO_API}{path}") as url:
        return json.load(url)


def version_label(record: dict) -> Optional[str]:
    """The record's version, or Zenodo's own ``v<n>`` numbering when it has none."""
    meta = record["metadata"]
    index = (meta.get("relations", {}).get("version") or [{}])[0].get("index")
    return meta.get("version") or (f"v{index + 1}" if index is not None else None)


@dataclass
class ZenodoResource:
    # Record ID of the latest version once resolved; may start as any version or concept ID.
    zenodo_id: str
    concept_id: str = None
    concept_doi: str = None
    _versions: List[dict] = None
    # False for items found only by DOI (e.g. zbMATH articles): linked to, never rewritten.
    owned: bool = True
    title: str = None
    _description: str = None
    _publication_date: str = None
    _authors: List[Author] = field(default_factory=list)
    _resource_type: str = None
    _mardi_type: str = None
    _license: str = None
    _version: str = None
    _communities: List[Community] = field(default_factory=list)
    _projects: List[Project] = field(default_factory=list)
    metadata: Dict[str, object] = field(default_factory=dict)
    QID: str = None
    wdi: WikidataImporter = None
    api: Optional[MardiClient] = None

    # Zenodo community slug -> community item, shared by all resources.
    _community_items = None

    def __post_init__(self):
        if self.api is None:
            self.api = Importer.get_api("zenodo")
        if self.wdi is None:
            self.wdi = WikidataImporter()
        requested_id = str(self.zenodo_id)
        # A concept ID redirects to the latest version.
        record = zenodo_get(f"/records/{requested_id}")
        self.concept_id = str(record.get("conceptrecid") or record["id"])
        self.concept_doi = (record.get("conceptdoi") or record.get("doi")
                            or f"10.5281/zenodo.{record['id']}").upper()
        self._versions = self.fetch_versions(record)
        latest = self._versions[0]
        self.zenodo_id = str(latest["id"])
        self.metadata = latest["metadata"]
        if self.metadata:
            self.title = self.metadata["title"]

        self.QID = self.find_item({requested_id, str(record["id"])})

        if self.QID and self.owned:
            # Get authors.
            item = self.api.item.get(self.QID)
            author_QID = item.get_value("wdt:P50") or []
            for QID in author_QID:
                author_item = self.api.item.get(entity_id=QID)
                name = str(author_item.labels.get("en"))
                orcid = author_item.get_value("wdt:P496")
                orcid = orcid[0] if orcid else None
                aliases = []
                if author_item.aliases.get("en"):
                    for alias in author_item.aliases.get("en"):
                        aliases.append(str(alias))
                author = Author(
                    self.api, name=name, orcid=orcid, _aliases=aliases, _QID=QID
                )
                self._authors.append(author)
            return self.QID

    @staticmethod
    def fetch_versions(record: dict) -> List[dict]:
        """The newest ``MAX_VERSIONS`` versions of the record's concept, newest first."""
        rel = (record["metadata"].get("relations", {}).get("version") or [{}])[0]
        if rel.get("index") == 0 and rel.get("is_last"):
            return [record]
        hits = zenodo_get(f"/records/{record['id']}/versions?size={MAX_VERSIONS}&sort=version")
        return hits["hits"]["hits"] or [record]

    def find_item(self, record_ids: set) -> Optional[str]:
        """Item holding the concept DOI, a version DOI or a record ID of this concept."""
        p227 = self._pid("wdt:P4901")
        p27 = self._pid("wdt:P356")
        ids = set(record_ids) | {self.concept_id} | {str(v["id"]) for v in self._versions}
        dois = {self.concept_doi} | {f"10.5281/ZENODO.{i}" for i in ids}
        doi_values = " ".join(f'"{d}" "{d.lower()}"' for d in sorted(dois))
        id_values = " ".join(f'"{i}"' for i in sorted(ids))
        query = f"""SELECT DISTINCT ?item ?owned WHERE {{
            {{ VALUES ?doi {{ {doi_values} }} ?item wdt:{p27} ?doi }}
            UNION {{ VALUES ?id {{ {id_values} }} ?item wdt:{p227} ?id }}
            BIND(EXISTS {{ ?item wdt:{p227} [] }} AS ?owned) }}"""
        found = {}
        for b in execute_sparql_query(query)["results"]["bindings"]:
            qid = b["item"]["value"].rsplit("/", 1)[-1]
            found[qid] = b["owned"]["value"] in ("true", "1")
        if not found:
            return None
        # Prefer items owned by this importer, then the oldest.
        qid = min(found, key=lambda q: (not found[q], int(q[1:])))
        if len(found) > 1:
            log.warning("Zenodo concept %s is held by %s; using %s",
                        self.concept_id, ", ".join(sorted(found)), qid)
        self.owned = found[qid]
        return qid

    def _pid(self, prop: str) -> str:
        pid = self.api.get_local_id_by_label(prop, "property")
        return pid[0] if isinstance(pid, list) else pid

    def identifier_claims(self) -> tuple:
        """``P227`` claims for the newest versions and the concept DOI claim for ``P27``."""
        p227 = self._pid("wdt:P4901")
        edition = self._pid("wdt:P393")
        published = self._pid("wdt:P577")
        record_claims = []
        for version in self._versions[:MAX_VERSIONS]:
            qualifiers = []
            if (label := version_label(version)):
                qualifiers.append(String(label, prop_nr=edition))
            date = version["metadata"].get("publication_date") or ""
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                qualifiers.append(Time(f"+{date}T00:00:00Z", prop_nr=published))
            record_claims.append(ExternalID(str(version["id"]), prop_nr=p227, qualifiers=qualifiers))
        doi_claim = ExternalID(self.concept_doi, prop_nr=self._pid("wdt:P356"))
        return record_claims, doi_claim

    def community_items(self) -> Dict[str, str]:
        """Zenodo community slug -> item, for communities that have an item in the KG."""
        if ZenodoResource._community_items is None:
            query = f"SELECT ?item ?slug WHERE {{ ?item wdt:{self._pid('wdt:P9934')} ?slug }}"
            ZenodoResource._community_items = {
                b["slug"]["value"]: b["item"]["value"].rsplit("/", 1)[-1]
                for b in execute_sparql_query(query)["results"]["bindings"]
            }
        return ZenodoResource._community_items

    @property
    def description(self):
        desc_long = ""
        if "description" in self.metadata.keys():
            desc_long = self.metadata["description"]
            desc_long = re.sub(
                CLEANR, "", desc_long
            )  # parse out html tags from the description
            desc_long = re.sub(
                r"\n|\\N|\t|\\T", " ", desc_long
            )  # parse out tabs and new lines
            desc_long = re.sub(
                r"^\s+|\s+$", "", desc_long
            )  # parse out leading and trailing white space
        if re.match("\w+", desc_long):
            self._description = desc_long
        return self._description

    @property
    def publication_date(self):
        if not self._publication_date:
            pub_date = self.metadata.get("publication_date")
            if pub_date and re.match(r"\d{4}-\d{2}-\d{2}", pub_date):
                self._publication_date = f"{pub_date}T00:00:00Z"
        return self._publication_date

    @property
    def license(self):
        if not self._license and ("license" in self.metadata.keys()):
            self._license = self.metadata["license"]
        return self._license

    @property
    def version(self):
        if not self._version and ("version" in self.metadata.keys()):
            self._version = self.metadata["version"]
        return self._version

    @property
    def authors(self):
        if not self._authors:
            for creator in self.metadata.get("creators", []):
                name = creator.get("name")
                orcid = creator.get("orcid")
                affiliation = creator.get("affiliation")
                author = Author(
                    self.api, name=name, orcid=orcid, affiliation=affiliation
                )
                self._authors.append(author)
        return self._authors

    @property
    def resource_type(self):
        if not self._resource_type:
            resource_type_data = self.metadata.get("resource_type", {})
            resource_type = resource_type_data.get("title")
            if not resource_type:
                return self._resource_type

            if resource_type == "Dataset":
                self._resource_type = "wd:Q1172284"
                self._mardi_type = "MaRDI dataset profile"
            elif resource_type == "Software":
                self._resource_type = "wd:Q7397"
                self._mardi_type = "MaRDI software profile"
            elif resource_type == "Presentation":
                self._resource_type = "wd:Q604733"
            elif resource_type == "Report":
                self._resource_type = "wd:Q10870555"
                self._mardi_type = "MaRDI publication profile"
            elif resource_type == "Poster":
                self._resource_type = "wd:Q429785"
            elif resource_type == "Figure":
                self._resource_type = "wd:Q478798"
            elif resource_type == "Video/Audio":
                self._resource_type = "wd:Q2431196"
            elif resource_type == "Lesson":
                self._resource_type = "wd:Q379833"
            elif resource_type == "Preprint":
                self._resource_type = "wd:Q580922"
                self._mardi_type = "MaRDI publication profile"
            elif resource_type == "Journal article":
                self._resource_type = "wd:Q13442814"
                self._mardi_type = "MaRDI publication profile"
            elif resource_type == "Book":
                self._resource_type = "wd:Q571"
                self._mardi_type = "MaRDI publication profile"
            else:
                # Other -> Information resource
                self._resource_type = "wd:Q37866906"
        return self._resource_type

    @property
    def communities(self):
        if not self._communities and "communities" in self.metadata.keys():
            # if "communities" in self.metadata.keys():
            known = self.community_items()
            for communityCur in self.metadata["communities"]:
                community_id = communityCur.get("id")
                if community_id in known:
                    community = Community(
                        api=self.api, wdi=self.wdi, community_id=community_id,
                        QID=known[community_id],
                    )
                    self._communities.append(community)
        return self._communities

    @property
    def projects(self):
        community = None
        if self._communities:
            for communityCur in self._communities:
                if communityCur.community_id == "mathplus":
                    community = communityCur
                    break
        if (
            not self._projects
            and community
            and self.metadata.get("related_identifiers")
        ):
            for related_ids in self.metadata.get("related_identifiers"):
                # print("identifier: " + related_ids["identifier"])
                if related_ids["identifier"] in Project.get_project_ids():
                    project = Project(
                        api=self.api,
                        community=community,
                        project_id=related_ids["identifier"],
                    )
                    self._projects.append(project)
        return self._projects

    def exists(self):
        if self.QID:
            return self.QID

    def update(self):
        # description_prop_nr = "P727"
        zenodo_item = self.api.item.new()
        zenodo_item.labels.set(language="en", value=self.title)

        zenodo_id = zenodo_item.is_instance_of_with_property(
            "wd:Q1172284", "wdt:P4901", self.zenodo_id
        )
        new_item = self.api.item.get(entity_id=zenodo_id)

        if self.license["id"] == "cc-by-4.0":
            new_item.add_claim("wdt:P275", "wd:Q20007257")
        elif self.license["id"] == "cc-by-sa-4.0":
            new_item.add_claim("wdt:P275", "wd:Q18199165")
        elif self.license["id"] == "cc-by-nc-sa-4.0":
            new_item.add_claim("wdt:P275", "wd:Q42553662")
        elif self.license["id"] == "mit-license":
            new_item.add_claim("wdt:P275", "wd:Q334661")

        return new_item.write()

    def create(self, update=False):
        if not update:
            if self.QID:
                return self.QID
            item = self.api.item.new()
            update_claim = "append_or_replace"
        else:
            if not self.owned:
                return self.QID
            item = self.api.item.get(entity_id=self.QID)
            update_claim = "replace_all"

        if self.title:
            item.labels.set(language="en", value=self.title)

        if self.resource_type and self.resource_type != "wd:Q37866906":
            desc = f"{self.metadata['resource_type']['title']} published at Zenodo repository. "
            item.add_claim("wdt:P31", self.resource_type, action=update_claim)
        else:
            desc = "Resource published at Zenodo repository. "
        item.descriptions.set(language="en", value=desc)

        if self.description:
            prop_nr = self.api.get_local_id_by_label("description", "property")
            item.add_claim(prop_nr, self.description, action=update_claim)

        # Publication date
        if self.publication_date:
            item.add_claim("wdt:P577", self.publication_date, action=update_claim)

        # Authors
        if update:
            # Remove all existing P50 and P2093 claims
            author_item_prop = self.api.get_local_id_by_label("wdt:P50", "property")
            author_string_prop = self.api.get_local_id_by_label("wdt:P2093", "property")

            for prop in (author_item_prop, author_string_prop):
                old_claims = item.claims.get(prop)
                if old_claims:
                    for c in old_claims:
                        c.remove()

            # Add new author claims
            item.add_claims(self.__preprocess_authors())

        else:
            claims = self.__preprocess_authors()
            item.add_claims(claims)

        # Record IDs of the newest versions & concept DOI
        record_claims, doi_claim = self.identifier_claims()
        item.claims.add(record_claims, ActionIfExists.REPLACE_ALL)
        item.claims.add(doi_claim, ActionIfExists.REPLACE_ALL)

        # License
        if self.license:
            if self.license["id"] == "cc-by-4.0":
                item.add_claim("wdt:P275", "wd:Q20007257", action=update_claim)
            elif self.license["id"] == "cc-by-sa-4.0":
                item.add_claim("wdt:P275", "wd:Q18199165", action=update_claim)
            elif self.license["id"] == "cc-by-nc-sa-4.0":
                item.add_claim("wdt:P275", "wd:Q42553662", action=update_claim)
            elif self.license["id"] == "mit-license":
                item.add_claim("wdt:P275", "wd:Q334661", action=update_claim)

        if self.version:
            if self.resource_type:
                if self.resource_type == "wd:Q1172284":  # dataset
                    prop_nr = self.api.get_local_id_by_label(
                        "dataset version identifier", "property"
                    )
                    item.add_claim(prop_nr, self.version, action=update_claim)
                elif self.resource_type == "wd:Q7397":  # software:
                    item.add_claim("wdt:P348", self.version, action=update_claim)

        # Communities: appended, so several communities and manual statements coexist
        if self.communities:
            prop_nr = self.api.get_local_id_by_label("community", "property")
            for community in self.communities:
                item.add_claim(prop_nr, community.QID)

        # Projects
        if self.projects:
            prop_nr = self.api.get_local_id_by_label(
                "Internal Project ID", "property"
            )
            claims = []
            for project in self.projects:
                project.create()
                claims.append(self.api.get_claim(prop_nr, project.QID))
            item.claims.add(claims, ActionIfExists.REPLACE_ALL if update else ActionIfExists.APPEND_OR_REPLACE)

        if self._mardi_type:
            item.add_claim("MaRDI profile type", self._mardi_type, action=update_claim)

        self.QID = item.write().id

        if self.QID:
            print(
                f"Zenodo resource with Zenodo id: {self.zenodo_id} created with ID {self.QID}."
            )
            return self.QID
        else:
            print(
                f"Zenodo resource with Zenodo id: {self.zenodo_id} could not be created."
            )
            return None

    def __preprocess_authors(self) -> List[str]:
        """Processes the author information of each publication.

        Create the author if it does not exist already as an
        entity in wikibase.

        If an author has no ORCID and no existing QID, store the author as
        an author name string (P2093) instead of creating an author item.

        Returns:
        List:
            Author claims to be added (P50 entity claims and/or P2093 string claims).
        """
        claims = []
        for author in self.authors:
            if author.orcid or author.QID:
                if not author.QID:
                    author.create()
                claims.append(self.api.get_claim("wdt:P50", author.QID))
            else:
                if author.name:
                    claims.append(self.api.get_claim("wdt:P2093", author.name))
        return claims
