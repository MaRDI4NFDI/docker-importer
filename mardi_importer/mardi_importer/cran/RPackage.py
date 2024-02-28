#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.integrator import MardiIntegrator, MardiItemEntity
from mardi_importer.publications import (ArxivPublication, 
                                         CrossrefPublication,
                                         ZenodoResource, 
                                         Author)
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, remove_claims

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from io import StringIO

from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

import logging
log = logging.getLogger('CRANlogger')

@dataclass
class RPackage:
    """Class to manage R package items in the local Wikibase instance.

    Attributes:
        date:
          Date of publication
        label:
          Package name
        description:
          Title of the R package
        long_description:
          Detailed description of the R package
        url:
          URL to the CRAN repository
        version:
          Version of the R package
        versions:
          Previous published versions
        author:
          Author(s) of the package
        license:
          Software license
        dependency:
          Dependencies to R and other packages
        imports:
          Imported R packages
        maintainer:
          Software maintainer
        _QID:
          Package QID
        integrator:
          API to MaRDI integrator
    """
    date: str
    label: str
    description: str
    api: MardiIntegrator
    long_description: str = ""
    url: str = ""
    version: str = ""
    versions: List[Tuple[str, str]] = field(default_factory=list)
    authors: List[Author] = field(default_factory=list)
    license_data: List[Tuple[str, str]] = field(default_factory=list)
    dependencies: List[Tuple[str, str]] = field(default_factory=list)
    imports: List[Tuple[str, str]] = field(default_factory=list)
    maintainer: str = ""
    author_pool: List[Author] = field(default_factory=list)
    crossref_publications: List[CrossrefPublication] = field(default_factory=list)
    arxiv_publications: List[ArxivPublication] = field(default_factory=list)
    zenodo_resources: List[ZenodoResource] = field(default_factory=list)
    _QID: str = ""
    _item: MardiItemEntity = None

    @property
    def QID(self) -> str:
        """Return the QID of the R package in the knowledge graph.

        Searches for an item with the package label in the Wikibase
        SQL tables and returns the QID if a matching result is found.

        Returns:
            str: The entity QID representing the R package.
        """
        self._QID = self._QID or self.item.is_instance_of('wd:Q73539779')
        return self._QID

    @property
    def item(self) -> MardiItemEntity:
        """Return the integrator Item representing the R package.

        Adds also the label and description of the package.

        Returns:
            MardiItemEntity: Integrator item
        """
        if not self._item:
            self._item = self.api.item.new()
            self._item.labels.set(language="en", value=self.label)
            description = self.description
            if self.label == self.description:
                description += " (R Package)"
            self._item.descriptions.set(
                language="en",
                value=description
            )
        return self._item

    def exists(self) -> str:
        """Checks if an item corresponding to the R package already exists.

        Returns:
          str: Entity ID
        """
        if self.QID:
            self._item = self.api.item.get(entity_id=self.QID)
        return self.QID

    def is_updated(self) -> bool:
        """Checks if the Item corresponding to the R package is up to date.

        Compares the last update property in the local knowledge graph with
        the publication date imported from CRAN.

        Returns:
          bool: **True** if both dates coincide, **False** otherwise.
        """
        return self.date == self.get_last_update()

    def pull(self):
        """Imports metadata from CRAN corresponding to the R package.

        Imports **Version**, **Dependencies**, **Imports**m **Authors**,
        **Maintainer** and **License** and saves them as instance
        attributes.
        """
        self.url = f"https://CRAN.R-project.org/package={self.label}"

        try:
            page = requests.get(self.url)
            soup = BeautifulSoup(page.content, 'lxml')
        except:
            log.warning(f"Package {self.label} package not found in CRAN.")
            return None
        else:
            if soup.find_all('table'):
                self.long_description = soup.find_all('p')[0].get_text() or ""
                self.parse_publications(self.long_description)
                self.long_description = re.sub("\n", "", self.long_description).strip()
                self.long_description = re.sub("\t", "", self.long_description).strip()

                table = soup.find_all('table')[0]
                package_df = self.clean_package_list(table)                

                if "Version" in package_df.columns:
                    self.version = package_df.loc[1, "Version"]
                if "Author" in package_df.columns:
                    self.authors = package_df.loc[1, "Author"]
                if "License" in package_df.columns:
                    self.license_data = package_df.loc[1, "License"]
                if "Depends" in package_df.columns:
                    self.dependencies = package_df.loc[1, "Depends"]
                if "Imports" in package_df.columns:
                    self.imports = package_df.loc[1, "Imports"]
                if "Maintainer" in package_df.columns:
                    self.maintainer = package_df.loc[1, "Maintainer"]

                self.get_versions()
            else:
                log.warning(f"Metadata table not found in CRAN. Package has probably been archived.")
            return self

    def create(self) -> None:
        """Create a package in the Wikibase instance.

        This function pulls the package, inserts its claims, and writes
        it to the Wikibase instance.

        Returns:
            None
        """
        package = self.pull()
        
        if package:
            package = package.insert_claims().write()

        if package:
            log.info(f"Package created with QID: {package['QID']}.")
            #print('package created')
        else:
            log.info(f"Package could not be created.")
            #print('package not created')

    def write(self) -> Optional[Dict[str, str]]:
        """Write the package item to the Wikibase instance.

        If the item has claims, it will be written to the Wikibase instance.
        If the item is successfully written, a dictionary with the QID of the
        item will be returned.

        Returns:
            Optional[Dict[str, str]]:
                A dictionary with the QID of the written item if successful,
                or None otherwise.
        """
        if self.item.claims:
            item = self.item.write()
            if item:
                return {'QID': item.id}

    def insert_claims(self):

        # Instance of: R package
        self.item.add_claim("wdt:P31", "wd:Q73539779")

        # Programmed in: R
        self.item.add_claim("wdt:P277", "wd:Q206904")

        # Long description
        prop_nr = self.api.get_local_id_by_label("description", "property")
        self.item.add_claim(prop_nr, self.long_description)

        # Last update date
        self.item.add_claim("wdt:P5017", f"+{self.date}T00:00:00Z")

        # Software version identifiers
        for version, publication_date in self.versions:
            qualifier = [self.api.get_claim("wdt:P577", publication_date)]
            self.item.add_claim("wdt:P348", version, qualifiers=qualifier)

        if self.version:
            qualifier = [self.api.get_claim("wdt:P577", f"+{self.date}T00:00:00Z")]
            self.item.add_claim("wdt:P348", self.version, qualifiers=qualifier)

        # Disambiguate Authors and create corresponding Author items
        self.author_pool = Author.disambiguate_authors(self.author_pool)

        # Authors
        for author in self.authors:
            author.pull_QID(self.author_pool)
            self.item.add_claim("wdt:P50", author.QID)

        # Maintainer
        self.maintainer.pull_QID(self.author_pool)
        self.item.add_claim("wdt:P126", self.maintainer.QID)

        # Licenses
        if self.license_data:
            claims = self.process_claims(self.license_data, 'wdt:P275', 'wdt:P9767')
            self.item.add_claims(claims)

        # Dependencies
        if self.dependencies:
            claims = self.process_claims(self.dependencies, 'wdt:P1547', 'wdt:P348')
            self.item.add_claims(claims)

        # Imports
        if self.imports:
            prop_nr = self.api.get_local_id_by_label("imports", "property")
            claims = self.process_claims(self.imports, prop_nr, 'wdt:P348')
            self.item.add_claims(claims)

        # Related publications and sources
        cites_work = "wdt:P2860"
        for publications in [self.crossref_publications, self.arxiv_publications, self.zenodo_resources]:
            for publication in publications:
                for author in publication.authors:
                    author.pull_QID(self.author_pool)
                publication.create()
                self.item.add_claim(cites_work, publication.QID)

        # CRAN Project
        self.item.add_claim("wdt:P5565", self.label)

        # Wikidata QID
        wikidata_QID = self.get_wikidata_QID()
        if wikidata_QID: self.item.add_claim("Wikidata QID", wikidata_QID)

        return self

    def update(self):
        """Updates existing WB item with the imported metadata from CRAN.

        The metadata corresponding to the package is first pulled from CRAN and
        saved as instance attributes through :meth:`pull`. The statements that
        do not coincide with the locally saved information are updated or
        subsituted with the updated information.

        Uses :class:`mardi_importer.wikibase.WBItem` to update the item
        corresponding to the R package.

        Returns:
          str: ID of the updated R package.
        """
        if self.pull():
            # Obtain current Authors
            current_authors = self.item.get_value('wdt:P50')
            for author_qid in current_authors:
                author_item = self.api.item.get(entity_id=author_qid)
                author_label = str(author_item.labels.get('en'))
                current_author = Author(self.api, name=author_label)
                current_author._QID = author_qid
                self.author_pool += [current_author]
                
            # Disambiguate Authors and create corresponding Author items
            self.author_pool = Author.disambiguate_authors(self.author_pool)

            # GUID to remove
            remove_guid = []
            props_to_delete = ['wdt:P50', 'wdt:P275', 'wdt:P1547', 'imports', 'wdt:P2860']
            for prop_str in props_to_delete:
                prop_nr = self.api.get_local_id_by_label(prop_str, 'property')
                for claim in self.item.claims.get(prop_nr):
                    remove_guid.append(claim.id)

            for guid in remove_guid:
                remove_claims(guid, login=self.api.login, is_bot=True)

            # Restart item state
            self.exists()

            if self.item.descriptions.values.get('en') != self.description:
                description = self.description
                if self.label == self.description:
                    description += " (R Package)"
                self.item.descriptions.set(
                    language="en",
                    value=description
                )

            # Long description
            self.item.add_claim("description", self.long_description, action="replace_all")

            # Last update date
            self.item.add_claim("wdt:P5017", f"+{self.date}T00:00:00Z", action="replace_all")

            # Software version identifiers
            for version, publication_date in self.versions:
                qualifier = [self.api.get_claim("wdt:P577", publication_date)]
                self.item.add_claim("wdt:P348", version, qualifiers=qualifier)
            
            if self.version:
                qualifier = [self.api.get_claim("wdt:P577", f"+{self.date}T00:00:00Z")]
                self.item.add_claim("wdt:P348", self.version, qualifiers=qualifier)            

            # Authors
            for author in self.authors:
                author.pull_QID(self.author_pool)
                self.item.add_claim("wdt:P50", author.QID)

            # Maintainer
            self.maintainer.pull_QID(self.author_pool)
            self.item.add_claim("wdt:P126", self.maintainer.QID, action="replace_all")

            # Licenses
            if self.license_data:
                claims = self.process_claims(self.license_data, 'wdt:P275', 'wdt:P9767')
                self.item.add_claims(claims)

            # Dependencies
            if self.dependencies:
                claims = self.process_claims(self.dependencies, 'wdt:P1547', 'wdt:P348')
                self.item.add_claims(claims)

            # Imports
            if self.imports:
                prop_nr = self.api.get_local_id_by_label("imports", "property")
                claims = self.process_claims(self.imports, prop_nr, 'wdt:P348')
                self.item.add_claims(claims)            

            # Related publications and sources
            cites_work = "wdt:P2860"
            for publications in [self.crossref_publications, self.arxiv_publications, self.zenodo_resources]:
                for publication in publications:
                    for author in publication.authors:
                        author.pull_QID(self.author_pool)
                    publication.create()
                    self.item.add_claim(cites_work, publication.QID)

            # CRAN Project
            self.item.add_claim("wdt:P5565", self.label, action="replace_all")

            # Wikidata QID
            wikidata_QID = self.get_wikidata_QID()
            if wikidata_QID: self.item.add_claim("Wikidata QID", wikidata_QID, action="replace_all")

            package = self.write()
                    
            if package:
                print(f"Package with QID updated: {package['QID']}.")
            else:
                print(f"Package could not be updated.")

    def process_claims(self, data, prop_nr, qualifier_nr=None):

        claims = []
        for value, qualifier_value in data:
            qualifier_prop_nr = (
                'wdt:P2699' if qualifier_value.startswith('https') else qualifier_nr
            )
            qualifier = (
                [self.api.get_claim(qualifier_prop_nr, qualifier_value)]
                if qualifier_value else []
            )
            claims.append(self.api.get_claim(prop_nr, value, qualifiers=qualifier))
        return claims

    def parse_publications(self, description):
        """Extracts the DOI identification of related publications.

        Identifies the DOI of publications that are mentioned using the
        format *doi:* or *arXiv:* in the long description of the
        R package.

        Returns:
          List:
            List containing the wikibase IDs of mentioned publications.
        """
        doi_references = re.findall('<doi:(.*?)>', description)
        arxiv_references = re.findall('<arXiv:(.*?)>', description)
        zenodo_references = re.findall('<zenodo:(.*?)>', description)

        doi_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, doi_references))
        arxiv_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, arxiv_references))
        zenodo_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, zenodo_references))

        crossref_references = []

        for doi in doi_references:
            doi = doi.strip().lower()
            if re.search('10.48550/', doi):
                arxiv_id = doi.replace(":",".")
                arxiv_id = arxiv_id.replace('10.48550/arxiv.', '')
                arxiv_references.append(arxiv_id.strip())
            elif re.search('10.5281/', doi):
                zenodo_id = doi.replace(":",".")
                zenodo_id = doi.replace('10.5281/zenodo.', '')
                zenodo_references.append(zenodo_id.strip())
            else:
                crossref_references.append(doi)

        for doi in crossref_references:
            publication = CrossrefPublication(self.api, doi)
            self.author_pool += publication.authors
            self.crossref_publications.append(publication)

        for arxiv_id in arxiv_references:
            arxiv_id = arxiv_id.replace(":",".")
            publication = ArxivPublication(self.api, arxiv_id)
            if publication.title != "Error":
                self.author_pool += publication.authors
                self.arxiv_publications.append(publication)

        for zenodo_id in zenodo_references:
            zenodo_id = zenodo_id.replace(":",".")
            publication = ZenodoResource(self.api, zenodo_id)
            self.author_pool += publication.authors
            self.zenodo_resources.append(publication)

    def get_last_update(self):
        """Returns the package last update date saved in the Wikibase instance.

        Returns:
            str: Last update date in format DD-MM-YYYY.
        """
        last_update = self.item.get_value("wdt:P5017")
        return last_update[0][1:11] if last_update else None

    def clean_package_list(self, table_html):
        """Processes raw imported data from CRAN to enable the creation of items.

        - Package dependencies are splitted at the comma position.
        - License information is processed using the :meth:`parse_license` method.
        - Author information is processed using the :meth:`parse_authors` method.
        - Maintainer information is processed using the :meth:`parse_maintainer` method.

        Args:
            table_html:
              HTML code obtained with BeautifulSoup corresponding to the table
              containing the metadata of the R package imported from CRAN.
        Returns:
            (Pandas dataframe):
              Dataframe with processed data from a single R package including columns:
              **Version**, **Author**, **License**, **Depends**, **Imports**
              and **Maintainer**.
        """
        package_df = pd.read_html(StringIO(str(table_html)))
        package_df = package_df[0].set_index(0).T
        package_df.columns = package_df.columns.str[:-1]
        if "Depends" in package_df.columns:
            package_df["Depends"] = package_df["Depends"].apply(self.parse_software)
        if "Imports" in package_df.columns:
            package_df["Imports"] = package_df["Imports"].apply(self.parse_software)
        if "License" in package_df.columns:
            package_df["License"] = package_df["License"].apply(self.parse_license)
        if "Author" in package_df.columns:
            package_df["Author"] = str(table_html.find("td", text="Author:").find_next_sibling("td")).replace('\n', '').replace('\r', '')
            package_df["Author"] = package_df["Author"].apply(self.parse_authors)
        if "Maintainer" in package_df.columns:
            package_df["Maintainer"] = package_df["Maintainer"].apply(self.parse_maintainer)
        return package_df

    def parse_software(self, software_str: str) -> List[Tuple[str, str]]:
        """Processes the dependency and import information of each R package.

        This includes:
        - Extracting the version information of each dependency/import if provided.
        - Providing the Item QID given the dependency/import label.
        - Creating a new Item if the dependency/import is not found in the
          local knowledge graph.

        Returns:
            List[Tuple[str, str]]:
                List of tuples including software QID and version.
        """
        if pd.isna(software_str):
            return []

        software_list = str(software_str).split(", ")
        software_tuples = []

        for software_string in software_list:
            software_version = re.search("\((.*?)\)", software_string)
            software_version = software_version.group(1) if software_version else ""

            software_name = re.sub("\(.*?\)", "", software_string).strip()

            # Instance of R package
            if software_name == "R":
                # Software = R
                software_QID = self.api.query("local_id", "Q206904")
            else:
                item = self.api.item.new()
                item.labels.set(language="en", value=software_name)
                software_id = item.is_instance_of("wd:Q73539779")
                if software_id:
                    # Software = R package
                    software_QID = software_id
                else:
                    # Software = New instance of R package
                    item.add_claim("wdt:P31", "wd:Q73539779")
                    item.add_claim("wdt:P277", "wd:Q206904")
                    software_QID = item.write().id

            software_tuples.append((software_QID, software_version))

        return software_tuples

    def parse_license(self, x: str) -> List[Tuple[str, str]]:
        """Splits string of licenses.

        Takes into account that licenses are often not uniformly listed.
        Characters \|, + and , are used to separate licenses. Further
        details on each license are often included in square brackets.

        The concrete License is identified and linked to the corresponding
        item that has previously been imported from Wikidata. Further license
        information, when provided between round or square brackets, is added
        as a qualifier.

        If a file license is mentioned, the linked to the file license
        in CRAN is added as a qualifier.

        Args:
            x (str): String imported from CRAN representing license
              information.

        Returns:
            List[Tuple[str, str]]:
                List of license tuples. Each tuple contains the license QID
                as the first element and the license qualifier as the
                second element.
        """
        if pd.isna(x):
            return []

        license_list = []
        licenses = str(x).split(" | ")

        i = 0
        while i in range(len(licenses)):
            if not re.findall(r"\[", licenses[i]) or (
                re.findall(r"\[", licenses[i]) and re.findall(r"\]", licenses[i])
            ):
                license_list.append(licenses[i])
                i += 1
            elif re.findall(r"\[", licenses[i]) and not re.findall(
                r"\]", licenses[i]
            ):
                j = i + 1
                license_aux = licenses[i]
                closed = False
                while j < len(licenses) and not closed:
                    license_aux += " | "
                    license_aux += licenses[j]
                    if re.findall(r"\]", licenses[j]):
                        closed = True
                    j += 1
                license_list.append(license_aux)
                i = j

        split_list = []
        for item in license_list:
            items = item.split(" + ")
            i = 0
            while i in range(len(items)):
                if not re.findall(r"\[", items[i]) or (
                    re.findall(r"\[", items[i]) and re.findall(r"\]", items[i])
                ):
                    split_list.append(items[i])
                    i += 1
                elif re.findall(r"\[", items[i]) and not re.findall(r"\]", items[i]):
                    j = i + 1
                    items_aux = items[i]
                    closed = False
                    while j < len(items) and not closed:
                        items_aux += " + "
                        items_aux += items[j]
                        if re.findall(r"\]", items[j]):
                            closed = True
                        j += 1
                    split_list.append(items_aux)
                    i = j
        license_list = list(dict.fromkeys(split_list))

        license_tuples = []
        for license_str in license_list:
            license_qualifier = ""
            if re.findall(r"\(.*?\)", license_str):
                qualifier_groups = re.search(r"\((.*?)\)", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_aux = re.sub(r"\(.*?\)", "", license_str)
                if re.findall(r"\[.*?\]", license_aux):
                    qualifier_groups = re.search(r"\[(.*?)\]", license_str)
                    license_qualifier = qualifier_groups.group(1)
                    license_str = re.sub(r"\[.*?\]", "", license_aux)
                else:
                    license_str = license_aux
            elif re.findall(r"\[.*?\]", license_str):
                qualifier_groups = re.search(r"\[(.*?)\]", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_str = re.sub(r"\[.*?\]", "", license_str)

            license_str = license_str.strip()
            if license_str in ["file LICENSE", "file LICENCE"]:
                license_qualifier = f"https://cran.r-project.org/web/packages/{self.label}/LICENSE"

            license_QID = self.get_license_QID(license_str)
            license_tuples.append((license_QID, license_qualifier))
        return license_tuples

    def parse_authors(self, x):
        """Splits the string corresponding to the authors into a dictionary.

        Author information in CRAN is not registered uniformly. This function
        parses the imported string and returns just the names of the individuals
        that can be unequivocally identified as authors (i.e. they are followed
        by the *[aut]* abbreviation).

        Generally, authors in CRAN are indicated with the abbreviation *[aut]*.
        When no abbreviations are included, only the first individual is imported
        to Wikibase (otherwise it can often not be established whether
        information after the first author refers to another individual,
        an institution, a funder, etc.)

        Args:
            x (String): String imported from CRAN representing author
              information.

        Returns:
            (Dict): Dictionary of authors and corresponding ORCID ID, if provided.
        """
        td_match = re.match(r'<td>(.*?)</td>', x)
        if td_match: x = td_match.groups()[0]

        x = re.sub("<img alt.*?a>", "", x) # Delete img tags
        x = re.sub(r"\(.*?\)", "", x) # Delete text in brackets
        x = re.sub(r'"', "", x) # Delete quotation marks
        x = re.sub("\t", "", x) # Delete tabs
        x = re.sub("ORCID iD", "", x) # Delete orcid id refs
        author_list = re.findall(r".*?\]", x)

        authors = []
        if author_list:
            for author in author_list:
                labels = re.findall(r"\[.*?\]", author)
                if labels:
                    is_author = re.findall("aut", labels[0])
                    if is_author:
                        orcid = re.findall(r"\d{4}-\d{4}-\d{4}-.{4}", author)
                        if orcid:
                            orcid = orcid[0]
                        author = re.sub(r"<a href=.*?>", "", author)
                        author = re.sub(r"\[.*?\]", "", author)
                        author = re.sub(r"^\s?,", "", author)
                        author = re.sub(r"^\s?and\s?", "", author)
                        author = re.sub(
                            r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "", author
                        )
                        author = author.strip()
                        multiple_words = author.split(" ")
                        if len(multiple_words) > 1:
                            if author:
                                authors.append(Author(self.api, author, orcid))
        else:
            authors_comma = x.split(", ")
            authors_and = x.split(" and ")
            if len(authors_and) > len(authors_comma):
                author = re.sub(
                    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "", authors_and[0]
                )
            else:
                author = re.sub(
                    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
                    "",
                    authors_comma[0],
                )
            if len(author.split(" ")) > 5 or re.findall(r"[@\(\)\[\]&]", author):
                author = ""
            if author:
                authors.append(Author(self.api, author))
        self.author_pool += authors
        return authors

    def parse_maintainer(self, name: str) -> str:
        """Remove unnecessary information from maintainer string.

        Args:
            x (str): String imported from CRAN which may contain e-mail
                        address and comments within brackets

        Returns:
            (str): Name of the maintainer
        """
        if pd.isna(name): return name

        quotes = re.match(r'"(.*?)"', name)
        if quotes:
            name = quotes.groups()[0]

        name = re.sub(r"<.*?>", "", name)
        name = re.sub(r"\(.*?\)", "", name)
        name = name.strip()
        name = name.split(',')
        maintainer = Author(self.api, name=name[0])
        self.author_pool += [maintainer]
        return maintainer

    def get_license_QID(self, license_str: str) -> str:
        """Returns the Wikidata item ID corresponding to a software license.

        The same license is often denominated in CRAN using differents names.
        This function returns the wikidata item ID corresponding to a single
        unique license that is referenced in CRAN under different names (e.g.
        *Artistic-2.0* and *Artistic License 2.0* both refer to the same
        license, corresponding to item *Q14624826*).

        Args:
            license_str (str): String corresponding to a license imported from CRAN.

        Returns:
            (str): Wikidata item ID.
        """
        def get_license(label: str) -> str:
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value=label)
            return license_item.is_instance_of("wd:Q207621")

        license_mapping = {
            "ACM": get_license("ACM Software License Agreement"),
            "AGPL":"wd:Q28130012",
            "AGPL-3": "wd:Q27017232",
            "Apache License": "wd:Q616526",
            "Apache License 2.0": "wd:Q13785927",
            "Apache License version 1.1": "wd:Q17817999",
            "Apache License version 2.0": "wd:Q13785927",
            "Artistic-2.0": "wd:Q14624826",
            "Artistic License 2.0": "wd:Q14624826",
            "BSD 2-clause License": "wd:Q18517294",
            "BSD 3-clause License": "wd:Q18491847",
            "BSD_2_clause": "wd:Q18517294",
            "BSD_3_clause": "wd:Q18491847",
            "BSL": "wd:Q2353141",
            "BSL-1.0": "wd:Q2353141",
            "CC0": "wd:Q6938433",
            "CC BY 4.0": "wd:Q20007257",
            "CC BY-SA 4.0": "wd:Q18199165",
            "CC BY-NC 4.0": "wd:Q34179348",
            "CC BY-NC-SA 4.0": "wd:Q42553662",
            "CeCILL": "wd:Q1052189",
            "CeCILL-2": "wd:Q19216649",
            "Common Public License Version 1.0": "wd:Q2477807",
            "CPL-1.0": "wd:Q2477807",
            "Creative Commons Attribution 4.0 International License": "wd:Q20007257",
            "EPL": "wd:Q1281977",
            "EUPL": "wd:Q1376919",
            "EUPL-1.1": "wd:Q1376919",
            "file LICENCE": get_license("File License"),
            "file LICENSE": get_license("File License"),
            "FreeBSD": "wd:Q34236",
            "GNU Affero General Public License": "wd:Q1131681",
            "GNU General Public License": "wd:Q7603",
            "GNU General Public License version 2": "wd:Q10513450",
            "GNU General Public License version 3": "wd:Q10513445",
            "GPL": "wd:Q7603",
            "GPL-2": "wd:Q10513450",
            "GPL-3": "wd:Q10513445",
            "LGPL": "wd:Q192897",
            "LGPL-2": "wd:Q23035974",
            "LGPL-2.1": "wd:Q18534390",
            "LGPL-3": "wd:Q18534393",
            "Lucent Public License": "wd:Q6696468",
            "MIT": "wd:Q334661",
            "MIT License": "wd:Q334661",
            "Mozilla Public License 1.1": "wd:Q26737735",
            "Mozilla Public License 2.0": "wd:Q25428413",
            "Mozilla Public License Version 2.0": "wd:Q25428413",
            "MPL": "wd:Q308915",
            "MPL version 1.0": "wd:Q26737738",
            "MPL version 1.1": "wd:Q26737735",
            "MPL version 2.0": "wd:Q25428413",
            "MPL-1.1": "wd:Q26737735",
            "MPL-2.0": "wd:Q25428413",
            "Unlimited": get_license("Unlimited License"),
        }

        license_info = license_mapping.get(license_str)
        if callable(license_info):
            return license_info()
        else:
            return license_info

    def get_wikidata_QID(self) -> Optional[str]:
        """Get the Wikidata QID for the R package.

        Searches for the R package in Wikidata using its label. Retrieves
        the QID of matching entities and checks if there is an instance of
        an R package. If so, returns the QID.

        Returns:
            Optional[str]:
                The Wikidata QID of the R package if found, or None otherwise.
        """
        results = search_entities(
            search_string=self.label,
            mediawiki_api_url='https://www.wikidata.org/w/api.php'
            )

        for result in results:
            item = self.api.item.get(
                entity_id=result,
                mediawiki_api_url='https://www.wikidata.org/w/api.php'
                )
            if 'P31' in item.claims.get_json().keys():
                instance_claims = item.claims.get('P31')
                if instance_claims:
                    for claim in instance_claims:
                        claim = claim.get_json()
                        if claim['mainsnak']['datatype'] == "wikibase-item":
                            # If instance of R package
                            if 'datavalue' in claim['mainsnak'].keys():
                                if claim['mainsnak']['datavalue']['value']['id'] == "Q73539779":
                                    return result
                                
    def get_versions(self):
        url = f"https://cran.r-project.org/src/contrib/Archive/{self.label}"

        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'lxml')
        except:
            log.warning(f"Version page for package {self.label} not found.")
        else:
            if soup.find_all('table'):
                table = soup.find_all('table')[0]
                versions_df = pd.read_html(StringIO(str(table)))
                versions_df = versions_df[0]
                versions_df = versions_df.drop(columns=['Unnamed: 0', 'Size', 'Description'])
                versions_df = versions_df.drop(index= [0, 1])
                
                for _, row in versions_df.iterrows():
                    name = row['Name']
                    publication_date = row['Last modified']
                    if isinstance(name, str):
                        version = re.sub(f'{self.label}_', '', name)
                        version = re.sub('.tar.gz', '', version)
                        
                        publication_date = publication_date.split()[0]
                        publication_date = f"+{publication_date}T00:00:00Z"
                        
                        self.versions.append((version, publication_date))
