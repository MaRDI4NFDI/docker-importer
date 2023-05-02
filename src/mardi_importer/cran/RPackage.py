#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.publications.ArxivPublication import ArxivPublication
from mardi_importer.publications.CrossrefPublication import CrossrefPublication
from mardi_importer.publications.ZenodoResource import ZenodoResource
from mardi_importer.publications.Author import Author
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import re
import logging
log = logging.getLogger('CRANlogger')

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
          Extended description of the R package
        url:
          URL to the CRAN repository
        version:
          Version of the R package
        author:
          Author(s) of the package
        license:
          Software license
        dependency:
          Dependencies to R and other packages
        maintainer:
          Software maintainer          
    """
    def __init__(self, date, label, title, integrator):
        self.date = date
        self.label = label
        self.description = title
        self.long_description = ""
        self.url = ""
        self.version = ""
        self.author = ""
        self.license = ""
        self.dependency = ""
        self.imports = ""
        self.maintainer = ""
        self.author_ID = []
        self.QID = None
        self.api = integrator
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.label)
        description = ""
        if self.label != self.description:
            description = self.description
        else:
            description += self.description + " (R Package)"
        item.descriptions.set(
            language="en",
            value=description
        )
        return item

    def pull(self):
        """Imports metadata from CRAN corresponding to the R package.

        Imports **Version**, **Dependencies**, **Authors**, **Maintainer**
        and **License** and saves them as instance attributes.
        """
        url = f"https://CRAN.R-project.org/package={self.label}"
        self.url = url

        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'lxml')
        except:
            log.warning(f"Package {self.label} package not found in CRAN.")
            return None
        else:
            if soup.find_all('table'):
                table = soup.find_all('table')[0]
                self.long_description = soup.find_all('p')[0].get_text()
                package_df = self.clean_package_list(table)
                if "Version" in package_df.columns:
                    self.version = package_df.loc[1, "Version"]
                if "Author" in package_df.columns:
                    self.author = package_df.loc[1, "Author"]
                if "License" in package_df.columns:
                    self.license = package_df.loc[1, "License"]
                if "Depends" in package_df.columns:
                    self.dependency = package_df.loc[1, "Depends"]
                if "Imports" in package_df.columns:
                    self.imports = package_df.loc[1, "Imports"]
                if "Maintainer" in package_df.columns:
                    self.maintainer = package_df.loc[1, "Maintainer"]
                return self
            else:
                log.warning(f"Metadata table not found in CRAN. Package has probably been archived.")
                return None


    def exists(self):
        """Checks if a WB item corresponding to the R package already exists.

        Searches for a WB item with the package label in the SQL Wikibase
        tables and returns **True** if a matching result is found.

        It uses for that the :meth:`mardi_importer.wikibase.WBItem.instance_exists()` 
        method.

        Returns: 
          String: Entity ID
        """
        if self.QID: return self.QID
        self.QID = self.item.is_instance_of('wd:Q73539779')
        #self.item.id = self.QID
        return self.QID

    def is_updated(self):
        """Checks if the WB item corresponding to the R package is up to date.

        Compares the publication date in the local knowledge graph with the
        publication date imported from CRAN.

        Returns: 
          Boolean: **True** if both dates coincide, **False** otherwise.
        """
        return self.date in self.get_WB_package_date()

    def create(self):
        """Creates a WB item with the imported metadata from CRAN.
        
        The metadata corresponding to one package is first pulled as instance 
        attributes through :meth:`pull`. Before creating the new entity 
        corresponding to an R package, new entities corresponding to dependencies 
        and authors are alreday created, when these do not already exist in the
        local Wikibase instance.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the 
        corresponding new item.

        Returns:
          String: ID of the created R package.
        """
        if self.pull():
            self.insert_claims(self.item)
            package = self.item.write()
            if package.id:
                log.info(f"Package created with ID {package.id}.")
                return package.id
            else:
                log.info(f"Package could not be created.")

    def update(self):
        """Updates existing WB item with the imported metadata from CRAN.
        
        The metadata corresponding to the package is first pulled from CRAN and
        saved as instance attributes through :meth:`pull`. The statements that 
        do not coincide with the locally saved information are updated or 
        subsituted with the updated information.

        Uses :class:`mardi_importer.wikibase.WBItem` to update the item
        corresponding to the R package.

        Returns:
          String: ID of the updated R package.
        """
        if self.pull():
            self.item = self.api.item.get(entity_id=self.QID)

            if self.item.descriptions.values.get('en') != self.description:
                description = ""
                if self.label != self.description:
                    description = self.description
                else:
                    description += self.description + " (R Package)"
                self.item.descriptions.set(
                    language="en",
                    value=description
                )

            new_item = self.api.item.new()

            self.author_ID = self.item.get_value('wdt:P50')
            self.insert_claims(new_item)

            self.item.claims.add(
                new_item.claims,
                ActionIfExists.APPEND_OR_REPLACE,
            )
            self.item.write()

            if self.QID:
                log.info(f"Package with ID {self.QID} has been updated.")
                return self.QID
            else:
                log.info(f"Package could not be updated.")
                return None
        return None

    def insert_claims(self, item):
        # Instance of: R package
        item.add_claim("wdt:P31", "wd:Q73539779")

        # Programmed in: R
        item.add_claim("wdt:P277", "wd:Q206904")

        # R package CRAN URL
        # item.add_claim("wdt:P2699", self.url)

        # Publication date
        item.add_claim("wdt:P5017", time="+%sT00:00:00Z" % (self.date))

        # Software version identifier
        qualifier = [self.api.get_claim("wdt:P577", time="+%sT00:00:00Z" % (self.date))]
        item.add_claim("wdt:P348", self.version, qualifiers=qualifier)

        # Authors
        self.author_ID = self.preprocess_authors()
        claims = []
        for author in self.author_ID:
            claims.append(self.api.get_claim("wdt:P50", author))
        item.add_claims(claims)

        # Maintainer
        maintainer_ID = self.preprocess_maintainer()
        item.add_claim("wdt:P126", maintainer_ID)
        
        # Licenses
        licenses = self.process_licenses()
        item.add_claims(licenses)

        # Dependencies
        dependencies = self.process_dependencies()
        item.add_claims(dependencies)

        # Imports
        imports = self.process_imports()
        item.add_claims(imports)

        # Related publication
        publication_list = self.preprocess_publications()
        cites_work = "wdt:P2860"
        claims = []
        for publication in publication_list:
            claims.append(self.api.get_claim(cites_work, publication))
        item.add_claims(claims)

        # CRAN Project
        item.add_claim("wdt:P5565", self.label)

        # Wikidata QID
        wikidata_QID = self.get_wikidata_QID()
        if wikidata_QID: item.add_claim("Wikidata QID", wikidata_QID)

    def preprocess_authors(self):
        """Processes the author information of each R package. This includes:

        - Searching if an author with the given ID already exists in the KG.
        - Alternatively, create WB Items for new authors.
            
        Returns:
          List: 
            Item IDs corresponding to each author.
        """

        author_ID = []
        for name, orcid in self.author.items():
            new_author = ""
            if name.lower() in ["r foundation", "the r foundation"]:
                author = self.api.query('local_id', 'Q111430684')
                author_ID.append(author)
            elif name == "R Core Team":
                author = self.api.query('local_id', 'Q116739338')
                author_ID.append(author)
            elif name == "CRAN Team":
                author = self.api.query('local_id', 'Q116739332')
                author_ID.append(author)
            else:
                author = Author(self.api, name, orcid, self.author_ID)
                if author.QID:
                    new_author = author.QID
                elif self.QID:
                    current_authors = self.item.get_value("wdt:P50")
                    for author_id in current_authors:
                        author_label = self.api.item.get(entity_id=author_id).labels.values['en']
                        if name == author_label:
                            new_author = author_id
                    if not new_author:
                        new_author = author.create()
                else:
                    new_author = author.create()
                author_ID.append(new_author)
        return author_ID

    def preprocess_maintainer(self):
        """Processes the maintainer information of each R package. This includes:

        - Providing the Item ID given the maintainer name.
        - Creating a new WB Item if the maintainer is not found in the 
          local graph.

        Returns:
          String: 
            Item ID corresponding to the maintainer.
        """
        for author in self.author_ID:
            package_author_name = str(self.api.item.get(entity_id=author).labels.values['en'])
            if Author(self.api, package_author_name).compare_names(self.maintainer):
                return author
        # Create item for the maintainer, if it does not exist already
        maintainer = self.api.item.new()
        maintainer.labels.set(language="en", value=self.maintainer)
        maintainer.add_claim("wdt:P31", "wd:Q5")
        return maintainer.write().id

    def preprocess_software(self, packages):
        """Processes the dependency and import information of each R package. This includes:

        - Extracting the version information of each dependency/import if provided.
        - Providing the Item ID given the dependency/import label.
        - Creating a new WB Item if the dependency/import is not found in the 
          local knowledge graph.

        Returns:
          Dict: 
            Dictionary with key value corresponding to the Item ID of each dependency or
            import. The value indicates the version of each dependency or import, if
            provided, which is added in the statement as a qualifier.
        """
        if packages == "dependencies":
            process_list = self.dependency
        elif packages == "imports":
            process_list = self.imports
        software = {}
        if type(process_list) is list:
            for software_string in process_list:
                software_version = re.search("\((.*?)\)", software_string)               
                software_name = re.sub("\(.*?\)", "", software_string).strip()
                item = self.api.item.new()
                item.labels.set(language="en", value=software_name)
                software_id = item.is_instance_of("wd:Q73539779") # Instance of R package
                if software_name == "R":
                    # Software = R
                    software_ID = self.api.query("local_id", "Q206904")
                elif software_id: 
                    # Software = R package
                    software_ID = software_id 
                else:
                    # Software = New instance of R package
                    item.add_claim("wdt:P31", "wd:Q73539779")
                    item.add_claim("wdt:P277", "wd:Q206904")
                    software_ID = item.write().id

                software[software_ID] = ""
                if software_version:
                    software[software_ID] = software_version.group(1)
                    
        return software

    def preprocess_publications(self):
        """Extracts the DOI identification of related publications.

        Identifies the DOI of publications that are mentioned using the 
        format *doi:* or *arXiv:* in the long description of the 
        R package.

        Returns:
          List:
            List containing the wikibase IDs of mentioned publications.
        """
        publication_id_array = []
        publication_authors = self.author_ID
        scholarly_article = "wd:Q13442814"
        doi_id = "wdt:P356"

        doi_references = re.findall('<doi:(.*?)>', self.long_description)
        arxiv_references = re.findall('<arXiv:(.*?)>', self.long_description)
        zenodo_references = re.findall('<zenodo:(.*?)>', self.long_description)

        doi_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, doi_references))
        arxiv_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, arxiv_references))
        zenodo_references = list(map(lambda x: x[:-1] if x.endswith('.') else x, zenodo_references))

        crossref_references = []

        for doi in doi_references:
            doi = doi.strip().lower()
            if re.search('10.48550/', doi):
                arxiv_id = doi.replace('10.48550/arxiv.', '')
                arxiv_references.append(arxiv_id)
            elif re.search('10.5281/', doi):
                zenodo_id = doi.replace('10.5281/zenodo.', '')
                zenodo_references.append(zenodo_id)
            else:
                crossref_references.append(doi)  

        for doi in crossref_references:
            publication = CrossrefPublication(self.api, doi, publication_authors)
            publication_item = self.api.item.new()
            publication_item.labels.set(language="en", value=publication.title)
            publication_id = publication_item.is_instance_of_with_property(scholarly_article, doi_id, doi)

            if publication_id:
                publication_item = self.api.item.get(entity_id=publication_id)
                coauthors = publication_item.get_value("wdt:P50")
                for coauthor in coauthors:
                    if coauthor not in publication_authors:
                        publication_authors.append(coauthor)
                #publication_authors += coauthors
            else:
                publication_id = publication.create()
                for coauthor in publication.coauthors:
                    if coauthor not in publication_authors:
                        publication_authors.append(coauthor)
                #publication_authors += publication.coauthors

            if publication_id:
                publication_id_array.append(publication_id)

        for arxiv_id in arxiv_references:
            arxiv_id = arxiv_id.strip()
            if ":" in arxiv_id: arxiv_id = arxiv_id.replace(":",".")
            if "10.48550/" in arxiv_id: arxiv_id = arxiv_id.lower().replace('10.48550/arxiv.', '')
            publication = ArxivPublication(self.api, arxiv_id, publication_authors)

            publication_item = self.api.item.new()
            publication_item.labels.set(language="en", value=publication.title)
            arxiv_id_prop_nr = "wdt:P818"
            publication_id = publication_item.is_instance_of_with_property(scholarly_article, arxiv_id_prop_nr, arxiv_id)

            if publication_id:
                publication_item = self.api.item.get(entity_id=publication_id)
                coauthors = publication_item.get_value("wdt:P50")
                for coauthor in coauthors:
                    if coauthor not in publication_authors:
                        publication_authors.append(coauthor)
                #publication_authors += coauthors
            else:
                publication_id = publication.create()
                for coauthor in publication.coauthors:
                    if coauthor not in publication_authors:
                        publication_authors.append(coauthor)
                #publication_authors += publication.coauthors

            if publication_id:
                publication_id_array.append(publication_id)

        for zenodo_id in zenodo_references:
            zenodo_id = zenodo_id.strip()
            if ":" in zenodo_id: zenodo_id = zenodo_id.replace(":",".")
            if "10.5281/" in zenodo_id: zenodo_id = zenodo_id.lower().replace('10.5281/zenodo.', '')

            resource_id = None
            resource = ZenodoResource(self.api, zenodo_id, publication_authors)
            
            resource_item = self.api.item.new()
            resource_item.labels.set(language="en", value=resource.title)
            zenodo_prop_nr = "wdt:4901"

            for resource_item.resource_type in ["wd:Q1172284", 
                                                "wd:Q7397", 
                                                "wd:Q604733", 
                                                "wd:Q10870555", 
                                                "wd:Q429785", 
                                                "wd:Q478798", 
                                                "wd:Q2431196", 
                                                "wd:Q379833"]:
                found = resource_item.is_instance_of_with_property(
                                resource_item.resource_type,
                                zenodo_prop_nr,
                                zenodo_id
                            )
                if found: resource_id = found

            if not resource_id:
                resource_id = resource.create()

            if resource_id:
                publication_id_array.append(resource_id)

        return publication_id_array

    def process_dependencies(self):
        """Adds the statements corresponding to the package dependencies.
        
        Insert the wikibase statements corresponding the required R package for
        the instantiated R package. The statement includes a link to the item
        representing the dependency and, when provided, a qualifier
        specifying the required version of the dependency.

        Args:
          item (WBItem):
            Item representing the R package to which the statement must be added.
        """
        preprocessed_dependencies = self.preprocess_software("dependencies")
        claims = []
        for software, version in preprocessed_dependencies.items():
            qualifier = []
            if version:
                qualifier = [self.api.get_claim("wdt:P348", version)]
            claims.append(self.api.get_claim("wdt:P1547", software, qualifiers=qualifier))
        return claims

    def process_imports(self):
        """Adds the statements corresponding to the package imports.
        
        Insert the wikibase statements corresponding the imported R packages for
        the instantiated R package. The statement includes a link to the item
        representing the imported package and, when provided, a qualifier
        specifying the required version of this package.

        Args:
          item (WBItem):
            Item representing the R package to which the imported packages
            statements must be added.
        """
        preprocessed_imports = self.preprocess_software("imports")
        prop_nr = self.api.get_local_id_by_label("imports", "property")
        claims = []
        for software, version in preprocessed_imports.items():
            qualifier = []
            if version:
                qualifier = [self.api.get_claim("wdt:P348", version)]
            claims.append(self.api.get_claim(prop_nr, software, qualifiers=qualifier))
        return claims
            
        #for software, version in preprocessed_imports.items():
        #    item.add_statement(import_property, software, WD_P348=version) if len(version) > 0 else item.add_statement(import_property, software)

    def process_licenses(self):
        """Processes the license string and adds the corresponding statements.

        The concrete License is identified and linked to the corresponding
        item that has previously been imported from Wikidata. Further license
        information, when provided between round or square brackets, is added
        as a qualifier.

        If a file license is mentioned, the linked to the file license
        in CRAN is added as a qualifier.

        Args:
          item (WBItem):
            Item representing the R package to which the statement must be added.
        """
        claims = []
        for license_str in self.license:
            license_qualifier = ""
            if re.findall("\(.*?\)", license_str):
                qualifier_groups = re.search("\((.*?)\)", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_aux = re.sub("\(.*?\)", "", license_str)
                if re.findall("\[.*?\]", license_aux):
                    qualifier_groups = re.search("\[(.*?)\]", license_str)
                    license_qualifier = qualifier_groups.group(1)
                    license_str = re.sub("\[.*?\]", "", license_aux)
                else:
                    license_str = license_aux
            elif re.findall("\[.*?\]", license_str):
                qualifier_groups = re.search("\[(.*?)\]", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_str = re.sub("\[.*?\]", "", license_str)
            license_str = license_str.strip()
            license_QID = self.get_license_QID(license_str)
            if license_str == "file LICENSE" or license_str == "file LICENCE":
                qualifier = [self.api.get_claim("wdt:P2699", f"https://cran.r-project.org/web/packages/{self.label}/LICENSE")]
                claims.append(self.api.get_claim("wdt:P275", license_QID, qualifiers=qualifier))
            elif license_QID:
                if license_qualifier:
                    qualifier = [self.api.get_claim("wdt:P9767", text=license_qualifier)]
                    claims.append(self.api.get_claim("wdt:P275", license_QID, qualifiers=qualifier))
                else:
                    claims.append(self.api.get_claim("wdt:P275", license_QID))
        return claims

    def get_WB_package_date(self):
        """Reads the package publication date saved in the local Wikibase instance.

        Queries the WB Item corresponding to the R package label through the 
        Wikibase API.

        Returns:
            String: Package publication date in format DD-MM-YYYY.
        """
        package_dates = self.item.get_value("wdt:P5017") or []
        return list(map(lambda x: x[1:11], package_dates))

    def clean_package_list(self, table_html):
        """Processes raw imported data from CRAN to enable the creation of items.

        - Package dependencies are splitted at the comma position.
        - License information is processed using the :meth:`split_license` method.
        - Author information is processed using the :meth:`split_authors` method.
        - Maintainer information is processed using the :meth:`clean_maintainer` method.

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
        package_df = pd.read_html(str(table_html))
        package_df = package_df[0].set_index(0).T
        package_df.columns = package_df.columns.str[:-1]
        if "Depends" in package_df.columns:
            package_df["Depends"] = package_df["Depends"].apply(self.split_list)
        if "Imports" in package_df.columns:
            package_df["Imports"] = package_df["Imports"].apply(self.split_list)
        if "License" in package_df.columns:
            package_df["License"] = package_df["License"].apply(self.split_license)
        if "Author" in package_df.columns:
            package_df["Author"] = str(table_html.find("td", text="Author:").find_next_sibling("td")).replace('\n', '').replace('\r', '')
            package_df["Author"] = package_df["Author"].apply(self.split_authors)
        if "Maintainer" in package_df.columns:
            package_df["Maintainer"] = package_df["Maintainer"].apply(self.clean_maintainer)
        return package_df

    @staticmethod
    def split_list(x):
        """Splits given list in the comma position.

        Args:
            x (String): String to be splitted.
        Returns:
            (List): List of elements
        """
        return [] if pd.isna(x) else str(x).split(", ")

    @staticmethod
    def split_license(x):
        """Splits string of licenses.

        Takes into account that licenses are often not uniformly listed.
        Characters \|, + and , are used to separate licenses. Further
        details on each license are often included in square brackets.

        Args:
            x (String): String imported from CRAN representing license
              information.

        Returns:
            (List): List of licenses.
        """
        if not pd.isna(x):
            licenses = str(x).split(" | ")
            license_list = []
            i = 0
            while i in range(len(licenses)):
                if not re.findall("\[", licenses[i]) or (
                    re.findall("\[", licenses[i]) and re.findall("\]", licenses[i])
                ):
                    license_list.append(licenses[i])
                    i += 1
                elif re.findall("\[", licenses[i]) and not re.findall(
                    "\]", licenses[i]
                ):
                    j = i + 1
                    license_aux = licenses[i]
                    closed = False
                    while j < len(licenses) and not closed:
                        license_aux += " | "
                        license_aux += licenses[j]
                        if re.findall("\]", licenses[j]):
                            closed = True
                        j += 1
                    license_list.append(license_aux)
                    i = j
            split_list = []
            for item in license_list:
                items = item.split(" + ")
                i = 0
                while i in range(len(items)):
                    if not re.findall("\[", items[i]) or (
                        re.findall("\[", items[i]) and re.findall("\]", items[i])
                    ):
                        split_list.append(items[i])
                        i += 1
                    elif re.findall("\[", items[i]) and not re.findall("\]", items[i]):
                        j = i + 1
                        items_aux = items[i]
                        closed = False
                        while j < len(items) and not closed:
                            items_aux += " + "
                            items_aux += items[j]
                            if re.findall("\]", items[j]):
                                closed = True
                            j += 1
                        split_list.append(items_aux)
                        i = j
            return list(dict.fromkeys(split_list))
        else:
            return []

    def split_authors(self,x):
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
        x = re.sub("<td>", "", x)
        x = re.sub("</td>", "", x)
        x = re.sub("<img alt.*?a>", "", x)
        x = re.sub("\(.*?\)", "", x)
        x = re.sub("\t", "", x)
        x = re.sub("ORCID iD", "", x)
        authors = re.findall(".*?\]", x)
        author_dict = {}
        if authors:
            for author in authors:
                labels = re.findall("\[.*?\]", author)
                if labels:
                    is_author = re.findall("aut", labels[0])
                    if is_author:
                        orcid = None
                        if re.findall("\d{4}-\d{4}-\d{4}-.{4}", author):
                            orcid = re.findall("\d{4}-\d{4}-\d{4}-.{4}", author)[0]
                        author = re.sub("<a href=.*?>", "", author)
                        author = re.sub("\[.*?\]", "", author)
                        author = re.sub("^\s?,", "", author)
                        author = re.sub("^\s?and\s?", "", author)
                        author = re.sub(
                            "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "", author
                        )
                        author = author.strip()
                        multiple_words = author.split(" ")
                        if len(multiple_words) > 1:
                            author = self.capitalize_author(author)
                            if author:
                                author_dict[author] = orcid
        else:
            authors_comma = x.split(", ")
            authors_and = x.split(" and ")
            if len(authors_and) > len(authors_comma):
                author = re.sub(
                    "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "", authors_and[0]
                )
            else:
                author = re.sub(
                    "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
                    "",
                    authors_comma[0],
                )
            if len(author.split(" ")) > 5 or re.findall("[@\(\)\[\]&]", author):
                author = ""
            author = self.capitalize_author(author)
            if author:
                author_dict[author] = None
        return author_dict

    def capitalize_author(self, author):
        if author != "":
            author_terms = author.split()
            author = author_terms[0].capitalize()
            for index in range(1,len(author_terms)):
                author = author + " " + author_terms[index].capitalize()
        return author

    def clean_maintainer(self, x):
        """Remove unnecessary information from maintainer string.

        Args:
            x (String): String imported from CRAN which my contain e-mail
                        address and comments within brackets

        Returns:
            (String): Name of the maintainer
        """
        if not pd.isna(x):
            x = re.sub("<.*?>", "", x)
            x = re.sub("\(.*?\)", "", x)
            x = self.capitalize_author(x)
            return x.strip()
        return x

    def get_license_QID(self, license_str):
        """Returns the Wikidata item ID corresponding to a software license.

        The same license is often denominated in CRAN using differents names.
        This function returns the wikidata item ID corresponding to a single
        unique license that is referenced in CRAN under different names (e.g.
        *Artistic-2.0* and *Artistic License 2.0* both refer to the same 
        license, corresponding to item *Q14624826*).

        Args:
            license (String): String corresponding to a license imported from CRAN.

        Returns:
            (String): Wikidata item ID.
        """
        if license_str == "ACM":
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value="ACM Software License Agreement")
            return license_item.is_instance_of("wd:Q207621")
        elif license_str == "AGPL":
            return "wd:Q28130012"
        elif license_str == "AGPL-3":
            return "wd:Q27017232"
        elif license_str == "Apache License":
            return "wd:Q616526"
        elif license_str == "Apache License 2.0":
            return "wd:Q13785927"
        elif license_str == "Apache License version 1.1":
            return "wd:Q17817999"
        elif license_str == "Apache License version 2.0":
            return "wd:Q13785927"
        elif license_str == "Artistic-2.0":
            return "wd:Q14624826"
        elif license_str == "Artistic License 2.0":
            return "wd:Q14624826"
        elif license_str == "BSD 2-clause License":
            return "wd:Q18517294"
        elif license_str == "BSD 3-clause License":
            return "wd:Q18491847"
        elif license_str == "BSD_2_clause":
            return "wd:Q18517294"
        elif license_str == "BSD_3_clause":
            return "wd:Q18491847"
        elif license_str == "BSL":
            return "wd:Q2353141"
        elif license_str == "BSL-1.0":
            return "wd:Q2353141"
        elif license_str == "CC0":
            return "wd:Q6938433"
        elif license_str == "CC BY 4.0":
            return "wd:Q20007257"
        elif license_str == "CC BY-SA 4.0":
            return "wd:Q18199165"
        elif license_str == "CC BY-NC 4.0":
            return "wd:Q34179348"
        elif license_str == "CC BY-NC-SA 4.0":
            return "wd:Q42553662"
        elif license_str == "CeCILL":
            return "wd:Q1052189"
        elif license_str == "CeCILL-2":
            return "wd:Q19216649"
        elif license_str == "Common Public License Version 1.0":
            return "wd:Q2477807"
        elif license_str == "CPL-1.0":
            return "wd:Q2477807"
        elif license_str == "Creative Commons Attribution 4.0 International License":
            return "wd:Q20007257"
        elif license_str == "EPL":
            return "wd:Q1281977"
        elif license_str == "EUPL":
            return "wd:Q1376919"
        elif license_str == "EUPL-1.1":
            return "wd:Q1376919"
        elif license_str == "file LICENCE" or license_str == "file LICENSE":
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value="File License")
            return license_item.is_instance_of("wd:Q207621")
        elif license_str == "FreeBSD":
            return "wd:Q34236"
        elif license_str == "GNU Affero General Public License":
            return "wd:Q1131681"
        elif license_str == "GNU General Public License":
            return "wd:Q7603"
        elif license_str == "GNU General Public License version 2":
            return "wd:Q10513450"
        elif license_str == "GNU General Public License version 3":
            return "wd:Q10513445"
        elif license_str == "GPL":
            return "wd:Q7603"
        elif license_str == "GPL-2":
            return "wd:Q10513450"
        elif license_str == "GPL-3":
            return "wd:Q10513445"
        elif license_str == "LGPL":
            return "wd:Q192897"
        elif license_str == "LGPL-2":
            return "wd:Q23035974"
        elif license_str == "LGPL-2.1":
            return "wd:Q18534390"
        elif license_str == "LGPL-3":
            return "wd:Q18534393"
        elif license_str == "Lucent Public License":
            return "wd:Q6696468"
        elif license_str == "MIT":
            return "wd:Q334661"
        elif license_str == "MIT License":
            return "wd:Q334661"
        elif license_str == "Mozilla Public License 1.1":
            return "wd:Q26737735"
        elif license_str == "Mozilla Public License 2.0":
            return "wd:Q25428413"
        elif license_str == "Mozilla Public License Version 2.0":
            return "wd:Q25428413"
        elif license_str == "MPL":
            return "wd:Q308915"
        elif license_str == "MPL version 1.0":
            return "wd:Q26737738"
        elif license_str == "MPL version 1.1":
            return "wd:Q26737735"
        elif license_str == "MPL version 2.0":
            return "wd:Q25428413"
        elif license_str == "MPL-1.1":
            return "wd:Q26737735"
        elif license_str == "MPL-2.0":
            return "wd:Q25428413"
        elif license_str == "Unlimited":
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value="Unlimited License")
            return license_item.is_instance_of("wd:Q207621")

    def get_wikidata_QID(self):
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
