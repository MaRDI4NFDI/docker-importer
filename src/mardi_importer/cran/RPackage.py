#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBMapping import get_wbs_local_id
from mardi_importer.wikibase.SPARQLUtils import SPARQL_exists
from mardi_importer.crossref.Publication import Publication
from mardi_importer.crossref.Author import Author
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
        self.QID = None
        self.api = integrator
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.label)
        item.descriptions.set(
            language="en", 
            value=self.description
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
        return self.QID

    def is_updated(self):
        """Checks if the WB item corresponding to the R package is up to date.

        Compares the publication date in the local knowledge graph with the
        publication date imported from CRAN.

        Returns: 
          Boolean: **True** if both dates coincide, **False** otherwise.
        """
        return self.date == self.get_WB_package_date()

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

            # Instance of: R package
            self.item.add_claim("wdt:P31", "wd:Q73539779")

            # R package CRAN URL
            self.item.add_claim("wdt:P2699", self.url)

            # Publication date
            self.item.add_claim("wdt:P5017", time="+%sT00:00:00Z" % (self.date))

            # Software version identifier
            qualifier = [self.api.get_claim("wdt:P577", time="+%sT00:00:00Z" % (self.date))]
            self.item.add_claim("wdt:P348", self.version, qualifiers=qualifier)

            # Authors
            author_ID = self.preprocess_authors()
            claims = []
            for author in author_ID:
                claims.append(self.api.get_claim("wdt:P50", author))
            self.item.add_claims(claims)

            # Maintainer
            maintainer_ID = self.preprocess_maintainer(author_ID)
            self.item.add_claim("wdt:P126", maintainer_ID)
            ##########################################################
            
            # Licenses
            self.add_licenses()

            # Dependencies
            self.add_dependencies()

            # Imports
            self.add_imports()

            # Related publication
            publication_list = self.preprocess_publications(author_ID)
            cites_work = "wdt:P2860"
            claims = []
            for publication in publication_list:
                claims.append(self.api.get_claim(cites_work, publication))
            self.item.add_claims(claims)

            #print(self.item)
            package = self.item.write()
            if package.id:
                log.info(f"Package created with ID {package.id}.")
                return package.id
            else:
                log.info(f"Package could not be created.")
                return None
        return None

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
            item = WBItem(ID=self.ID)

            if self.description != item.get_description():
                item.add_description(self.description)

            # Update URL
            wb_url = item.get_value("WD_P2699")
            if not len(wb_url) > 0:
                item.add_statement("WD_P2699", self.url)

            # Update Publication Date
            wb_date = item.get_value("WD_P5017")
            if len(wb_date) > 0:
                if self.date != wb_date[0]:
                    claim_guid = item.get_claim_guid("WD_P5017")[0]
                    statement = item.return_statement("WD_P5017", "+%sT00:00:00Z" % (self.date))
                    item.update_claim(claim_guid, statement)
            else:
                item.add_statement("WD_P5017", "+%sT00:00:00Z" % (self.date))

            # Update version
            wb_version = item.get_value("WD_P348")
            if len(wb_version) > 0:
                if self.version not in wb_version[0]:
                    item.add_statement("WD_P348", self.version, WD_P577="+%sT00:00:00Z" % (self.date))

            # Update authors
            author_ID = self.preprocess_authors()
            if author_ID != item.get_value("WD_P50"):
                claim_guid = item.get_claim_guid("WD_P50")
                item.remove_claim(claim_guid)
                for author in author_ID:
                    item.add_statement("WD_P50", author)

            # Update maintainer
            wb_maintainer = item.get_value("WD_P126")
            maintainer_ID = self.preprocess_maintainer(author_ID)
            if len(wb_maintainer) > 0:
                if maintainer_ID != wb_maintainer[0]:
                    claim_guid = item.get_claim_guid("WD_P126")[0]                    
                    statement = item.return_statement("WD_P126", maintainer_ID)
                    item.update_claim(claim_guid, statement)
            else:
                item.add_statement("WD_P126", maintainer_ID)

            # Substitute License information
            claim_guid = item.get_claim_guid("WD_P275")
            item.remove_claim(claim_guid)
            self.add_licenses(item)

            # Substitute Dependency information
            claim_guid = item.get_claim_guid("WD_P1547")
            item.remove_claim(claim_guid)
            self.add_dependencies(item)

            # Substitute Imports information
            software_property = WBItem("Property for items about software").label_exists()
            import_property = WBProperty("imports").instance_exists(software_property)
            claim_guid = item.get_claim_guid(import_property)
            item.remove_claim(claim_guid)
            self.add_imports(item)

            # Add Publicationspublication_list = self.preprocess_publications(author_ID)
            publication_list = self.preprocess_publications(author_ID)
            related_publication = WBProperty("related publication").label_exists()
            claim_guid = item.get_claim_guid(related_publication)
            item.remove_claim(claim_guid)
            for publication in publication_list:
                item.add_statement(related_publication,publication)

            package_ID = item.update()
            if package_ID:
                log.info(f"Package with ID {package_ID} has been updated.")
                return package_ID
            else:
                log.info(f"Package could not be updated.")
                return None
        return None

    def preprocess_authors(self):
        """Processes the author information of each R package. This includes:

        - Searching if an author with the given ID already exists in the KG.
        - Alternatively, create WB Items for new authors.
            
        Returns:
          List: 
            Item IDs corresponding to each author.
        """
        author_ID = []
        for author, orcid in self.author.items():
            author_qid = None
            human = "wd:Q5"
            orcid_id = "wdt:P496"
            if orcid:
                item = self.api.item.new()
                item.labels.set(language="en", value=author)
                author_qid = item.is_instance_of_with_property(human, orcid_id, orcid)
            if not author_qid and self.QID:
                current_authors_id = self.item.get_value("wdt:P50")
                for author_id in current_authors_id:
                    author_label = self.api.item.get(entity_id=author_id).labels.values['en']
                    if author == author_label:
                        author_qid = author_id
            if not author_qid:
                if len(author) > 0:
                    author_item = Author(author, self.api)
                    if orcid:
                        author_item.add_orcid(orcid)
                    author_qid = author_item.create()
            if author_qid:
                author_ID.append(author_qid)
        return author_ID

    def preprocess_maintainer(self, author_ID):
        """Processes the maintainer information of each R package. This includes:

        - Providing the Item ID given the maintainer name.
        - Creating a new WB Item if the maintainer is not found in the 
          local graph.

        Returns:
          String: 
            Item ID corresponding to the maintainer.
        """
        for author in author_ID:
            package_author_name = str(self.api.item.get(entity_id=author).labels.values['en'])
            if Author(package_author_name, self.api).compare_names(self.maintainer):
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
                    software_ID = item.write().id

                software[software_ID] = ""
                if software_version:
                    software[software_ID] = software_version.group(1)
                    
        return software

    def preprocess_publications(self, author_ID):
        """Extracts the DOI identification of related publications.

        Identifies the DOI of publications that are mentioned using the 
        format *doi:* or *arXiv:* in the long description of the 
        R package.

        Returns:
          List:
            List containing the wikibase IDs of mentioned publications.
        """
        doi_list = []
        doi_references = re.findall('<doi:(.*?)>', self.long_description)
        arxiv_references = re.findall('<arXiv:(.*?)>', self.long_description)
        for reference in doi_references:
            doi_list.append(reference[:-1]) if reference[-1] == "." else doi_list.append(reference)
        for reference in arxiv_references:
            doi_list.append('10.48550/arXiv.' + reference[-1]) if reference[-1] == "." else doi_list.append('10.48550/arXiv.' + reference)

        publication_id_array = []
        for doi in doi_list:
            #scientific_publication = get_wbs_local_id("Q591041")
            #doi_id = get_wbs_local_id("P356")

            publication = Publication(doi, self.api)
            publication.add_related_authors(author_ID)
            publication.pull()

            publication_item = self.api.item.new()
            publication_item.labels.set(language="en", value=publication.title)
            publication_id = publication_item.is_instance_of_with_property("wd:Q591041", "wdt:P356", doi)
            #publication_id = WBItem(publication.title).instance_property_exists(scientific_publication, doi_id, doi)

            if not publication_id:
                publication_id = publication.create()
                # This
                author_ID = publication.related_authors

            if publication_id:
                publication_id_array.append(publication_id)

        return publication_id_array

    def add_dependencies(self):
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
        self.item.add_claims(claims)

    def add_imports(self):
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
        self.item.add_claims(claims)
            
        #for software, version in preprocessed_imports.items():
        #    item.add_statement(import_property, software, WD_P348=version) if len(version) > 0 else item.add_statement(import_property, software)

    def add_licenses(self):
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
            #license_property = WBProperty("License version").label_exists()
            if license_str == "file LICENSE" or license_str == "file LICENCE":
                qualifier = [self.api.get_claim("wdt:P2699", f"https://cran.r-project.org/web/packages/{self.label}/LICENSE")]
                claims.append(self.api.get_claim("wdt:P275", license_QID, qualifiers=qualifier))
            elif license_QID:
                if license_qualifier:
                    qualifier = [self.api.get_claim("wdt:P9767", text=license_qualifier)]
                    claims.append(self.api.get_claim("wdt:P275", license_QID, qualifiers=qualifier))
                else:
                    claims.append(self.api.get_claim("wdt:P275", license_QID))
        self.item.add_claims(claims)

    def get_WB_package_date(self):
        """Reads the package publication date saved in the local Wikibase instance.

        Queries the WB Item corresponding to the R package label through the 
        Wikibase API.

        Returns:
            String: Package publication date in format DD-MM-YYYY.
        """
        try:
            values = self.item.get_value("wdt:P5017")
            if len(values) > 0:
                return values[0][1:11]
            return None
        except:
            return None

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
            author_dict[author] = None
        return author_dict

    @staticmethod
    def capitalize_author(author):
        if author != "":
            author_terms = author.split()
            author = author_terms[0].capitalize()
            for index in range(1,len(author_terms)):
                author = author + " " + author_terms[index].capitalize()
        return author

    def clean_maintainer(self,x):
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
