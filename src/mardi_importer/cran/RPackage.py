#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.wikibase.WBItem import WBItem
from mardi_importer.wikibase.WBProperty import WBProperty
import pandas as pd
import json
import re


class RPackage:
    """Class to manage R package items in the local Wikibase instance.

    Attributes:
        wb_connection: 
          Provides a bot session to connect to the local instance of wikibase.
        date:
          Date of publication
        label:
          Package name
        description:
          Title of the R package
        url:
          URL to the CRAN repository
        version:
          Version of the R package
        Author:
          Author(s) of the package
        License:
          Software license
        Dependency:
          Dependencies to R and other packages
        Maintainer:
          Software maintainer          
    """
    def __init__(self, wb_connection, date, label, title):
        self.wb_connection = wb_connection
        self.date = date
        self.label = label
        self.description = title
        self.url = ""
        self.version = ""
        self.author = ""
        self.license = ""
        self.dependency = ""
        self.maintainer = ""

    def exists(self):
        """Checks if a WB item corresponding to the R package already exists.

        Searches for a WB item with the package label and returns **True**
        if a matching result is found.

        Returns: 
          Boolean: **True** if package exists, **False** otherwise.
        """
        return not not self.wb_connection.read_entity_by_title("item", self.label)

    def is_updated(self):
        """Checks if the WB item corresponding to the R package is up to date.

        Compares the publication date in the local knowledge graph with the
        publication date imported from CRAN.

        Returns: 
          Boolean: **True** if both dates coincide, **False** otherwise.
        """
        return self.date == self.get_WB_package_date()

    def update(self):
        """Updates existing WB item with the imported metadata from CRAN.
        """
        pass

    def create(self):
        """Creates a WB item with the imported metadata from CRAN.
        
        The metadata corresponding to one package is first pulled as instance 
        attributes through :meth:`pull`. Before creating the new entity 
        corresponding to an R package, new entities corresponding to dependencies 
        and authors are alreday created, when these do not already exist in the
        local Wikibase instance.

        Uses :class:`mardi_importer.wikibase.WBItem` to create the 
        corresponding new item.
        """
        self.pull()

        # Create items for each Author, if they do not exist already
        author_ID = []
        for author in self.author:
            item = WBItem(author)
            author_ID.append(
                item.exists() or item.add_statement("WD_P31", "WD_Q5").create()
            )

        # Create item for the maintainer, if it does not exist already
        item = WBItem(self.maintainer)
        maintainer_ID = item.exists() or item.add_statement("WD_P31", "WD_Q5").create()

        # Create items for the dependencies (including dependency version), if they do not exist already
        dependency_ID = []
        dependency_version = []
        if type(self.dependency) is list:
            for dependency in self.dependency:
                version = re.findall("\(.*?\)", dependency)
                dependency = re.sub("\(.*?\)", "", dependency).strip()
                item = WBItem(dependency)
                dependency_ID.append(
                    item.exists()
                    or item.add_statement("WD_P31", "WD_Q73539779").create()
                )
                dependency_version.append(
                    re.sub("\)", "", re.sub("\(", "", version[0])) if version else ""
                )

        item = WBItem(self.label)
        item.add_description(self.description)
        item.add_statement("WD_P31", "WD_Q73539779")
        item.add_statement("WD_P2699", self.url)
        item.add_statement("WD_P577", "+%sT00:00:00Z" % (self.date))
        item.add_statement("WD_P348", self.version)
        for author in author_ID:
            item.add_statement("WD_P50", author)

        for license in self.license:
            license_qualifier = ""
            if re.findall("\(.*?\)", license):
                license_qualifier = re.findall("\(.*?\)", license)[0]
                license = re.sub("\(.*?\)", "", license)
            elif re.findall("\[.*?\]", license):
                license_qualifier = re.findall("\[.*?\]", license)[0]
                license = re.sub("\[.*?\]", "", license)
            license = license.strip()
            license_ID = self.get_license_ID(license)
            license_property = WBProperty("License version").exists()
            if license_ID:
                item.add_statement(
                    "WD_P275", license_ID, **{license_property: license_qualifier}
                ) if license_qualifier else item.add_statement("WD_P275", license_ID)

        for i, dependency in enumerate(dependency_ID):
            if len(dependency_version[i]) > 0:
                item.add_statement(
                    "WD_P1547", dependency, WD_P348=dependency_version[i]
                )
            else:
                item.add_statement("WD_P1547", dependency)
        item.add_statement("WD_P126", maintainer_ID)

        new_id = item.create()
        print(new_id)

    def pull(self):
        """Imports metadata from CRAN corresponding to the R package.

        Imports **Version**, **Dependencies**, **Authors**, **Maintainer**
        and **License** and saves them as instance attributes.
        """
        url = f"https://CRAN.R-project.org/package={self.label}"
        self.url = url

        raw_data = pd.read_html(url)
        raw_data = raw_data[0].set_index(0).T
        raw_data.columns = raw_data.columns.str[:-1]

        package_df = self.clean_package_list(raw_data)
        if "Version" in package_df.columns:
            self.version = package_df.loc[1, "Version"]
        if "Author" in package_df.columns:
            self.author = package_df.loc[1, "Author"]
        if "License" in package_df.columns:
            self.license = package_df.loc[1, "License"]
        if "Depends" in package_df.columns:
            self.dependency = package_df.loc[1, "Depends"]
        if "Maintainer" in package_df.columns:
            self.maintainer = package_df.loc[1, "Maintainer"]

    def get_WB_package_date(self):
        """Reads the package publication date saved in the local Wikibase instance.

        Queries the WB Item corresponding to the R package label through the 
        MediaWiki API.

        Returns:
            String: Package publication date in format DD-MM-YYYY.
        """
        try:
            packageID = self.wb_connection.read_entity_by_title("item", self.label)
            params = {"action": "wbgetentities", "ids": packageID}
            r1 = self.wb_connection.session.post(
                self.wb_connection.WIKIBASE_API, data=params
            )
            r1.json = r1.json()
            time_package = r1.json["entities"][packageID]["claims"]["P9"][0]
            return time_package["mainsnak"]["datavalue"]["value"]["time"][1:11]
        except:
            return None

    def clean_package_list(self, package_df):
        """Processes raw imported data from CRAN to enable the creation of items.

        - Package dependencies are splitted at the comma position.
        - License information is processed using the :meth:`split_license` function.
        - Author information is processed using the :meth:`split_author` function.
        - Maintainer information is splitted between name and e-mail.

        Args:
            package_df (Pandas dataframe):
              Dataframe with raw data corresponding to an R package imported from
              CRAN.
        Returns:
            (Pandas dataframe): 
              Dataframe with processed data from a single R package including columns: 
              **Version**, **Author**, **License**, **Depends**, **Imports** 
              and **Maintainer**.
        """
        if "Depends" in package_df.columns:
            package_df["Depends"] = package_df["Depends"].apply(self.split_list)
        if "Imports" in package_df.columns:
            package_df["Imports"] = package_df["Imports"].apply(self.split_list)
        if "License" in package_df.columns:
            package_df["License"] = package_df["License"].apply(self.split_license)
        if "Author" in package_df.columns:
            package_df["Author"] = package_df["Author"].apply(self.split_authors)
        if "Maintainer" in package_df.columns:
            package_df[["Maintainer", "Email_Maintainer"]] = package_df[
                "Maintainer"
            ].str.split(" <", 1, expand=True)
            package_df["Email_Maintainer"] = package_df["Email_Maintainer"].apply(
                lambda x: x[:-1]
            )
        return package_df

    @staticmethod
    def split_list(x):
        """Splits given list in the comma position.

        Args:
            x (String): String to be splitted.
        Returns:
            (List): List of elements
        """
        return x if pd.isna(x) else str(x).split(", ")

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
            return x

    @staticmethod
    def split_authors(x):
        """Splits the string corresponding to the authors into a list.

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
            (List): List of authors.
        """
        x = re.sub("\(.*?\)", "", x)
        x = re.sub("\t", "", x)
        x = re.sub("ORCID iD", "", x)
        authors = re.findall(".*?\]", x)
        if authors:
            author_array = []
            for author in authors:
                labels = re.findall("\[.*?\]", author)
                if labels:
                    is_author = re.findall("aut", labels[0])
                    if is_author:
                        author = re.sub("\[.*?\]", "", author)
                        author = re.sub("^\s?,", "", author)
                        author = re.sub("^\s?and\s?", "", author)
                        author = re.sub(
                            "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "", author
                        )
                        author = author.strip()
                        multiple_words = author.split(" ")
                        if len(multiple_words) > 1:
                            author_array.append(author)
            return author_array
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
            if len(author.split(" ")) > 5:
                author = ""
            return [author.strip()]

    @staticmethod
    def get_license_ID(license):
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
        if license == "ACM":
            item = WBItem(license)
            return item.exists()
        elif license == "AGPL":
            return "WD_Q28130012"
        elif license == "AGPL-3":
            return "WD_Q27017232"
        elif license == "Apache License":
            return "WD_Q616526"
        elif license == "Apache License 2.0":
            return "WD_Q13785927"
        elif license == "Apache License version 1.1":
            return "WD_Q17817999"
        elif license == "Apache License version 2.0":
            return "WD_Q13785927"
        elif license == "Artistic-2.0":
            return "WD_Q14624826"
        elif license == "Artistic License 2.0":
            return "WD_Q14624826"
        elif license == "BSD 2-clause License":
            return "WD_Q18517294"
        elif license == "BSD 3-clause License":
            return "WD_Q18491847"
        elif license == "BSD_2_clause":
            return "WD_Q18517294"
        elif license == "BSD_3_clause":
            return "WD_Q18491847"
        elif license == "BSL":
            return "WD_Q2353141"
        elif license == "BSL-1.0":
            return "WD_Q2353141"
        elif license == "CC0":
            return "WD_Q6938433"
        elif license == "CC BY 4.0":
            return "WD_Q20007257"
        elif license == "CC BY-SA 4.0":
            return "WD_Q18199165"
        elif license == "CC BY-NC 4.0":
            return "WD_Q34179348"
        elif license == "CC BY-NC-SA 4.0":
            return "WD_Q42553662"
        elif license == "CeCILL":
            return "WD_Q1052189"
        elif license == "CeCILL-2":
            return "WD_Q19216649"
        elif license == "Common Public License Version 1.0":
            return "WD_Q2477807"
        elif license == "CPL-1.0":
            return "WD_Q2477807"
        elif license == "Creative Commons Attribution 4.0 International License":
            return "WD_Q20007257"
        elif license == "EPL":
            return "WD_Q1281977"
        elif license == "EUPL":
            return "WD_Q1376919"
        elif license == "EUPL-1.1":
            return "WD_Q1376919"
        elif license == "file LICENCE":
            item = WBItem("file LICENCE")
            return item.exists()
        elif license == "file LICENSE":
            item = WBItem(license)
            return item.exists()
        elif license == "FreeBSD":
            return "WD_Q34236"
        elif license == "GNU Affero General Public License":
            return "WD_Q1131681"
        elif license == "GNU General Public License":
            return "WD_Q7603"
        elif license == "GNU General Public License version 2":
            return "WD_Q10513450"
        elif license == "GNU General Public License version 3":
            return "WD_Q10513445"
        elif license == "GPL":
            return "WD_Q7603"
        elif license == "GPL-2":
            return "WD_Q10513450"
        elif license == "GPL-3":
            return "WD_Q10513445"
        elif license == "LGPL":
            return "WD_Q192897"
        elif license == "LGPL-2":
            return "WD_Q23035974"
        elif license == "LGPL-2.1":
            return "WD_Q18534390"
        elif license == "LGPL-3":
            return "WD_Q18534393"
        elif license == "Lucent Public License":
            return "WD_Q6696468"
        elif license == "MIT":
            return "WD_Q334661"
        elif license == "MIT License":
            return "WD_Q334661"
        elif license == "Mozilla Public License 1.1":
            return "WD_Q26737735"
        elif license == "Mozilla Public License 2.0":
            return "WD_Q25428413"
        elif license == "Mozilla Public License Version 2.0":
            return "WD_Q25428413"
        elif license == "MPL":
            return "WD_Q308915"
        elif license == "MPL version 1.0":
            return "WD_Q26737738"
        elif license == "MPL version 1.1":
            return "WD_Q26737735"
        elif license == "MPL version 2.0":
            return "WD_Q25428413"
        elif license == "MPL-1.1":
            return "WD_Q26737735"
        elif license == "MPL-2.0":
            return "WD_Q25428413"
        elif license == "Unlimited":
            item = WBItem("Unlimited License")
            return item.exists()
