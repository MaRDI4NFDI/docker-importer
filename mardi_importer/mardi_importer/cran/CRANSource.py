from mardi_importer.base import ADataSource
from .RPackage import RPackage

import pandas as pd
import time
import json
import os
import logging
log = logging.getLogger('CRANlogger')

class CRANSource(ADataSource):
    """Processes data from the Comprehensive R Archive Network.

    Metadata for each R package is scrapped from the CRAN Repository. Each 
    Wikibase item corresponding to each R package is subsequently updated
    or created, in case of a new package.
    
    Attributes:
        packages (Pandas dataframe): 
          Dataframe with **package name**, **title** and **date of publication** for
          each package in CRAN.
    """

    def __init__(self, user: str, password: str):
        super().__init__(user, password)
        self.packages = ""

    def setup(self):
        """Create all necessary properties and entities for CRAN
        """
        # Import entities from Wikidata
        self.import_wikidata_entities("/wikidata_entities.txt")

        # Create new required local entities
        self.create_local_entities("/new_entities.json")

    def pull(self):
        """Reads **date**, **package name** and **title** from the CRAN Repository URL.

        The result is saved as a pandas dataframe in the attribute **packages**.

        Returns:
            Pandas dataframe: Attribute ``packages``
        """
        url = r"https://cran.r-project.org/web/packages/available_packages_by_date.html"

        tables = pd.read_html(url)
        self.packages = tables[0]
        return self.packages

    def push(self):
        """Updates the MaRDI Wikibase entities corresponding to R packages.

        For each **package name** in the attribute **packages** checks 
        if the date in CRAN coincides with the date in the MaRDI 
        knowledge graph. If not, the package is updated. If the package 
        is not found in the MaRDI knowledge graph, the corresponding 
        item is created.

        It creates a :class:`mardi_importer.cran.RPackage` instance
        for each package.
        """
        # Limit the query to only 30 packages (Comment next line to process data on all ~19000 packages)
        #self.packages = self.packages.loc[:100, :]

        flag = False
        
        for _, row in self.packages.iterrows():
            package_date = row["Date"]
            package_label = row["Package"]
            package_title = row["Title"]

            #if not flag and package_label != "BeSS":
            #    continue
            #flag = True
            #if package_label == "GeoModels":

            package = RPackage(package_date, package_label, package_title, self.api)
            if package.exists():
                if not package.is_updated():
                    print(f"Package {package_label} found: Not up to date. Attempting update...")
                    package.update()
                else:
                    print(f"Package {package_label} found: Already up to date.")
            else:
                print(f"Package {package_label} not found: Attempting item creation...")
                package.create()

            time.sleep(2)
