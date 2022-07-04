#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.importer.Importer import ADataSource, ImporterException
from mardi_importer.cran.RPackage import RPackage
import configparser
import pandas as pd
import time
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

    def __init__(self):
        self.packages = ""

    def pull(self):
        """Reads **date**, **package name** and **title** from the CRAN Repository URL.

        The result is saved as a pandas dataframe in the attribute **packages**.

        Returns:
            Pandas dataframe: Attribute ``packages``

        Raises:
            ImporterException: If table at the CRAN url cannot be accessed or read.
        """
        url = r"https://cran.r-project.org/web/packages/available_packages_by_date.html"

        try:
            tables = pd.read_html(url)  # Returns list of all tables on page
        except Exception as e:
            raise ImporterException(
                "Error attempting to read table from CRAN url\n{}".format(e)
            )
        else:
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
        #self.packages = self.packages.loc[1:30, :]

        for i, row in self.packages.iterrows():
            package_date = self.packages.loc[i, "Date"]
            package_label = self.packages.loc[i, "Package"]
            package_title = self.packages.loc[i, "Title"]
            package = RPackage(package_date, package_label, package_title)
            if package.exists():
                if not package.is_updated():
                    log.info(f"Package {package_label} found: Not up to date. Attempting update...")
                    package.update()
                else:
                    log.info(f"Package {package_label} found: Already up to date.")
            else:
                log.info(f"Package {package_label} not found: Attempting item creation...")
                package.create()

            time.sleep(2)
