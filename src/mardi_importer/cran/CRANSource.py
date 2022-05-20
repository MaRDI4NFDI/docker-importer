#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mardi_importer.importer.Importer import ADataSource, ImporterException
from mardi_importer.wikibase.WBAPIConnection import WBAPIConnection
from mardi_importer.cran.Rpackage import Rpackage
import configparser
from datetime import date
import pandas as pd
import requests
import time
import re


class CRANSource(ADataSource):
    """Reads data from the Comprehensive R Archive Network by scrapping the website"""

    def __init__(self):
        self.packages = ""

    def pull(self):
        """
        Overrides abstract method.
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

    def push(self):
        config = configparser.ConfigParser()
        config.sections()
        config.read("/config/credentials.ini")
        username = config["default"]["username"]
        botpwd = config["default"]["password"]
        WIKIBASE_API = config["default"]["WIKIBASE_API"]
        wb_connection = WBAPIConnection(username, botpwd, WIKIBASE_API)

        # Limit the query to only 5 packages (Comment next line to process data on all ~19000 packages)
        self.packages = self.packages.loc[1:30, :]

        for i, row in self.packages.iterrows():
            package_date = self.packages.loc[i, "Date"]
            package_label = self.packages.loc[i, "Package"]
            package_title = self.packages.loc[i, "Title"]
            package = Rpackage(
                wb_connection, package_date, package_label, package_title
            )

            print(package_label)
            if package.exists():
                if not package.is_updated():
                    package.update()
            else:
                package.create()

            time.sleep(2)
