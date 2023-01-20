from configparser import ConfigParser, NoOptionError
import sys

from mardi_importer.importer.Importer import AConfigParser


class IntegratorConfigParser(AConfigParser):
    """Config parser for ZBMath data"""

    def __init__(self, config_path):
        """
        Args:
            config_path (string): path to config file
        """
        self.config_path = config_path
        self.config = ConfigParser()

    def parse_config(self):
        """
        Overrides abstract method.
        This method reads a config file containing the config dfor handling ZBMath data.

        Returns:
            dict: dict of (config_key, value) pairs extracted from config file
        """

        config_dict = {}
        self.config.read(self.config_path)
        if not "Integrator" in self.config:
            sys.exit("Error: Config file does not contain section Integrator")
        for key in ["mediawiki_api_url", "sparql_endpoint_url", "wikibase_url"]:
            try:
                config_dict[key] = self.config["Integrator"][key]
            except NoOptionError:
                sys.exit("Error: No " + key + " in Integrator section of config.")

        return config_dict
