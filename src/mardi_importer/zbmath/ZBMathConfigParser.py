from configparser import ConfigParser, NoOptionError
import sys

from mardi_importer.importer.Importer import AConfigParser


class ZBMathConfigParser(AConfigParser):
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
        if not "ZBMath" in self.config:
            sys.exit("Error: Config file does not contain section ZBMath")

        try:
            config_dict["out_dir"] = self.config["DEFAULT"]["output_directory"]
        except NoOptionError:
            sys.exit("Error: No output_directory in DEFAULT section of config")

        try:
            config_dict["tags"] = [
                x.strip() for x in self.config["ZBMath"]["tags"].split(",")
            ]
        except NoOptionError:
            sys.exit("Error: No tags in ZBMath section of config.")

        for key in ["raw_dump_path", "processed_dump_path", "split_id"]:
            try:
                val = self.config["ZBMath"][key]
                if val == "None":
                    config_dict[key] = None
                else:
                    config_dict[key] = val
            except NoOptionError:
                sys.exit("Error: No " + key + " in ZBMath section of config.")

        for key in ["from_date", "until_date"]:
            try:
                val = self.config["ZBMath"][key]
                if val == "None":
                    config_dict[key] = None
                else:
                    config_dict[key] = val
            except NoOptionError:
                sys.exit("Error: No " + key + " in ZBMath section of config.")

        return config_dict
