from mardi_importer.integrator.IntegratorUnit import IntegratorUnit
from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
import os

import sys


class Integrator:
    """What this is supposed to do:
    - import one or many entities
    - filter by languages
    - not their dependencies or import them one level deep
    - have a connection to wikidata for pulling and one to
        local wiki for posting
    - use bot user for this
    """

    def __init__(self, conf_path) -> None:
        self.primary_integrator_units = {}
        self.secondary_integrator_units = {}  # items mentioned in statements
        self.wikibase_integrator = WikibaseIntegrator()
        self.imported_items = []
        config_parser = IntegratorConfigParser(conf_path)
        self.config_dict = config_parser.parse_config()

    def create_units(self, id_list, languages, recurse):
        """Downloads data from wikidata and saves them
        as IntegratorUnits
        """
        self.change_config(instance="wikidata")
        for item_id in id_list:
            (
                labels,
                descriptions,
                aliases,
                claims,
                entity_type,
            ) = self.get_wikidata_information(
                wikidata_id=item_id, languages=languages, recurse=recurse
            )
            if recurse == True:
                # for each of the ids in the claims, get stuff with recurse = False
                # and add to secondary
                for secondary_id in claims:
                    # add property
                    if secondary_id not in self.secondary_integrator_units:
                        (
                            sec_labels,
                            sec_descriptions,
                            sec_aliases,
                            sec_claims,
                            sec_entity_type,
                        ) = self.get_wikidata_information(
                            wikidata_id=secondary_id, languages=languages, recurse=False
                        )

                        self.secondary_integrator_units[secondary_id] = IntegratorUnit(
                            labels=sec_labels,
                            descriptions=sec_descriptions,
                            aliases=sec_aliases,
                            entity_type=sec_entity_type,
                            claims=sec_claims,
                            wikidata_id=secondary_id,
                        )
                    # add target
                    if "id" in claims[secondary_id][0]["mainsnak"]["datavalue"]:
                        target_id = claims[secondary_id][0]["mainsnak"]["datavalue"][
                            "id"
                        ]
                        if target_id not in self.secondary_integrator_units:
                            (
                                tar_labels,
                                tar_descriptions,
                                tar_aliases,
                                tar_claims,
                                tar_entity_type,
                            ) = self.get_wikidata_information(
                                wikidata_id=target_id,
                                languages=languages,
                                recurse=False,
                            )
                            self.secondary_integrator_units[target_id] = IntegratorUnit(
                                labels=tar_labels,
                                descriptions=tar_descriptions,
                                aliases=tar_aliases,
                                entity_type=tar_entity_type,
                                claims=tar_claims,
                                wikidata_id=target_id,
                            )

            self.primary_integrator_units[item_id] = IntegratorUnit(
                labels=labels,
                descriptions=descriptions,
                aliases=aliases,
                entity_type=entity_type,
                claims=claims,
                wikidata_id=item_id,
            )

    def check_entity_exists(self, unit, wikidata_id):
        "Check if entity exists"
        print(unit.entity_type)
        print(vars(unit))
        print(unit.labels['en'])
        #--------------------------------------------
        language = 'en'
        params = {
        'action': 'wbsearchentities',
        'search': 'URL',
        'language': language,
        'type': 'property',
        'limit': 50,
        'format': 'json'
        }
        cont_count = 0
        results = []
        from wikibaseintegrator.wbi_helpers import mediawiki_api_call_helper
        search_results = mediawiki_api_call_helper(data=params, allow_anonymous=True)
        print(search_results)
        sys.exit("reproduce exit")

        #--------------------------------------------
        from wikibaseintegrator.wbi_helpers import search_entities
        test_dict = search_entities(
        #test_dict = self.wikibase_integrator.wbi_helpers.search_entities(
            #search_string=unit.labels['en'], language='en', search_type=unit.entity_type, dict_result=True
            search_string="URL", language="en", search_type="property"
         )
        print(test_dict)
        sys.exit("Exit: check entity exists")

    def import_items(self):
        """Import items in self.integrator units or update"""
        self.change_config(instance="local")
        print(type(self.wikibase_integrator))
        print(self.wikibase_integrator.login)
        test_login = self.change_login(instance="local")
        print(self.wikibase_integrator.login)
        print(type(self.wikibase_integrator.login))
        for wikidata_id, unit in self.secondary_integrator_units.items():

            self.check_entity_exists(unit=unit, wikidata_id=wikidata_id)
            if wikidata_id[0] == "Q":
                entity = self.wikibase_integrator.item.new()
            elif wikidata_id[0] == "P":
                entity = self.wikibase_integrator.property.new()
            print(unit.labels)
            for lang, val in unit.labels.items():
                entity.labels.set(language=lang, value=val)
            print("descriptions!")
            print(unit.descriptions)
            print("aliases!")
            print(unit.aliases)
            for lang, val in unit.descriptions.items():
                entity.descriptions.set(language=lang, value=val)
            for lang, val_list in unit.aliases.items():
                for val in val_list:
                    entity.aliases.set(language=lang, values=val)
            entity.datatype = "wikibase-property"
            print(entity.aliases)
            print(type(entity.datatype))
            # entity.datatype.set_value('P100')
            print(type(entity))
            print(entity.aliases)
            print(unit.labels)
            print(unit.descriptions)
            print(unit.aliases)

            #     ------------------------------------------------
            import ujson

            data = entity.get_json()
            print("!!!!!!!!!!!!!!!!!!!!!!!!")
            print(data)
            print(type(data))
            print(type(ujson.dumps(data)))

            payload = {
                "action": "wbeditentity",
                # "data": ujson.dumps(
                #     {
                #         "labels": {
                #             "en": {"language": "en", "value": "Testbla"},
                #             "de": {"language": "de", "value": "Testbla"},
                #         },
                #         "descriptions": {
                #             "en": {
                #                 "language": "en",
                #                 "value": 'fix "Category:")',
                #             },
                #             "de": {
                #                 "language": "de",
                #                 "value": "haltet (ohne das Präfix „Category:“)",
                #             },
                #         },
                #         "aliases": {
                #             "en": [
                #                 {"language": "en", "value": "test"},
                #                 {"language": "en", "value": "test2"},
                #             ]
                #         },
                #         # "aliases": {
                #         #     "en": [
                #         #         "commons",
                #         #         "category Commo",
                #         #         "category on Coms",
                #         #     ],
                #         #     "de": ["commonst"],
                #         # },
                #         "datatype": "wikibase-property",
                #     }
                # ),
                "data": ujson.dumps(data),
                "format": "json",
                "token": "+\\",
                #'badge': '1'
                #'assert': 'bot'
            }
            is_bot = self.wikibase_integrator.is_bot
            if is_bot:
                payload.update({"bot": ""})
            payload.update({"new": entity.type})
            login = self.wikibase_integrator.login

            # json_result: dict = mediawiki_api_call_helper(data=payload, login=login, allow_anonymous=allow_anonymous, is_bot=is_bot, **kwargs)

            from wikibaseintegrator.wbi_config import config

            mediawiki_api_url = config["MEDIAWIKI_API_URL"]
            user_agent = "WikibaseIntegrator/0.12.0"
            headers = {"User-Agent": user_agent}
            payload.update({"token": login.get_edit_token()})
            session = login.get_session()

            response = None
            import requests

            # session = requests.Session()
            from time import sleep
            import json

            for n in range(100):
                try:
                    response = session.request(
                        method="POST",
                        url=mediawiki_api_url,
                        data=payload,
                        headers=headers,
                    )
                except requests.exceptions.ConnectionError as e:
                    print("Connection error: %s. Sleeping for %d seconds.", e, 60)
                    sleep(60)
                    continue
                if response.status_code in (500, 502, 503, 504):
                    print(
                        "Service unavailable (HTTP Code %d). Sleeping for %d seconds.",
                        response.status_code,
                        60,
                    )
                    sleep(60)
                    continue
                break
            response.raise_for_status()
            print(response.content)
            print("????????????????????/")
            print(response.request.body)
            print(response.request.headers)
            print(mediawiki_api_url)
            json_data = response.json()
            print(json_data)
            sys.exit("Exited")
            # -----------------------------------------------------
            entity.write(login=test_login)
        # import each of the secondaries
        # import each of the primaries
        # after importing or updating, mark as known in list or sthj

    def get_wikidata_information(self, wikidata_id, languages, recurse):
        labels = {}
        descriptions = {}
        aliases = {}
        if wikidata_id[0] == "Q":
            entity = self.wikibase_integrator.item.get(entity_id=wikidata_id).get_json()
        elif wikidata_id[0] == "P":
            entity = self.wikibase_integrator.property.get(
                entity_id=wikidata_id
            ).get_json()
        for lang in languages:
            label_info = entity["labels"].get(lang)
            if label_info:
                labels[lang] = label_info["value"]
            description_info = entity["descriptions"].get(lang)
            if description_info:
                descriptions[lang] = description_info["value"]
            alias_info = entity["aliases"].get(lang)
            alias_value_list = []
            if alias_info:
                for alias_dict in alias_info:
                    alias_value_list.append(alias_dict["value"])
            aliases[lang] = alias_value_list

        if recurse == False:
            claims = None
        else:
            claims = entity["claims"]
        entity_type = entity["type"]
        return (labels, descriptions, aliases, claims, entity_type)

    def change_config(self, instance):
        if instance == "wikidata":
            wbi_config["MEDIAWIKI_API_URL"] = "https://www.wikidata.org/w/api.php"
            wbi_config["SPARQL_ENDPOINT_URL"] = "https://query.wikidata.org/sparql"
            wbi_config["WIKIBASE_URL"] = "http://www.wikidata.org"
        elif instance == "local":
            wbi_config["MEDIAWIKI_API_URL"] = self.config_dict["mediawiki_api_url"]
            wbi_config["SPARQL_ENDPOINT_URL"] = self.config_dict["sparql_endpoint_url"]
            wbi_config["WIKIBASE_URL"] = self.config_dict["wikibase_url"]
        else:
            sys.exit("Invalid instance")

    def change_login(self, instance):
        if instance == "wikidata":
            pass
        elif instance == "local":
            # from wikibaseintegrator import wbi_login
            login_instance = wbi_login.Clientlogin(
                user=os.environ.get("BOTUSER_NAME"),
                password=os.environ.get("BOTUSER_PW"),
            )
            self.wikibase_integrator.login = login_instance
        else:
            sys.exit("Invalid instance")
        return login_instance
