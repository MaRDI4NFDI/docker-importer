from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.models.qualifiers import Qualifiers
from wikibaseintegrator.models.claims import Claim
from wikibaseintegrator.models.references import Reference
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.datatypes import (
    Item,
    Property,
    String,
    MonolingualText,
    Time,
    CommonsMedia,
    ExternalID,
    Form,
    GeoShape,
    GlobeCoordinate,
    Lexeme,
    Math,
    MusicalNotation,
    Quantity,
    Sense,
    TabularData,
    URL,
)
from wikibaseintegrator import wbi_login
from wikibaseintegrator.wbi_helpers import search_entities
from wikibaseintegrator.wbi_enums import ActionIfExists
import os
import sqlalchemy as db
import datetime

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
        # list of wikidata ids that do not have the required language and
        # should therefore not be imported
        self.invalid_wikidata_ids = []
        self.wikibase_integrator = WikibaseIntegrator()
        self.imported_items = []
        config_parser = IntegratorConfigParser(conf_path)
        self.config_dict = config_parser.parse_config()
        self.wikidata_linker_id = None
        # wikidata id to imported id
        self.id_mapping = {}
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"]
        db_host = os.environ["DB_HOST"]
        self.engine = db.create_engine(
            f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}"
        )
        self.connection = self.engine.connect()

    def check_or_create_db_table(self):
        """
        Check if db table for id mapping is there; if not, create.

        Args:
            None

        Returns:
            None
        """
        with self.engine.connect() as connection:
            metadata = db.MetaData()
            if not db.inspect(self.engine).has_table("wb_id_mapping"):
                mapping_table = db.Table(
                    "wb_id_mapping",
                    metadata,
                    db.Column("id", db.Integer, primary_key=True),
                    db.Column("wikidata_id", db.String(24), nullable=False),
                    db.Column("internal_id", db.String(24), nullable=False),
                    db.UniqueConstraint("wikidata_id"),
                    db.UniqueConstraint("internal_id"),
                )
                metadata.create_all(self.engine)

    def insert_id_in_db(self, wikidata_id, internal_id, connection):
        """
        Insert wikidata_id and internal_id into mapping table.

        Args:
            wikidata_id: Wikidata id
            internal_id: local wiki id
            connection: sqlalchemy connection object

        Returns:
            None
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", metadata, autoload=True, autoload_with=self.engine
        )

        ins = table.insert().values(wikidata_id=wikidata_id, internal_id=internal_id)
        result = connection.execute(ins)

    def test_import(self, id_list, languages, recurse):
        # needs to be before change_config, because it gets
        # change to local in add_local_entity
        # self.wikidata_linker_id = self.add_local_entity(
        #     entity_type="property",
        #     labels={"en": "has_wikidata_id"},
        #     descriptions={"en": "has a wikidata id"},
        #     datatype="external-id",
        # )
        self.change_config(instance="local")
        login = self.change_login(instance="local")
        for wikidata_id in id_list:
            print(f"current wikidata entity id: {wikidata_id}")
            entity = self.get_wikidata_information(
                wikidata_id=wikidata_id, languages=languages, recurse=recurse
            )
            if not entity:
                print(f"No labels for entity with id {wikidata_id}, skipping")
                continue
            if recurse == True:
                self.test_convert_claim_ids(entity, languages, login)
            # if it is not there yet
            if not self.test_check_entity_exists(entity, wikidata_id, self.connection):
                self.change_config(instance="local")
                login = self.change_login(instance="local")
                new_id = entity.write(login=login, as_new=True).id

                self.id_mapping[wikidata_id] = new_id
                try:
                    self.insert_id_in_db(wikidata_id, new_id, self.connection)
                except:
                    print("both wikidata id and new id should be ids, not items")
                    print("first wikidata, then new")
                    print(wikidata_id)
                    print(new_id)
                    sys.exit("sql exit")

            # if it is there
            else:
                self.change_config(instance="local")
                if wikidata_id[0] == "Q":
                    local_entity = self.wikibase_integrator.item.get(
                        entity_id=self.id_mapping[wikidata_id]
                    )
                elif wikidata_id[0] == "P":
                    local_entity = self.wikibase_integrator.property.get(
                        entity_id=self.id_mapping[wikidata_id]
                    )
                # replace descriptions
                try:
                    local_entity.descriptions = entity.description
                except:
                    pass
                    # local_entity.descriptions=None
                # replace aliases["mainsnak"]ases
                # add new claims if they are different from old claims
                local_entity.claims.add(
                    entity.claims,
                    ActionIfExists.APPEND_OR_REPLACE,
                )
                self.change_config(instance="local")
                login = self.change_login(instance="local")
                # try:
                local_entity.write(login=login)
                # except Exception as e:
                #   print(local_entity)
                #     print(recurse)
                #     # print where claim id is P25
                #     print(local_entity.claims.get("P40"))
                #   sys.exit("testing")

        # todo:
        # if there are no labels for the desired language, rempove this value from claims
        sys.exit("Test exit")

    def write_claim_entities(self, wikidata_id, languages, login):
        entity = self.get_wikidata_information(
            wikidata_id=wikidata_id, languages=languages, recurse=False
        )
        if not entity:
            return None
        if not self.test_check_entity_exists(entity, wikidata_id, self.connection):
            local_id = entity.write(login=login, as_new=True).id
            self.id_mapping[wikidata_id] = local_id
            self.insert_id_in_db(wikidata_id, local_id, self.connection)
            # also add to db just like normal ones
            return local_id
        else:
            # if it does exist, only update --> get sth with this id with get;
            # do not insert anywhere, that already gets done when checking
            return self.id_mapping[wikidata_id]

    def get_wikidata_information(self, wikidata_id, languages, recurse):
        self.change_config(instance="wikidata")
        if wikidata_id[0] == "Q":
            entity = self.wikibase_integrator.item.get(entity_id=wikidata_id)
        elif wikidata_id[0] == "P":
            entity = self.wikibase_integrator.property.get(entity_id=wikidata_id)
        else:
            raise Exception(
                f"Wrong ID format, should start with P or Q but ID is {wikidata_id}"
            )
        if not languages == "all":
            # set labels in desired languages
            label_dict = {
                k: entity.labels.values[k]
                for k in languages
                if k in entity.labels.values
            }
            if not label_dict:
                return None
            entity.labels.values = label_dict
            # set descriptions in desired languages
            description_dict = {
                k: entity.descriptions.values[k]
                for k in languages
                if k in entity.descriptions.values
            }
            entity.descriptions.values = description_dict
            # set aliases in desired languages
            alias_dict = {
                k: entity.aliases.aliases[k]
                for k in languages
                if k in entity.aliases.aliases
            }
            entity.aliases.aliases = alias_dict
        # set claims to None if recurse = False, else just leave them as is
        if recurse == False:
            from wikibaseintegrator.models.claims import Claims

            entity.claims = Claims()

        # necessary because otherwise, this can't be written to local instance
        # entity.id = None

        return entity

    def test_convert_claim_ids(self, entity, languages, login):
        entity_names = [
            "wikibase-item",
            "wikibase-property",
            "wikibase-lexeme",
        ]
        import copy

        claims = copy.deepcopy(entity.claims.claims)
        # todo: completely replace entity.claims.claims
        # --> make a new dict with new_propid : new claim_list
        new_claims = {}
        # each property can have several targets
        for prop_id, claim_list in claims.items():
            local_claim_list = []
            local_prop_id = self.write_claim_entities(
                wikidata_id=prop_id, languages=languages, login=login
            )
            if not local_prop_id:
                print("local id skipped")
                continue
            # get target id if it is an entity
            for c in claim_list:
                c_dict = c.get_json()
                if c_dict["mainsnak"]["datatype"] in entity_names:
                    if c_dict["mainsnak"]["datavalue"]:
                        local_mainsnak_id = self.write_claim_entities(
                            wikidata_id=c_dict["mainsnak"]["datavalue"]["value"]["id"],
                            languages=languages,
                            login=login,
                        )
                        if not local_mainsnak_id:
                            continue
                        self.check_value_links(
                            snak=c_dict["mainsnak"], languages=languages, login=login
                        )
                        # c_dict.pop("id")
                        # c_dict["id"] = None
                        c_dict["mainsnak"]["datavalue"]["value"][
                            "id"
                        ] = local_mainsnak_id
                        c_dict["mainsnak"]["datavalue"]["value"]["numeric-id"] = int(
                            local_mainsnak_id[1:]
                        )
                        c_dict["mainsnak"]["property"] = local_prop_id
                        # to avoid problem with missing reference hash
                        if "references" in c_dict:
                            c_dict.pop("references")
                        new_c = Claim().from_json(c_dict)
                        # try:
                        #     new_c = Claim().from_json(c_dict)
                        # except:
                        #     print(prop_id)
                        #     print(c_dict)
                        #     print(list(c_dict))
                        #     print(c_dict["references"])
                        #     sys.exit("testing")
                        new_c.id = None
                    else:
                        continue
                else:
                    new_c = c
                    new_c.mainsnak.property_number = local_prop_id
                    new_c.id = None
                # get reference details
                ref_list = c.references.references
                if ref_list:
                    new_ref_list = []
                    for ref in ref_list:
                        # snaks_order = []
                        new_ref_snak_dict = {}  # this??
                        ref_snak_dict = ref.get_json()
                        for ref_prop_id, ref_snak_list in ref_snak_dict[
                            "snaks"
                        ].items():
                            new_ref_prop_id = self.write_claim_entities(
                                wikidata_id=ref_prop_id,
                                languages=languages,
                                login=login,
                            )
                            if not new_ref_prop_id:
                                continue
                            # snaks_order.append(new_ref_prop_id)
                            new_ref_snak_list = []
                            for ref_snak in ref_snak_list:
                                if ref_snak["datatype"] in entity_names:
                                    new_ref_snak_id = self.write_claim_entities(
                                        wikidata_id=ref_snak["datavalue"]["value"][
                                            "id"
                                        ],
                                        languages=languages,
                                        login=login,
                                    )
                                    if not new_ref_snak_id:
                                        continue
                                    self.check_value_links(
                                        snak=ref_snak,
                                        languages=languages,
                                        login=login,
                                    )
                                    ref_snak["datavalue"]["value"][
                                        "id"
                                    ] = new_ref_snak_id
                                    ref_snak["datavalue"]["value"]["numeric-id"] = int(
                                        new_ref_snak_id[1:]
                                    )
                                ref_snak["property"] = new_ref_prop_id
                                new_ref_snak_list.append(ref_snak)
                            new_ref_snak_dict[new_ref_prop_id] = new_ref_snak_list
                            # if ref_prop_id == "P4656":
                            #     print(new_ref_snak_dict)
                            #     sys.exit("teststing")
                        complete_new_ref_snak_dict = {}
                        complete_new_ref_snak_dict["hash"] = None
                        complete_new_ref_snak_dict["snaks"] = new_ref_snak_dict
                        complete_new_ref_snak_dict["snaks-order"] = []
                        r = Reference()
                        # print(r.from_json(json_data=complete_new_ref_snak_dict))
                        # sys.exit("testing")
                        try:
                            new_ref_list.append(
                                r.from_json(json_data=complete_new_ref_snak_dict)
                            )
                        except Exception as e:
                            print(e)
                            print(complete_new_ref_snak_dict)
                            sys.exit("ref from json failed exot")

                    new_c.references.references = new_ref_list
                # new_c.references.references = Reference()  # delete
                # get qualifier details
                qual_dict = c.qualifiers.get_json()
                new_qual_dict = {}
                test_check = False
                for qual_id, qual_list in qual_dict.items():
                    new_qual_id = self.write_claim_entities(
                        wikidata_id=qual_id, languages=languages, login=login
                    )
                    if new_qual_id == "P43":
                        test_check = True
                        print("first found!")
                        print(qual_list)
                    else:
                        print(f"Nope! This is qwual id {new_qual_id}")
                    if not new_qual_id:
                        continue
                    new_qual_list = []
                    for qual_val in qual_list:
                        if qual_val["datatype"] in entity_names:
                            new_qual_val_id = self.write_claim_entities(
                                wikidata_id=qual_val["datavalue"]["value"]["id"],
                                languages=languages,
                                login=login,
                            )
                            if not new_qual_val_id:
                                continue
                            self.check_value_links(
                                snak=qual_val,
                                languages=languages,
                                login=login,
                            )
                            qual_val["datavalue"]["value"]["id"] = new_qual_val_id
                            qual_val["datavalue"]["value"]["numeric-id"] = int(
                                new_qual_val_id[1:]
                            )
                        qual_val["property"] = new_qual_id
                        self.check_value_links(
                            snak=qual_val,
                            languages=languages,
                            login=login,
                        )
                        if new_qual_id == "P43":
                            print("qual val")
                            print(qual_val)
                            print("qual_list")
                            print(qual_list)
                            # sys.exit("foundz it")
                        new_qual_list.append(qual_val)
                    new_qual_dict[new_qual_id] = new_qual_list
                q = Qualifiers()
                new_c.qualifiers = q.from_json(json_data=new_qual_dict)
                if test_check:
                    print(new_c.qualifiers)
                    # sys.exit("found it!")
                local_claim_list.append(new_c)
            new_claims[local_prop_id] = local_claim_list
        entity.claims.claims = new_claims

    def test_check_entity_exists(self, entity, wikidata_id, connection):
        """Check if entity exists with a lookup (in this order) in
        self.id_mapping, db table and wiki. Add to where it is missing,
        if it only exists in some of them.

        Args:
           unit: an IntegratorUnit
           wikidata_id
           connection: sqlalchemy connection
        Returns:
           bool: Entity already exists or not
        """
        # if the id is in id mapping, the entity
        # has been created in this run
        self.change_config(instance="local")
        if wikidata_id in self.id_mapping:
            print(self.id_mapping[wikidata_id])
            return True
        else:
            print("not in id mapping")

        # check if entity is in db
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", metadata, autoload=True, autoload_with=self.engine
        )
        sql = db.select([table.columns.internal_id]).where(
            table.columns.wikidata_id == wikidata_id,
        )
        db_result = connection.execute(sql).fetchone()
        # if it is in db, it already exists
        # and should be added to the db mapping
        # to speed up the lookup
        if db_result:
            print(" db result")
            print(db_result)
            self.id_mapping[wikidata_id] = db_result["internal_id"]
            return True

        # if unit is not in dict and not in db, try string search
        # to see if it already exists in wiki
        try:
            result = search_entities(
                search_string=str(entity.labels.get("en")),
                language="en",
                search_type=entity.type,
                dict_result=True,
            )
        except:
            test = str(entity.labels.get("en"))
            print(test)
            print(type(test))
            sys.exit("result exit")
        # if is in neither of the three, it does not exist
        if not result:
            return False
        else:
            print(" result")
            print(result)
            # try to find an instance where label (for properties)
            # or label and description (for items) match the
            # entity information
            for subdict in result:
                if subdict["label"] == entity.labels.get("en"):
                    # for properties, the label is unique
                    if wikidata_id[0] == "P":
                        self.id_mapping[wikidata_id] = subdict["id"]
                        self.insert_id_in_db(wikidata_id, subdict["id"], connection)
                        return True
                    # an item is unique in combination (label, description)
                    elif wikidata_id[0] == "Q":
                        # if, additionally to label, the description also matches
                        if subdict["description"] == entity.descriptions.get("en"):
                            self.id_mapping[wikidata_id] = subdict["id"]
                            self.insert_id_in_db(wikidata_id, subdict["id"], connection)
                            return True
                    else:
                        sys.exit(
                            "Exception: wikidata id starts with letter other than Q or P"
                        )
            # if no entity was found where
            # the required params match, it does
            # not exist
            return False

    def debug_write(self, entity):
        self.change_config(instance="local")
        login = self.change_login(instance="local")
        import ujson

        data = entity.get_json()
        print("!!!!!!!!!!!!!!!!!!!!!!!!")
        print(data)
        print(type(data))
        print(type(ujson.dumps(data)))

        payload = {
            "action": "wbeditentity",
            "data": ujson.dumps(data),
            "format": "json",
            "token": "+\\",
        }
        is_bot = self.wikibase_integrator.is_bot
        if is_bot:
            payload.update({"bot": ""})
        payload.update({"new": entity.type})
        login = self.wikibase_integrator.login

        from wikibaseintegrator.wbi_config import config

        mediawiki_api_url = config["MEDIAWIKI_API_URL"]
        user_agent = "WikibaseIntegrator/0.12.0"
        headers = {"User-Agent": user_agent}
        payload.update({"token": login.get_edit_token()})
        session = login.get_session()

        response = None
        import requests

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

    def create_id_list_from_file(self, file):
        id_list = []
        with open(file, "r") as file:
            for line in file:
                id_list.append(line.strip())
        return id_list

    def check_value_links(self, snak, languages, login):
        self.change_config("local")
        # print(wbi_config["WIKIBASE_URL"])
        if "datatype" in snak:
            data = snak["datavalue"]["value"]
            if snak["datatype"] == "quantity":
                # print("snak!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
                # print(snak)
                # sys.exit("taesting")
                if "unit" in data:
                    unit_string = data["unit"]
                    if "www.wikidata.org/" in unit_string:
                        uid = unit_string.split("/")[-1]
                        local_id = self.write_claim_entities(
                            wikidata_id=uid,
                            languages=languages,
                            login=login,
                        )
                        print(wbi_config["WIKIBASE_URL"])
                        print(wbi_config["WIKIBASE_URL"] + "/entity/" + local_id)
                        data["unit"] = wbi_config["WIKIBASE_URL"] + "entity/" + local_id
                # print(snak)
                # sys.exit("unit snak testingh")
            elif snak["datatype"] == "globecoordinate":
                # print("snak!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
                # print(snak)
                if "globe" in data:
                    globe_string = data["globe"]
                    if "www.wikidata.org/" in globe_string:
                        uid = globe_string.split("/")[-1]
                        local_id = self.write_claim_entities(
                            wikidata_id=uid,
                            languages=languages,
                            login=login,
                        )
                        data["value"]["globe"] = (
                            wbi_config["WIKIBASE_URL"] + "entity/" + local_id
                        )
                # print(snak)
                # print("globe snak testing")
                # sys.exit("globe")

    def change_config(self, instance):
        """
        Function for changing the config to allow using wikidata and local instance.
        Also set user agent to avoid warning.

        Args:
            instance: Instance name (choice between wikidata and local)

        Returns:
            None
        """
        wbi_config["USER_AGENT"] = "zuse_wlocalikibase_importer"
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
        """
        Function for using in as a botuser; needed for editing local data.

        Args:
            instance: Instance name (choice between wikidata and local)

        Returns:
            wbi login instance
        """
        if instance == "wikidata":
            pass
        elif instance == "local":
            login_instance = wbi_login.Clientlogin(
                user=os.environ.get("BOTUSER_NAME"),
                password=os.environ.get("BOTUSER_PW"),
            )
            self.wikibase_integrator.login = login_instance
        else:
            sys.exit("Invalid instance")
        return login_instance
