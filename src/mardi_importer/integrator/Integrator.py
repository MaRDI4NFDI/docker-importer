from mardi_importer.integrator.IntegratorUnit import IntegratorUnit
from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.datatypes import Item, String, MonolingualText, Time
from wikibaseintegrator import wbi_login
from wikibaseintegrator.wbi_helpers import search_entities
from wikibaseintegrator.wbi_enums import ActionIfExists
import os
import sqlalchemy as db

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
        # wikidata id to imported id
        self.id_mapping = {}
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"]
        db_host = os.environ["DB_HOST"]
        self.engine = db.create_engine(
            f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}"
        )

    def check_or_create_db_table(self):
        # check if db table is there
        # if not, create
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

    def insert_id_db(self, wikidata_id, internal_id, connection):
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", metadata, autoload=True, autoload_with=self.engine
        )

        ins = table.insert().values(wikidata_id=wikidata_id, internal_id=internal_id)
        result = connection.execute(ins)

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
                datatype,
            ) = self.get_wikidata_information(
                wikidata_id=item_id, languages=languages, recurse=recurse
            )

            if recurse == True:
                # for each of the ids in the claims, get stuff with recurse = False
                # and add to secondary
                for secondary_id in claims:
                    # add property
                    self.add_secondary_units(unit_id=secondary_id, languages=languages)

                    # add target
                    for relation in claims[secondary_id]:
                        if "id" in relation["mainsnak"]["datavalue"]["value"]:
                            target_id = relation["mainsnak"]["datavalue"]["value"]["id"]
                            self.add_secondary_units(
                                unit_id=target_id, languages=languages
                            )

                        if "references" in relation:
                            references = relation["references"][0]["snaks"]
                            for ref_id in references:
                                # add property name of reference
                                self.add_secondary_units(
                                    unit_id=ref_id, languages=languages
                                )
                                # for each target of this property in references, add item if it is an item
                                for ref_snak in references[ref_id]:
                                    if "id" in ref_snak["datavalue"][
                                        "value"
                                    ] and isinstance(
                                        ref_snak["datavalue"]["value"], dict
                                    ):
                                        self.add_secondary_units(
                                            unit_id=ref_snak["datavalue"]["value"][
                                                "id"
                                            ],
                                            languages=languages,
                                        )

            self.primary_integrator_units[item_id] = IntegratorUnit(
                labels=labels,
                descriptions=descriptions,
                aliases=aliases,
                entity_type=entity_type,
                claims=claims,
                wikidata_id=item_id,
                datatype=datatype,
            )

    def add_secondary_units(self, unit_id, languages):
        if unit_id not in self.secondary_integrator_units:
            (
                labels,
                descriptions,
                aliases,
                claims,
                entity_type,
                datatype,
            ) = self.get_wikidata_information(
                wikidata_id=unit_id, languages=languages, recurse=False
            )

            self.secondary_integrator_units[unit_id] = IntegratorUnit(
                labels=labels,
                descriptions=descriptions,
                aliases=aliases,
                entity_type=entity_type,
                claims=claims,
                wikidata_id=unit_id,
                datatype=datatype,
            )

    def check_entity_exists(self, unit, wikidata_id, connection):
        """Check if entity exists using wbsearchentity with label

        Args:
            unit: an IntegratorUnit
        Returns:
            bool: Entity already exists or not
        """
        # check if id is in id mapping
        if wikidata_id in self.id_mapping:
            return True

        # check if id in db
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", metadata, autoload=True, autoload_with=self.engine
        )
        sql = db.select([table.columns.internal_id]).where(
            table.columns.wikidata_id == wikidata_id,
        )
        db_result = connection.execute(sql).fetchone()
        if db_result:
            # add to id mapping, because it already exists
            self.id_mapping[wikidata_id] = db_result["internal_id"]
            return True

        # if unit is not in dict and not in db, try str search to see if it already exists in wiki
        result = search_entities(
            search_string=unit.labels["en"],
            language="en",
            search_type=unit.entity_type,
            dict_result=True,
        )
        if not result:
            return False
        else:
            for subdict in result:
                if subdict["label"] == unit.labels["en"]:
                    # for properties, the label is unique
                    if wikidata_id[0] == "P":
                        # insert into mapping
                        self.id_mapping[wikidata_id] = subdict["id"]
                        # insert into db
                        self.insert_id_db(wikidata_id, subdict["id"], connection)
                        return True
                    # an item is unqiue in combination (label, description)
                    elif wikidata_id[0] == "Q":
                        if subdict["description"] == unit.descriptions["en"]:
                            # insert into mapping
                            self.id_mapping[wikidata_id] = subdict["id"]
                            # insert into db
                            self.insert_id_db(wikidata_id, subdict["id"], connection)
                            return True
                    else:
                        sys.exit(
                            "Exception: wikidata id starts with letter other than Q or P"
                        )
            return False

    def import_items(self):
        """Import items in self.integrator units or update"""
        self.change_config(instance="local")
        test_login = self.change_login(instance="local")

        with self.engine.connect() as connection:
            for wikidata_id, unit in self.secondary_integrator_units.items():

                if self.check_entity_exists(
                    unit=unit, wikidata_id=wikidata_id, connection=connection
                ):
                    continue
                if wikidata_id[0] == "Q":
                    entity = self.wikibase_integrator.item.new()
                    # entity.datatype = "wikibase-item"
                elif wikidata_id[0] == "P":
                    entity = self.wikibase_integrator.property.new()
                    entity.datatype = unit.datatype
                for lang, val in unit.labels.items():
                    entity.labels.set(language=lang, value=val)
                for lang, val in unit.descriptions.items():
                    entity.descriptions.set(language=lang, value=val)
                for lang, val_list in unit.aliases.items():
                    for val in val_list:
                        entity.aliases.set(language=lang, values=val)

                try:
                    entity_description = entity.write(login=test_login)
                except:
                    print(wikidata_id)
                    if wikidata_id in self.id_mapping:
                        print(self.id_mapping[wikidata_id])
                    else:
                        print("Nope!")
                    print(unit.labels)
                    print(unit.descriptions)
                    print(unit.aliases)
                    print(unit.datatype)
                    self.engine.dispose()
                    sys.exit("Nope!!")
                # insert mapping into db
                self.insert_id_db(
                    wikidata_id=wikidata_id,
                    internal_id=entity_description.id,
                    connection=connection,
                )
                # insert mapping into dict
                self.id_mapping[wikidata_id] = entity_description.id

            for wikidata_id, unit in self.primary_integrator_units.items():
                # import primary like secondary
                if self.check_entity_exists(
                    unit=unit, wikidata_id=wikidata_id, connection=connection
                ):
                    # here, nothing needs to be inserted into id_mapping or db,
                    # as it is already there
                    # if the entity already exists, update
                    internal_id = self.id_mapping[wikidata_id]
                    if wikidata_id[0] == "Q":
                        entity = self.wikibase_integrator.item.get(
                            entity_id=internal_id
                        )
                    elif wikidata_id[0] == "P":
                        entity = self.wikibase_integrator.property.get(
                            entity_id=internal_id
                        )
                        entity.datatype = unit.datatype
                    for lang, val in unit.descriptions.items():
                        # descriptions are not replaced,
                        # new ones are only added if there was no old one
                        entity.descriptions.set(
                            language=lang,
                            value=val,
                            action_if_exists=ActionIfExists.KEEP,
                        )
                    # this should not create duplicates, but append non-existing aliases
                    for lang, val_list in unit.aliases.items():
                        for val in val_list:
                            entity.aliases.set(language=lang, values=val)
                    print("now befoire makingclaims")
                    claims = self.make_claims(unit=unit)
                    # add new claims if they are different from old claims
                    print("now before appending claims")
                    entity.claims.add(
                        claims,
                        ActionIfExists.APPEND_OR_REPLACE,
                    )
                    print("now after appending claims")
                    entity_description = entity.write(login=test_login)

                else:
                    # add new entity
                    if wikidata_id[0] == "Q":
                        entity = self.wikibase_integrator.item.new()
                    elif wikidata_id[0] == "P":
                        entity = self.wikibase_integrator.property.new()
                    for lang, val in unit.labels.items():
                        entity.labels.set(language=lang, value=val)
                    for lang, val in unit.descriptions.items():
                        entity.descriptions.set(language=lang, value=val)
                    for lang, val_list in unit.aliases.items():
                        for val in val_list:
                            entity.aliases.set(language=lang, values=val)
                    claims = self.make_claims(unit=unit)
                    entity.claims.add(claims)

                    try:
                        entity_description = entity.write(login=test_login)
                    except Exception as e:
                        print(e)
                        print(wikidata_id)
                        print(unit.labels)
                        print(unit.descriptions)
                        print(unit.aliases)
                        print(claims)
                        sys.exit("ewehfoi")
                    # insert mapping into db
                    self.insert_id_db(
                        wikidata_id=wikidata_id,
                        internal_id=entity_description.id,
                        connection=connection,
                    )
                    self.id_mapping[wikidata_id] = entity_description.id

    def make_claims(self, unit):
        print("MAKING CLAIMS!!")
        claims = []
        for prop_nr, info in unit.claims.items():
            for relation in info:
                print(relation)
                if "datavalue" in relation["mainsnak"]:
                    # add references
                    ref_list = []
                    if "references" in relation:
                        references = relation["references"][0]["snaks"]
                        for ref_id in references:
                            for ref_snak in references[ref_id]:
                                ref_list.append(
                                    self.get_target(
                                        ref_snak["datavalue"], prop_nr=ref_id
                                    )
                                )

                    target = self.get_target(
                        relation["mainsnak"]["datavalue"],
                        prop_nr=prop_nr,
                        references=list(ref_list),
                    )
                    claims.append(target)
        return claims

    def get_target(self, data_value, prop_nr, references=None):
        if data_value["type"] == "string":
            return String(
                value=data_value["value"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "wikibase-entityid":
            return Item(
                value=self.id_mapping[data_value["value"]["id"]],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "monolingualtext":
            return MonolingualText(
                text=data_value["value"]["text"],
                language=data_value["value"]["language"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "time":
            return Time(
                time=data_value["value"]["time"],
                timezone=data_value["value"]["timezone"],
                before=data_value["value"]["before"],
                after=data_value["value"]["after"],
                precision=data_value["value"]["precision"],
                calendarmodel=data_value["value"]["calendarmodel"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        else:
            print(data_value)
            sys.exit("Unknown data value type")

    def get_wikidata_information(self, wikidata_id, languages, recurse):
        labels = {}
        descriptions = {}
        aliases = {}
        if wikidata_id[0] == "Q":
            entity = self.wikibase_integrator.item.get(entity_id=wikidata_id).get_json()
            datatype = None
        elif wikidata_id[0] == "P":
            entity = self.wikibase_integrator.property.get(
                entity_id=wikidata_id
            ).get_json()
            datatype = entity["datatype"]
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
        return (labels, descriptions, aliases, claims, entity_type, datatype)

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
