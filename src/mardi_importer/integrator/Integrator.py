from mardi_importer.integrator.IntegratorUnit import IntegratorUnit
from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
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

    def create_units(self, id_list, languages, recurse):
        """Function for creating units. Primary units, which are units
        referenced in id_list, are created together with units for the properties
        and property targets in the claims, as well as properties and targets in
        the claim references, if given.

        Args:
            id_list: List of wikidata ids to be imported
            languages: list of desired languages
            recurse: if we want to import claims

        Returns:
            None (all units are added to self.primary_integrator_units
                and self.secondary_integrator_units)
        """
        # needs to be before change_config, because it gets
        # change to local in add_local_entity
        self.wikidata_linker_id = self.add_local_entity(
            entity_type="property",
            labels={"en": "has_wikidata_id"},
            descriptions={"en": "has a wikidata id"},
            datatype="external-id",
        )
        self.change_config(instance="wikidata")
        # for each of the primary items in id_list,
        # add it and all it links to

        for item_id in id_list:
            print(f"Creating entity {item_id}")
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
            # if label does not exist
            # in desired languages,or not in eglish, do not
            # add entity
            if not labels or "en" not in labels:
                self.invalid_wikidata_ids.append(item_id)
                continue
            # if we also want to import the claims
            if recurse == True:
                # add secondary units, where secondary units are the properties
                for secondary_id in claims:
                    # add property
                    self.add_secondary_units(unit_id=secondary_id, languages=languages)

                    for relation in claims[secondary_id]:
                        # exclude edge cases
                        if relation["mainsnak"]["snaktype"] in ["somevalue", "novalue"]:
                            continue
                        value = relation["mainsnak"]["datavalue"]["value"]

                        if "id" in value and isinstance(value, dict):
                            target_id = value["id"]
                            # add property target if it is an entity
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
                                # for each target of this property in references,
                                # add item if it is an item
                                for ref_snak in references[ref_id]:
                                    ref_value = ref_snak["datavalue"]["value"]
                                    if "id" in ref_value and isinstance(
                                        ref_value, dict
                                    ):
                                        # add target of property in references
                                        self.add_secondary_units(
                                            unit_id=ref_value["id"],
                                            languages=languages,
                                        )

            # add primary unit
            self.primary_integrator_units[item_id] = IntegratorUnit(
                labels=labels,
                descriptions=descriptions,
                aliases=aliases,
                entity_type=entity_type,
                claims=claims,
                wikidata_id=item_id,
                datatype=datatype,
            )

    def create_id_list_from_file(self, file):
        id_list = []
        with open(file, "r") as file:
            for line in file:
                id_list.append(line.strip())
        return id_list

    def add_secondary_units(self, unit_id, languages):
        """Function for creating secondary units, where a
        secondary unit is an entity unit that does not contain claims.

        Args:
            unit_id: wikidata id
            languages: desired languages to be pulled

        Returns:
            None (created units are added to self.secondary_integrator_units)
        """
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
            # if labels are not available in
            # desired languages, or not in english, unit should not
            # be added
            if not labels or "en" not in labels:
                self.invalid_wikidata_ids.append(unit_id)
                return
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
        if wikidata_id in self.id_mapping:
            return True

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
            self.id_mapping[wikidata_id] = db_result["internal_id"]
            return True

        # if unit is not in dict and not in db, try string search
        # to see if it already exists in wiki
        result = search_entities(
            search_string=unit.labels["en"],
            language="en",
            search_type=unit.entity_type,
            dict_result=True,
        )
        # if is in neither of the three, it does not exist
        if not result:
            return False
        else:
            # try to find an instance where label (for properties)
            # or label and description (for items) match the
            # entity information
            for subdict in result:
                if subdict["label"] == unit.labels["en"]:
                    # for properties, the label is unique
                    if wikidata_id[0] == "P":
                        self.id_mapping[wikidata_id] = subdict["id"]
                        self.insert_id_in_db(wikidata_id, subdict["id"], connection)
                        return True
                    # an item is unique in combination (label, description)
                    elif wikidata_id[0] == "Q":
                        # if, additionally to label, the description also matches
                        if subdict["description"] == unit.descriptions["en"]:
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

    def import_items(self):
        """Function for importing or updating entities in local
        wikibase instance. Import primary and secondary units.
        """
        self.change_config(instance="local")
        login = self.change_login(instance="local")

        with self.engine.connect() as connection:
            # add secondary units first so they are available for linking in the first
            # units and claims
            for wikidata_id, unit in self.secondary_integrator_units.items():
                print(f"Importing entity {wikidata_id}")
                # if the secondary unit already exists, there is no reason to
                # create it
                if self.check_entity_exists(
                    unit=unit, wikidata_id=wikidata_id, connection=connection
                ):
                    continue
                entity = self.make_entity(wikidata_id, unit)

                entity_description = entity.write(login=login)

                self.insert_id_in_db(
                    wikidata_id=wikidata_id,
                    internal_id=entity_description.id,
                    connection=connection,
                )
                self.id_mapping[wikidata_id] = entity_description.id

            for wikidata_id, unit in self.primary_integrator_units.items():
                print(f"Importing entity {wikidata_id}")
                if self.check_entity_exists(
                    unit=unit, wikidata_id=wikidata_id, connection=connection
                ):
                    # if the entity already exists, update
                    # does not need to be added to db or mapping, as this
                    # already happens in the check_entity_exists function
                    internal_id = self.id_mapping[wikidata_id]
                    entity = self.make_entity(wikidata_id, unit, internal_id)
                    entity_description = entity.write(login=login)

                else:
                    # add new entity
                    entity = self.make_entity(wikidata_id, unit, make_claims=True)
                    entity_description = entity.write(login=login)
                    # insert mapping into db
                    self.insert_id_in_db(
                        wikidata_id=wikidata_id,
                        internal_id=entity_description.id,
                        connection=connection,
                    )
                    self.id_mapping[wikidata_id] = entity_description.id

    def make_entity(self, wikidata_id, unit, internal_id=None, make_claims=False):
        """Function for creating new entity
        of type property or item. Also add labels, descriptions,
        aliases and claims, where appropriate.

        Args:
            wikidata_id
            unit: integrator unit; needed for datatype
            internal_id: optional; if given, get entity instead of creating
            make claims: bool, decides if claims are made when entity is
                    created; if it is updated, they are always made

        Returns:
            entity
        """

        wikidata_id_claim = ExternalID(
            value=wikidata_id,
            prop_nr=self.wikidata_linker_id,
            references=[[]],
        )

        if internal_id:
            if wikidata_id[0] == "Q":
                entity = self.wikibase_integrator.item.get(entity_id=internal_id)
            elif wikidata_id[0] == "P":
                entity = self.wikibase_integrator.property.get(entity_id=internal_id)
                # each property needs a dataype
                entity.datatype = unit.datatype
            for lang, val in unit.descriptions.items():
                # descriptions are not replaced,
                # new ones are only added if there was no old one
                entity.descriptions.set(
                    language=lang,
                    value=val,
                    action_if_exists=ActionIfExists.KEEP,
                )
            # if only updating, claims are always made
            claims = self.make_claims(unit=unit)
            claims.append(wikidata_id_claim)
            # add new claims if they are different from old claims
            entity.claims.add(
                claims,
                ActionIfExists.APPEND_OR_REPLACE,
            )
        else:
            if wikidata_id[0] == "Q":
                entity = self.wikibase_integrator.item.new()
            elif wikidata_id[0] == "P":
                entity = self.wikibase_integrator.property.new()
                # each property needs a datatype
                entity.datatype = unit.datatype
            for lang, val in unit.labels.items():
                entity.labels.set(language=lang, value=val)
            for lang, val in unit.descriptions.items():
                entity.descriptions.set(language=lang, value=val)
            if make_claims:
                claims = self.make_claims(unit=unit)
                claims.append(wikidata_id_claim)
                entity.claims.add(claims)
            else:
                entity.claims.add([wikidata_id_claim])
        # this will not create duplicates, even if entity already exists
        for lang, val_list in unit.aliases.items():
            for val in val_list:
                entity.aliases.set(language=lang, values=val)

        return entity

    def make_claims(self, unit):
        """Function for making claims, including their references.

        Args:
            unit: unit for which claims should be made

        Returns:
            claims
        """
        claims = []
        for prop_nr, info in unit.claims.items():
            if prop_nr in self.invalid_wikidata_ids:
                continue
            # there can be several targets for one property
            for relation in info:
                if "datavalue" in relation["mainsnak"]:
                    ref_list = []
                    # if there are references for the claim
                    if "references" in relation:
                        references = relation["references"][0]["snaks"]
                        for ref_id in references:
                            if ref_id in self.invalid_wikidata_ids:
                                continue
                            # add reference property targets
                            for ref_snak in references[ref_id]:
                                # if it is an entity and the id is in the invalid ids, skip
                                if ref_snak["datavalue"]["type"] == "wikibase-entityid":
                                    if (
                                        ref_snak["datavalue"]["value"]["id"]
                                        in self.invalid_wikidata_ids
                                    ):
                                        continue
                                target = self.get_target(
                                    ref_snak["datavalue"], prop_nr=ref_id
                                )
                                ref_list.append(target)
                    # add claim property targets
                    # if it is an entity and the id is in the invalid ids, skip
                    if relation["mainsnak"]["datavalue"]["type"] == "wikibase-entityid":
                        if (
                            relation["mainsnak"]["datavalue"]["value"]["id"]
                            in self.invalid_wikidata_ids
                        ):
                            continue
                    target = self.get_target(
                        relation["mainsnak"]["datavalue"],
                        prop_nr=prop_nr,
                        references=list(ref_list),
                    )
                    claims.append(target)
        return claims

    def get_target(self, data_value, prop_nr, references=None):
        """Function for returning a property target of the type
        String, Item, MonolingualText, Time, CommonsMedia, ExternalID,
        Form, GeoSHape, GlobeCoordinate, Lexeme, Math, MusicalNotation,
        Quantity, Sense, TabularData or URL.

        Args:
            data_value: datavalue dict from wikidata for the target
            prop_nr: property id
            references: references for claim

        Returns:
            object of one of the above mentioned types
        """
        value_dict = data_value["value"]
        if data_value["type"] == "string":
            return String(
                value=data_value["value"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "wikibase-entityid":
            internal_id = self.id_mapping[data_value["value"]["id"]]
            if internal_id[0] == "P":
                return Property(
                    value=internal_id,
                    prop_nr=self.id_mapping[prop_nr],
                    references=references,
                )
            else:
                return Item(
                    value=internal_id,
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
                time=self.get_value(keyword="time", value_dict=value_dict),
                timezone=self.get_value(keyword="timezone", value_dict=value_dict),
                before=self.get_value(keyword="before", value_dict=value_dict),
                after=self.get_value(keyword="after", value_dict=value_dict),
                precision=self.get_value(keyword="precision", value_dict=value_dict),
                calendarmodel=self.get_value(
                    keyword="calendarmodel", value_dict=value_dict
                ),
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "commonsmedia":
            return CommonsMedia(
                value=data_value["value"]["commonsmedia"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "external-id":
            return ExternalID(
                value=data_value["value"]["external-id"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "wikibase-form":
            return Form(
                value=data_value["value"]["wikibase-form"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "geo-shape":
            return GeoShape(
                value=data_value["value"]["geo-shape"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "globecoordinate":
            return GlobeCoordinate(
                latitude=self.get_value(keyword="latitude", value_dict=value_dict),
                longitude=self.get_value(keyword="longitude", value_dict=value_dict),
                altitude=self.get_value(keyword="altitude", value_dict=value_dict),
                precision=self.get_value(keyword="precision", value_dict=value_dict),
                globe=self.get_value(keyword="globe", value_dict=value_dict),
                wikibase_url=self.get_value(
                    keyword="wikibase_url", value_dict=value_dict
                ),
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "wikibase-lexeme":
            return Lexeme(
                value=data_value["value"]["wikibase-lexeme"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "math":
            return Math(
                value=data_value["value"]["math"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "musical-notation":
            return MusicalNotation(
                value=data_value["value"]["musical notation"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "quantity":
            return Quantity(
                amount=self.get_value(keyword="amount", value_dict=value_dict),
                upper_bound=self.get_value(
                    keyword="upper_bound", value_dict=value_dict
                ),
                lower_bound=self.get_value(
                    keyword="lower_bound", value_dict=value_dict
                ),
                unit=self.get_value(keyword="unit", value_dict=value_dict),
                wikibase_url=self.get_value(
                    keyword="wikibase_url", value_dict=value_dict
                ),
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "wikibase-sense":
            return Sense(
                value=data_value["value"]["wikibase-sense"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "tabular-data":
            return TabularData(
                value=data_value["value"]["tabular-data"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )
        elif data_value["type"] == "url":
            return URL(
                value=data_value["value"]["url"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
            )

        else:
            print(data_value)
            sys.exit("Unknown data value type")

    def get_value(self, keyword, value_dict):
        """Function for getting a keyword from a
            value dict, if it exists, else None

        Args:
            keyword
            value_dict

        Returns:
            value or None
        """
        if keyword in value_dict:
            if keyword == "time":
                timeformat = "+%Y-%m-%dT00:00:00Z"
                time = value_dict["time"]
                try:
                    datetime.datetime.strptime(time, timeformat)
                except:
                    # some timestamps are invalid (e.g. +2007-00-00T00:00:00Z)
                    # and have to be set to a valid value --> 01 instead of 00
                    time = time.split("-")
                    if time[1] == "00":
                        time[1] = "01"
                    if time[2].startswith("00"):
                        time[2] = "01" + time[2][2:]
                    time = ("-").join(time)
                return time
            return value_dict[keyword]
        else:
            return None

    def get_wikidata_information(self, wikidata_id, languages, recurse):
        """Function for getting information about a wikidata entity.

        Args:
            wikidata_id: the wikidata id
            languages: languages for which the information should be pulled
            recurse: do we also want the entities claims or not?

        Returns:
            labels: entity labels in languages
            descriptions: entity descriptions in languages
            aliases: entity aliases in languages
            claims: either claims or None, depending on recurse
            entity_type: entity type
            datatype: entity datatype
        """
        labels = {}
        descriptions = {}
        aliases = {}
        if wikidata_id[0] == "Q":
            entity = self.wikibase_integrator.item.get(entity_id=wikidata_id).get_json()
            # datatype is only relevant for properties
            datatype = None
        elif wikidata_id[0] == "P":
            entity = self.wikibase_integrator.property.get(
                entity_id=wikidata_id
            ).get_json()
            # datatype describes what kind of data a property
            # links to
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
            claims = []
        else:
            claims = entity["claims"]
        entity_type = entity["type"]
        return (labels, descriptions, aliases, claims, entity_type, datatype)

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

    def add_local_entity(
        self,
        entity_type,
        labels,
        descriptions,
        aliases=None,
        datatype=None,
        claims={},
    ):
        """Function for creating custom entities for local wikibase instance

        Args:
            entity_type: choice from 'item' and 'property'
            labels: format: {language:value,...}; there must at least be an english label
            descriptions: format: {language:value,...}; there must a least be an english description
            aliases: format: {language: [val1,val2,...],...}
            datatype: required if entity is a property; describes what type
                a property links to; specified in wikibaseintegrator.datatypes
            claims: dict; format: {internal_property_id: [{datavalue_dict: datavalue_dict, references: {internal_property_id: [datavalue_dict,...]}},...]}
                where datavalue_dict = {'type': target_type, 'value': info_dict} where info_dict contains information as
                specified in self.get_target
                all ids referenced in this must already exist in local instance

        Returns:
            None
        """

        self.change_config(instance="local")
        login = self.change_login(instance="local")
        id_if_exists = self.check_local_entity(
            labels=labels, descriptions=descriptions, entity_type=entity_type
        )
        if entity_type == "item":
            if id_if_exists:
                entity = self.wikibase_integrator.item.get(entity_id=id_if_exists)
            else:
                entity = self.wikibase_integrator.item.new()
        elif entity_type == "property":
            if id_if_exists:
                entity = self.wikibase_integrator.property.get(entity_id=id_if_exists)
            else:
                entity = self.wikibase_integrator.property.new()
            if datatype:
                entity.datatype = datatype
            else:
                sys.exit("For entity type 'property', a datatype must be specified")
        for lang, val in labels.items():
            entity.labels.set(language=lang, value=val)
        for lang, val in descriptions.items():
            entity.descriptions.set(language=lang, value=val)
        if aliases:
            for lang, vals in aliases.items():
                for val in vals:
                    entity.aliases.set(language=lang, values=val)

        claim_list = []
        for property_id, claim_dicts in claims.items():
            for claim_dict in claim_dicts:
                ref_list = []
                if "references" in claim_dict:
                    for ref_id, refs in claim_dict["references"].items():
                        for ref in refs:
                            ref_list.append(
                                self.get_target(data_value=ref, prop_nr=ref_id)
                            )
                claim_list.append(
                    self.get_target(
                        data_value=claim_dict["datavalue_dict"],
                        prop_nr=property_id,
                        references=list(ref_list),
                    )
                )
        entity.claims.add(
            claim_list,
            ActionIfExists.APPEND_OR_REPLACE,
        )
        entity_description = entity.write(login=login)
        return entity_description.id

    def check_local_entity(self, labels, descriptions, entity_type):
        """Function for checking if a new entity is
        present in the local wiki

        Args:
            labels
            descriptions
            entity_type: item or property

        Returns:
            None or id, if it is in the local wiki
        """
        result = search_entities(
            search_string=labels["en"],
            language="en",
            search_type=entity_type,
            dict_result=True,
        )
        if not result:
            return None
        else:
            # try to find an instance where label (for properties)
            # or label and description (for items) match the
            # entity information
            for subdict in result:
                if subdict["label"] == labels["en"]:
                    # for properties, the label is unique
                    if entity_type == "property":
                        return subdict["id"]
                    # an item is unique in combination (label, description)
                    elif entity_type == "item":
                        # if, additionally to label, the description also matches
                        if subdict["description"] == descriptions["en"]:
                            return subdict["id"]
            # if no entity was found where
            # the required params match, it does
            # not exist
            return None
