from mardi_importer.integrator.IntegratorUnit import IntegratorUnit
from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.models import Qualifiers
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

    def test_import(self, id_list, languages, recurse):
        # needs to be before change_config, because it gets
        # change to local in add_local_entity
        self.wikidata_linker_id = self.add_local_entity(
            entity_type="property",
            labels={"en": "has_wikidata_id"},
            descriptions={"en": "has a wikidata id"},
            datatype="external-id",
        )
        self.change_config(instance="wikidata")
        for entity_id in id_list:
            entity = self.test_get_wikidata_information(
                wikidata_id=entity_id, languages=languages, recurse=False
            )
            self.change_config(instance="local")
            login = self.change_login(instance="local")
            entity.id = None
            # try:
            # entity.write(login=login)
            # except:
            self.debug_write(entity)
            sys.exit(f"wrote entity {entity_id}")
        if recurse == True:
            # make sure to also import everything in claims, refs and quals, also targets
            # before importing, still check if it exists
            # potentially exclude lexemes
            # maybe exclude somevalue and novalue, but maybe not
            # add wikidata id to claims
            # make own method for writes
            # check if it should be updated or only taken if it doesnt exist
            # insert id in db and mapping
            pass
        # todo:
        # if there are no labels for the desired language, rempove this value from claims

    def test_get_wikidata_information(self, wikidata_id, languages, recurse):
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
                raise Exception("No labels left when using these languages")
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

        return entity

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
                    if secondary_id[0] == "L":
                        continue
                    # add property
                    print(secondary_id)
                    self.add_secondary_units(unit_id=secondary_id, languages=languages)

                    for relation in claims[secondary_id]:
                        # exclude edge cases
                        if relation["mainsnak"]["snaktype"] in ["somevalue", "novalue"]:
                            continue
                        value = relation["mainsnak"]["datavalue"]["value"]

                        if "id" in value and isinstance(value, dict):
                            target_id = value["id"]
                            # exclude lexemes
                            if target_id[0] == "L":
                                continue
                            # add property target if it is an entity
                            self.add_secondary_units(
                                unit_id=target_id, languages=languages
                            )

                        if "references" in relation:
                            for single_reference in relation["references"]:
                                references = single_reference["snaks"]
                                for ref_id in references:
                                    if ref_id[0] == "L":
                                        continue
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
                                            if ref_value["id"][0] != "L":
                                                # add target of property in references
                                                self.add_secondary_units(
                                                    unit_id=ref_value["id"],
                                                    languages=languages,
                                                )
                        if "qualifiers" in relation:
                            for qual_id, qual in relation["qualifiers"].items():
                                if qual_id[0] == "L":
                                    continue
                                # add property name of qualifier
                                self.add_secondary_units(
                                    unit_id=qual_id, languages=languages
                                )
                                # for each target of this property in qualifiers,
                                # add item if it is an item
                                for qual_snak in qual:
                                    if "datavalue" in qual_snak:
                                        qual_value = qual_snak["datavalue"]["value"]
                                        if "id" in qual_value and isinstance(
                                            qual_value, dict
                                        ):
                                            if qual_value["id"][0] != "L":
                                                # add target of property in qualifiers
                                                self.add_secondary_units(
                                                    unit_id=qual_value["id"],
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
        print(f"unit id from inside add secondary units: {unit_id}")
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

                try:
                    entity_description = entity.write(login=login)
                except:
                    #     ------------------------------------------------
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
                            print(
                                "Connection error: %s. Sleeping for %d seconds.", e, 60
                            )
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
                    print(f"wikidata id: {wikidata_id}")
                    sys.exit("Exited")
                    # -----------------------------------------------------
                    print(unit.claims)
                    print(entity.claims)
                    sys.exit("unit claim exit")

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
                    try:
                        entity_description = entity.write(login=login)
                    except Exception as e:
                        print(e)
                        print(unit.claims)
                        print(entity.claims)
                        sys.exit("second entity make")

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
                if unit.dataype == "wikibase-form":
                    entity.datatype = "string"
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
                    qual_object = Qualifiers()
                    # if there are references for the claim
                    if "references" in relation:
                        # there can be multiple refs with multiple parts
                        for single_reference in relation["references"]:
                            single_ref_list = []
                            references = single_reference["snaks"]
                            for ref_id in references:
                                if ref_id in self.invalid_wikidata_ids:
                                    continue
                                # add reference property targets
                                for ref_snak in references[ref_id]:
                                    # if it is an entity and the id is in the invalid ids, skip
                                    if (
                                        ref_snak["datavalue"]["type"]
                                        == "wikibase-entityid"
                                    ):
                                        if (
                                            ref_snak["datavalue"]["value"]["id"]
                                            in self.invalid_wikidata_ids
                                        ):
                                            continue
                                    target = self.get_target(
                                        ref_snak["datavalue"], prop_nr=ref_id
                                    )
                                    single_ref_list.append(target)
                            ref_list.append(single_ref_list)
                    # if "qualifiers" in relation:
                    #     qualifiers = relation["qualifiers"]
                    #     for qual_id, qual in qualifiers.items():
                    #         if qual_id in self.invalid_wikidata_ids:
                    #             continue
                    #         # add qualifier property targets
                    #         for qual_snak in qual:
                    #             if not "datavalue" in qual_snak:
                    #                 continue
                    #             # if it is an entity and the id is in the invalid ids, skip
                    #             if (
                    #                 qual_snak["datavalue"]["type"]
                    #                 == "wikibase-entityid"
                    #             ):
                    #                 if (
                    #                     qual_snak["datavalue"]["value"]["id"]
                    #                     in self.invalid_wikidata_ids
                    #                 ):
                    #                     continue
                    #             target = self.get_target(
                    #                 qual_snak["datavalue"], prop_nr=qual_id
                    #             )
                    #             qual_object.add(target)
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
                        qualifiers=None,  # qual_object,
                    )
                    claims.append(target)
        return claims

    def get_target(self, data_value, prop_nr, references=None, qualifiers=None):
        """Function for returning a property target of the type
        String, Item, MonolingualText, Time, CommonsMedia, ExternalID,
        Form, GeoSHape, GlobeCoordinate, Lexeme, Math, MusicalNotation,
        Quantity, Sense, TabularData or URL.

        Args:
            data_value: datavalue dict from wikidata for the target
            prop_nr: property id
            references: references for claim
            qualifiers: qualifiers for claim

        Returns:
            object of one of the above mentioned types
        """
        value_dict = data_value["value"]
        if data_value["type"] == "string":
            return String(
                value=data_value["value"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "wikibase-entityid":
            internal_id = self.id_mapping[data_value["value"]["id"]]
            if internal_id[0] == "P":
                return Property(
                    value=internal_id,
                    prop_nr=self.id_mapping[prop_nr],
                    references=references,
                    qualifiers=qualifiers,
                )
            else:
                return Item(
                    value=internal_id,
                    prop_nr=self.id_mapping[prop_nr],
                    references=references,
                    qualifiers=qualifiers,
                )
        elif data_value["type"] == "monolingualtext":
            return MonolingualText(
                text=data_value["value"]["text"],
                language=data_value["value"]["language"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
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
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "commonsmedia":
            return CommonsMedia(
                value=data_value["value"]["commonsmedia"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "external-id":
            return ExternalID(
                value=data_value["value"]["external-id"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "wikibase-form":
            # return Form(
            #     value=data_value["value"]["wikibase-form"],
            #     prop_nr=self.id_mapping[prop_nr],
            #     references=references,
            #     qualifiers=qualifiers,
            # )
            return String(
                value="test",
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "geo-shape":
            return GeoShape(
                value=data_value["value"]["geo-shape"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
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
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "wikibase-lexeme":
            return Lexeme(
                value=data_value["value"]["wikibase-lexeme"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "math":
            return Math(
                value=data_value["value"]["math"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "musical-notation":
            return MusicalNotation(
                value=data_value["value"]["musical notation"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
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
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "wikibase-sense":
            return Sense(
                value=data_value["value"]["wikibase-sense"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "tabular-data":
            return TabularData(
                value=data_value["value"]["tabular-data"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
            )
        elif data_value["type"] == "url":
            return URL(
                value=data_value["value"]["url"],
                prop_nr=self.id_mapping[prop_nr],
                references=references,
                qualifiers=qualifiers,
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
            if keyword != "wikibase_url":
                val = value_dict[keyword]
                # url in non-url field
                if isinstance(val, str) and "www.wikidata" in val:
                    return None
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
        print(f"wikidata id from inside get information: {wikidata_id}")
        if wikidata_id[0] == "Q":
            entity = self.wikibase_integrator.item.get(entity_id=wikidata_id).get_json()
            # datatype is only relevant for properties
            datatype = None
        elif wikidata_id[0] == "P":
            entity = self.wikibase_integrator.property.get(
                entity_id=wikidata_id
            ).get_json()
            test = self.wikibase_integrator.property.get(entity_id=wikidata_id)
            print(test.descriptions.values)
            print(test.claims.claims)
            print(type(test.claims.claims))
            print(list(test.claims.claims))
            sys.exit("test exit")
            # datatype describes what kind of data a property
            # links to
            datatype = entity["datatype"]
        else:
            raise Exception(
                f"Wrong ID format, should start with P, Q or L, but ID is {wikidata_id}"
            )
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
