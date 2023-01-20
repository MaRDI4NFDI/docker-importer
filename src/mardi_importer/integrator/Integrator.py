from mardi_importer.integrator.IntegratorConfigParser import IntegratorConfigParser
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.models.qualifiers import Qualifiers
from wikibaseintegrator.models.claims import Claim, Claims
from wikibaseintegrator.models.references import Reference
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
from wikibaseintegrator.wbi_helpers import search_entities
from wikibaseintegrator.wbi_enums import ActionIfExists
import os
import sqlalchemy as db
from wikibaseintegrator.datatypes import String

import sys


class MardiIntegrator(WikibaseIntegrator):
    def __init__(self, conf_path, languages) -> None:
        super(MardiIntegrator, self).__init__()
        self.languages = languages
        self.imported_items = []
        config_parser = IntegratorConfigParser(conf_path)
        self.config_dict = config_parser.parse_config()
        # local id of property for linking to wikidata id
        self.linker_id = None
        # wikidata id to imported id
        self.id_mapping = {}
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"]
        db_host = os.environ["DB_HOST"]
        self.engine = db.create_engine(
            f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}"
        )
        self.check_or_create_db_table()

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

    def insert_id_in_db(self, wikidata_id, internal_id):
        """
        Insert wikidata_id and internal_id into mapping table.

        Args:
            wikidata_id: Wikidata id
            internal_id: local wiki id

        Returns:
            None
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", metadata, autoload=True, autoload_with=self.engine
        )

        ins = table.insert().values(wikidata_id=wikidata_id, internal_id=internal_id)
        with self.engine.connect() as connection:
            result = connection.execute(ins)

    def import_entities(self, id_list, recurse):
        """Function for importing entities from wikidata
        into the local instance

        Args:
            id_list: List of strings of wikidata entity ids (Lexemes not supported)
            recurse: whether to import claims for the entities in id_list

        Returns:
            None
        """
        # config should be on local unless it is
        # required to be on remote
        self.change_config(instance="local")
        # does not work in init, so it is here
        self.change_login(instance="local")
        self.set_linker_id()
        for wikidata_id in id_list:
            if wikidata_id[0] == "L":
                print(
                    f"Warning: Lexemes not supported. Lexeme {wikidata_id} was not imported"
                )
                continue
            print(f"importing entity {wikidata_id}")
            entity = self.get_wikidata_information(
                wikidata_id=wikidata_id, recurse=recurse
            )
            if not entity:
                print(f"No labels for entity with id {wikidata_id}, skipping")
                continue
            if recurse == True:
                self.convert_claim_ids(entity)
                self.add_linker_claim(entity=entity, wikidata_id=wikidata_id)
            # if it is not there yet
            if not self.check_entity_exists(entity, wikidata_id):
                new_id = entity.write(login=self.login, as_new=True).id

                self.id_mapping[wikidata_id] = new_id
                self.insert_id_in_db(wikidata_id, new_id)
            # if it is there
            else:
                if wikidata_id[0] == "Q":
                    local_entity = self.item.get(entity_id=self.id_mapping[wikidata_id])
                elif wikidata_id[0] == "P":
                    local_entity = self.property.get(
                        entity_id=self.id_mapping[wikidata_id]
                    )
                # replace descriptions
                local_entity.descriptions = entity.descriptions
                # add new claims if they are different from old claims
                local_entity.claims.add(
                    entity.claims,
                    ActionIfExists.APPEND_OR_REPLACE,
                )
                # to also add this for older imports
                self.add_linker_claim(entity=local_entity, wikidata_id=wikidata_id)
                local_entity.write(login=self.login)

    def add_linker_claim(self, entity, wikidata_id):
        """Function for in-place addition of a claim with the
        property that points to the wikidata id
        to the local entity

        Args:
            entity: wikibaeintegrator entity whose claims
                    this should be added to
            wikidata_id: wikidata id of the wikidata item
        """
        claim = String(
            value=wikidata_id,
            prop_nr=self.linker_id,
        )
        entity.add_claims(claim)

    def get_linker_id(self, label_string_en):
        """Function for getting linker_id from the local
        instance by using search_entities function.
        If it does not exist yet, returns None.

        Args:
            label_string_en: string with the english label

        Returns:
            linker_id or None
        """
        result = search_entities(
            search_string=label_string_en,
            language="en",
            search_type="property",
            dict_result=True,
        )
        if not result:
            return None
        else:
            return result[0]["id"]

    def set_linker_id(self):
        """Function for setting self.linker_id of the local property that links to the
        wikidata id. Gets linker_id from local instance or creates it.

        Returns: None
        """
        label_string_en = "has wikidata id"
        linker_id = self.get_linker_id(label_string_en=label_string_en)
        if not linker_id:
            prop = self.property.new()
            prop.labels.set(language="en", value=label_string_en)
            prop.descriptions.set(language="en", value="has a wikidata id")
            prop.datatype = "string"
            linker_id = prop.write(login=self.login, as_new=True).id
        self.linker_id = linker_id

    def write_claim_entities(self, wikidata_id):
        """Function for importing entities that are mentioned
        in claims from wikidata to the local wikibase instance

        Args:
            wikidata_id(str): id of the entity to be imported

        Returns:
            local id or None, if the entity had no labels
        """
        entity = self.get_wikidata_information(wikidata_id=wikidata_id, recurse=False)
        # if entity had no labels
        if not entity:
            return None
        if not self.check_entity_exists(entity, wikidata_id):
            self.add_linker_claim(entity=entity, wikidata_id=wikidata_id)
            local_id = entity.write(login=self.login, as_new=True).id
            self.id_mapping[wikidata_id] = local_id
            self.insert_id_in_db(wikidata_id, local_id)
            return local_id
        else:
            # if it does exist, do nothing,
            # as it does not contain claims anyway
            return self.id_mapping[wikidata_id]

    def get_wikidata_information(self, wikidata_id, recurse):
        """Function for pulling wikidata information

        Args:
            wikidata_id(str): wikidata id of the desired entity
            recurse (Bool): if claims should also be imported

        Returns: wikibase integrator entity or None, if the entity has no labels

        """
        self.change_config(instance="wikidata")
        if wikidata_id[0] == "Q":
            entity = self.item.get(entity_id=wikidata_id)
        elif wikidata_id[0] == "P":
            entity = self.property.get(entity_id=wikidata_id)
        else:
            raise Exception(
                f"Wrong ID format, should start with P, L or Q but ID is {wikidata_id}"
            )
        self.change_config(instance="local")
        if not self.languages == "all":
            # set labels in desired languages
            label_dict = {
                k: entity.labels.values[k]
                for k in self.languages
                if k in entity.labels.values
            }
            # if there are no labels, this is not
            # a valid entity
            if not label_dict:
                return None
            entity.labels.values = label_dict

            # set descriptions in desired languages
            description_dict = {
                k: entity.descriptions.values[k]
                for k in self.languages
                if k in entity.descriptions.values
            }
            entity.descriptions.values = description_dict

            # set aliases in desired languages
            alias_dict = {
                k: entity.aliases.aliases[k]
                for k in self.languages
                if k in entity.aliases.aliases
            }
            entity.aliases.aliases = alias_dict
        if recurse == False:
            entity.claims = Claims()
        return entity

    def convert_claim_ids(self, entity):
        """Function for in-place conversion of wikidata
        ids found in claims into local ids

        Args:
            entity

        Returns:
            None
        """
        entity_names = [
            "wikibase-item",
            "wikibase-property",
        ]
        claims = entity.claims.claims
        new_claims = {}
        # structure of claims: Dict[str,List[Claim]]
        # where str is the property id
        for prop_id, claim_list in claims.items():
            local_claim_list = []
            local_prop_id = self.write_claim_entities(wikidata_id=prop_id)
            if not local_prop_id:
                print("Warning: local id skipped")
                continue
            for c in claim_list:
                c_dict = c.get_json()
                if c_dict["mainsnak"]["datatype"] in entity_names:
                    if "datavalue" in c_dict["mainsnak"]:
                        local_mainsnak_id = self.write_claim_entities(
                            wikidata_id=c_dict["mainsnak"]["datavalue"]["value"]["id"],
                        )
                        if not local_mainsnak_id:
                            continue
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
                        new_c.id = None
                    else:
                        continue
                elif c_dict["mainsnak"]["datatype"] == "wikibase-lexeme":
                    continue
                else:
                    self.convert_entity_links(snak=c_dict["mainsnak"])
                    new_c = c
                    new_c.mainsnak.property_number = local_prop_id
                    new_c.id = None
                # get reference details
                new_references = self.get_references(c)
                if new_references:
                    new_c.references.references = new_references
                # get qualifier details
                new_qualifiers = self.get_qualifiers(c)
                new_c.qualifiers = new_qualifiers
                local_claim_list.append(new_c)
            new_claims[local_prop_id] = local_claim_list
        entity.claims.claims = new_claims

    def get_references(self, claim):
        """Function for creating references from wikidata references
        and in place adding them to the claim

        Args:
            claim: a wikibaseintegrator claim

        Returns:
            List with references, can also be an empty list
        """
        entity_names = [
            "wikibase-item",
            "wikibase-property",
        ]
        # format: List(Reference)
        ref_list = claim.references.references
        if not ref_list:
            return None
        new_ref_list = []
        for ref in ref_list:
            new_snak_dict = {}
            snak_dict = ref.get_json()
            for prop_id, snak_list in snak_dict["snaks"].items():
                new_snak_list = []
                new_prop_id = self.write_claim_entities(
                    wikidata_id=prop_id,
                )
                if not new_prop_id:
                    continue
                for snak in snak_list:
                    if snak["datatype"] in entity_names:
                        if not "datavalue" in snak:
                            continue
                        new_snak_id = self.write_claim_entities(
                            wikidata_id=snak["datavalue"]["value"]["id"],
                        )
                        if not new_snak_id:
                            continue
                        snak["datavalue"]["value"]["id"] = new_snak_id
                        snak["datavalue"]["value"]["numeric-id"] = int(new_snak_id[1:])
                    elif snak["datatype"] == "wikibase-lexeme":
                        continue
                    else:
                        self.convert_entity_links(
                            snak=snak,
                        )
                    snak["property"] = new_prop_id
                    new_snak_list.append(snak)
                new_snak_dict[new_prop_id] = new_snak_list
            complete_new_snak_dict = {}
            complete_new_snak_dict["hash"] = None
            complete_new_snak_dict["snaks"] = new_snak_dict
            complete_new_snak_dict["snaks-order"] = []
            r = Reference()
            new_ref_list.append(r.from_json(json_data=complete_new_snak_dict))
        return new_ref_list

    def get_qualifiers(self, claim):
        """Function for creating qualifiers from wikidata qualifiers
        and in place adding them to the claim

        Args:
            claim: a wikibaseintegrator claim

        Returns:
            Qualifiers object, can also be an empty object
        """
        entity_names = [
            "wikibase-item",
            "wikibase-property",
        ]
        qual_dict = claim.qualifiers.get_json()
        new_qual_dict = {}
        for qual_id, qual_list in qual_dict.items():
            new_qual_id = self.write_claim_entities(wikidata_id=qual_id)
            if not new_qual_id:
                continue
            new_qual_list = []
            for qual_val in qual_list:
                if qual_val["datatype"] in entity_names:
                    if not "datavalue" in qual_val:
                        continue
                    new_qual_val_id = self.write_claim_entities(
                        wikidata_id=qual_val["datavalue"]["value"]["id"],
                    )
                    if not new_qual_val_id:
                        continue
                    qual_val["datavalue"]["value"]["id"] = new_qual_val_id
                    qual_val["datavalue"]["value"]["numeric-id"] = int(
                        new_qual_val_id[1:]
                    )
                elif qual_val["datatype"] == "wikibase-lexeme":
                    continue
                else:
                    self.convert_entity_links(
                        snak=qual_val,
                    )
                qual_val["property"] = new_qual_id
                new_qual_list.append(qual_val)
            new_qual_dict[new_qual_id] = new_qual_list
        q = Qualifiers()
        qualifiers = q.from_json(json_data=new_qual_dict)
        return qualifiers

    def check_entity_exists(self, entity, wikidata_id):
        """Check if entity exists with a lookup (in this order) in
        self.id_mapping, db table and wiki. Add to where it is missing,
        if it only exists in some of them.

        Args:
           unit: an IntegratorUnit
           wikidata_id
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
        with self.engine.connect() as connection:
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
            search_string=str(entity.labels.get("en")),
            language="en",
            search_type=entity.type,
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
                if subdict["label"] == entity.labels.get("en"):
                    # for properties, the label is unique
                    if wikidata_id[0] == "P":
                        self.id_mapping[wikidata_id] = subdict["id"]
                        self.insert_id_in_db(wikidata_id, subdict["id"])
                        return True
                    # an item is unique in combination (label, description)
                    elif wikidata_id[0] == "Q":
                        # if, additionally to label, the description also matches
                        if subdict["description"] == entity.descriptions.get("en"):
                            self.id_mapping[wikidata_id] = subdict["id"]
                            self.insert_id_in_db(wikidata_id, subdict["id"])
                            return True
                    else:
                        sys.exit(
                            "Exception: wikidata id starts with letter other than Q or P"
                        )
            # if no entity was found where
            # the required params match, it does
            # not exist
            return False

    def create_id_list_from_file(self, file):
        """Function for creating a list of ids
        from a while where each id is in a new line

        Args:
            file: path to file

        Returns: list of ids
        """
        id_list = []
        with open(file, "r") as file:
            for line in file:
                id_list.append(line.strip())
        return id_list

    def convert_entity_links(self, snak):
        """Function for in-place conversion of unit for quantity and globe for globecoordinate
        to a link to the local entity instead of a link to the wikidata entity.

        Args:
            snak: a wikibaseintegrator snak

        Returns:
            None
        """
        if "datatype" not in snak or "datavalue" not in snak:
            return
        data = snak["datavalue"]["value"]
        if snak["datatype"] == "quantity":
            if "unit" in data:
                link_string = data["unit"]
                key_string = "unit"
        elif snak["datatype"] == "globecoordinate":
            if "globe" in data:
                link_string = data["globe"]
                key_string = "globe"
        else:
            return
        if "www.wikidata.org/" in link_string:
            uid = link_string.split("/")[-1]
            local_id = self.write_claim_entities(
                wikidata_id=uid,
            )
            data[key_string] = wbi_config["WIKIBASE_URL"] + "entity/" + local_id

    def change_config(self, instance):
        """
        Function for changing the config to allow using wikidata and local instance.
        Also set user agent to avoid warning.
        Setting config to local should be default, after switching to remote.
        it should be switched back

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
            instance: Instance name (choice between wikidata and local; however,
            wikidata does not do anything as a login there is not needed at the moment)

        Returns:
            None
        """
        if instance == "wikidata":
            pass
        elif instance == "local":
            login_instance = wbi_login.Clientlogin(
                user=os.environ.get("BOTUSER_NAME"),
                password=os.environ.get("BOTUSER_PW"),
            )
            self.login = login_instance
        else:
            sys.exit("Invalid instance")
