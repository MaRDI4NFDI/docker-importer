import os
import re
import sqlalchemy as db

from .MardiEntities import MardiItemEntity, MardiPropertyEntity
from mardiclient import MardiClient
from wikibaseintegrator import wbi_login
from wikibaseintegrator.models import Claim, Claims, Qualifiers, Reference
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import search_entities, execute_sparql_query
from wikibaseintegrator.datatypes import (URL, CommonsMedia, ExternalID, Form, GeoShape, GlobeCoordinate, Item, Lexeme, Math, MonolingualText, MusicalNotation, Property, Quantity,
                                          Sense, String, TabularData, Time)

class MardiIntegrator(MardiClient):
    def __init__(self, languages=["en", "de"]) -> None:
        super().__init__()
        self.languages = languages

        self.setup = True
        self.login = self.config()        
        self.engine = self.create_engine()
        self.create_db_table()

        # local id of properties for linking to wikidata PID/QID
        self.wikidata_PID = self.init_wikidata_PID() if self.setup else None
        self.wikidata_QID = self.init_wikidata_QID() if self.setup else None

        self.item = MardiItemEntity(api=self)
        self.property = MardiPropertyEntity(api=self)

        self.excluded_properties = ['P1151', 'P1855', 'P2139', 'P2302', \
                                    'P2559', 'P2875', 'P3254', 'P3709', \
                                    'P3713', 'P3734', 'P6104', 'P6685', \
                                    'P8093', 'P8979']

    def config(self):
        """
        Sets up initial configuration for the integrator

        Returns:
            Clientlogin object
        """
        if os.environ.get("IMPORTER_USER") and os.environ.get("IMPORTER_PASS"): 
            wbi_config["USER_AGENT"] = os.environ.get("IMPORTER_AGENT")
            wbi_config["MEDIAWIKI_API_URL"] = os.environ.get("MEDIAWIKI_API_URL")
            wbi_config["SPARQL_ENDPOINT_URL"] = os.environ.get("SPARQL_ENDPOINT_URL")
            wbi_config["WIKIBASE_URL"] = os.environ.get("WIKIBASE_URL")
            return wbi_login.Clientlogin(
                user=os.environ.get("IMPORTER_USER"),
                password=os.environ.get("IMPORTER_PASS"),
            )
        else:
            self.setup = False

    def create_engine(self):
        """
        Creates SQLalchemy engine

        Returns:
            SQLalchemy engine
        """
        if self.setup:
            db_user = os.environ["DB_USER"]
            db_pass = os.environ["DB_PASS"]
            db_name = os.environ["DB_NAME"]
            db_host = os.environ["DB_HOST"]
            return db.create_engine(
                f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}"
            )

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

    def create_db_table(self):
        """
        Check if db table for id mapping is there; if not, create.

        Args:
            None

        Returns:
            None
        """
        if self.engine:
            with self.engine.connect() as connection:
                metadata = db.MetaData()
                if not db.inspect(self.engine).has_table("wb_id_mapping"):
                    mapping_table = db.Table(
                        "wb_id_mapping",
                        metadata,
                        db.Column("id", db.Integer, primary_key=True),
                        db.Column("wikidata_id", db.String(24), nullable=False),
                        db.Column("local_id", db.String(24), nullable=False),
                        db.Column("has_all_claims", db.Boolean(), nullable=False),  
                        db.UniqueConstraint("wikidata_id"),
                        db.UniqueConstraint("local_id"),
                    )
                    metadata.create_all(self.engine)
    
    def insert_id_in_db(self, wikidata_id, local_id, has_all_claims):
        """
        Insert wikidata_id, local_id and has_all_claims into mapping table.

        Args:
            wikidata_id: Wikidata id
            local_id: local Wikibase id
            has_all_claims: Boolean indicating whether the entity has been
                imported with all claims or no claims (i.e. no recurse)

        Returns:
            None
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", 
            metadata,
            autoload_with=self.engine
        )

        ins = table.insert().values(
            wikidata_id=wikidata_id,
            local_id=local_id,
            has_all_claims=has_all_claims
        )

        with self.engine.connect() as connection:
            connection.execute(ins)
            connection.commit()

    def update_has_all_claims(self, wikidata_id):
        """
        Set the has_all_claims property in the wb_id_mapping table
        to True for the given wikidata_id.

        Args:
            wikidata_id: Wikidata id to be updated.

        Returns:
            None
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", 
            metadata,
            autoload_with=self.engine
        )

        ins = table.update().values(
            has_all_claims=True
        ).where(table.c.wikidata_id == wikidata_id)

        with self.engine.connect() as connection:
            connection.execute(ins)
            connection.commit()

    def init_wikidata_PID(self):
        """
        Searches the wikidata PID property ID to link
        properties to its ID in wikidata. When not found,
        it creates the property.

        Returns
            wikidata_PID (str): wikidata PID property ID
        """
        label = "Wikidata PID"
        wikidata_PID = self.get_local_id_by_label(label, "property")
        if not wikidata_PID:
            prop = self.property.new()
            prop.labels.set(language="en", value=label)
            prop.descriptions.set(
                language="en", 
                value="Identifier in Wikidata of the corresponding properties"
            )
            prop.datatype = "external-id"
            wikidata_PID = prop.write(login=self.login, as_new=True).id
        return wikidata_PID

    def init_wikidata_QID(self):
        """
        Searches the wikidata QID property ID to link
        items to its ID in wikidata. When not found,
        it creates the property.

        Returns
            wikidata_QID (str): wikidata QID property ID
        """
        label = "Wikidata QID"
        wikidata_QID = self.get_local_id_by_label(label, "property")
        if not wikidata_QID:
            prop = self.property.new()
            prop.labels.set(language="en", value=label)
            prop.descriptions.set(
                language="en", 
                value="Corresponding QID in Wikidata"
            )
            prop.datatype = "external-id"
            wikidata_QID = prop.write(login=self.login, as_new=True).id
        return wikidata_QID

    def import_entities(self, id_list=None, filename="", recurse=True):
        """Function for importing entities from wikidata
        into the local instance.

        It can accept a single id, a list of ids or a file containing
        a the ids to be imported.

        Args:
            id_list: Single string or list of strings of wikidata 
                entity ids. Lexemes not supported.
            filename: Filename containing list of entities to be 
                imported.
            recurse: Whether to import claims for the entities in 
                id_list

        Returns:
            Imported entities (Dict): Dictionary containing the local ids of 
            all the imported entities.
        """
        imported_entities = {}
        if filename: id_list = self.create_id_list_from_file(filename)
        if isinstance(id_list, str): id_list = [id_list]

        for wikidata_id in id_list:

            if wikidata_id.startswith("L"):
                print(
                    f"Warning: Lexemes not supported. Lexeme {wikidata_id} was not imported"
                )
                continue

            print(f"importing entity {wikidata_id}")

            has_all_claims = self.query('has_all_claims', wikidata_id)
            if not has_all_claims:
                # API call
                entity = self.get_wikidata_information(
                    wikidata_id, 
                    recurse
                )

                if not entity:
                    print(f"No labels for entity with id {wikidata_id}, skipping")
                    continue

                if entity.type == "property" and entity.datatype.value in \
                    ["wikibase-lexeme", "wikibase-sense", "wikibase-form"]:
                    print(f"Warning: Lexemes not supported. Property skipped")
                    continue

                # Check if there is an internal ID redirection in Wikidata
                if wikidata_id != entity.id:
                    wikidata_id = entity.id
                    has_all_claims = self.query('has_all_claims', wikidata_id)
                    if has_all_claims:
                        imported_entities[wikidata_id] = self.query('local_id', wikidata_id)
                        continue

                if recurse:
                    self.convert_claim_ids(entity)

                entity.add_linker_claim(wikidata_id)
                
                local_id = entity.exists()
                if not local_id:
                    local_id = self.query('local_id', wikidata_id)

                if local_id:
                    # Update existing entity
                    if entity.type == "item":
                        local_entity = self.item.get(entity_id=local_id)
                    elif entity.type == "property":
                        local_entity = self.property.get(entity_id=local_id)
                    # replace descriptions
                    local_entity.descriptions = entity.descriptions
                    # add new claims if they are different from old claims
                    local_entity.claims.add(
                        entity.claims,
                        ActionIfExists.APPEND_OR_REPLACE,
                    )
                    local_entity.write(login=self.login)
                    if self.query('local_id', wikidata_id) and recurse:
                        self.update_has_all_claims(wikidata_id)
                    else:
                        self.insert_id_in_db(wikidata_id, local_id, has_all_claims=recurse)
                else:
                    # Create entity
                    local_id = entity.write(login=self.login, as_new=True).id
                    self.insert_id_in_db(wikidata_id, local_id, has_all_claims=recurse)  

            if has_all_claims:
                imported_entities[wikidata_id] = self.query('local_id', wikidata_id)
            else:
                imported_entities[wikidata_id] = local_id

        if len(imported_entities) == 1:
            return list(imported_entities.values())[0]
        return imported_entities

    def overwrite_entity(self, wikidata_id, local_id):
        """Function for completing an already existing local entity
        with its statements from wikidata.

        Args:
            wikidata_id: Wikidata entity ID to be imported.
            local_id: Local id of the existing entity that needs to
                be completed with further statements.

        Returns:
            local_id: Local entity ID
        """
        if wikidata_id.startswith("L"):
            print(
                f"Warning: Lexemes not supported. Lexeme {wikidata_id} was not imported"
            )

        print(f"Overwriting entity {local_id}")

        has_all_claims = self.query('has_all_claims', wikidata_id)
        if has_all_claims:
            return self.query('local_id', wikidata_id)
        else:
            # API call
            entity = self.get_wikidata_information(
                wikidata_id, 
                recurse=True
            )

            if entity:

                # Check if there is an entity ID redirection in Wikidata
                if wikidata_id != entity.id:
                    wikidata_id = entity.id
                    has_all_claims = self.query('has_all_claims', wikidata_id)
                    if has_all_claims:
                        return self.query('local_id', wikidata_id)

                self.convert_claim_ids(entity)
                entity.add_linker_claim(wikidata_id)
                
                # Retrieve existing entity
                if entity.type == "item":
                    local_entity = self.item.get(entity_id=local_id)
                elif entity.type == "property":
                    local_entity = self.property.get(entity_id=local_id)
                # replace descriptions
                local_entity.descriptions = entity.descriptions
                # add new claims if they are different from old claims
                local_entity.claims.add(
                    entity.claims,
                    ActionIfExists.APPEND_OR_REPLACE,
                )
                local_entity.write(login=self.login)
                if self.query('local_id', wikidata_id):
                    self.update_has_all_claims(wikidata_id)
                else:
                    self.insert_id_in_db(wikidata_id, local_id, has_all_claims=True)  
            
            return local_id

    def import_claim_entities(self, wikidata_id):
        """Function for importing entities that are mentioned
        in claims from wikidata to the local wikibase instance

        Args:
            wikidata_id(str): id of the entity to be imported

        Returns:
            local id or None, if the entity had no labels
        """
        local_id = self.query('local_id', wikidata_id)
        if local_id: return local_id
        else:
            entity = self.get_wikidata_information(wikidata_id)

            if not entity:
                return None

            if entity.type == "property" and \
                entity.datatype.value in ["wikibase-lexeme", \
                        "wikibase-sense", "wikibase-form"]:
                return None
                
            elif wikidata_id != entity.id:
                wikidata_id = entity.id
                local_id = self.query('local_id', wikidata_id)
                if local_id: return local_id

            # Check if the entity has been redirected by Wikidata
            # into another entity that has already been imported
            local_id = self.query('local_id', entity.id)
            if local_id: return local_id

            local_id = entity.exists()
            if local_id:
                if entity.type == "item":
                    new_entity = self.item.get(entity_id=local_id)
                elif entity.type == "property":
                    new_entity = self.property.get(entity_id=local_id)
                # replace descriptions
                new_entity.descriptions = entity.descriptions
                entity = new_entity
                entity.add_linker_claim(wikidata_id)
                local_id = entity.write(login=self.login).id
            else:
                entity.add_linker_claim(wikidata_id)
                local_id = entity.write(login=self.login, as_new=True).id

            self.insert_id_in_db(wikidata_id, local_id, has_all_claims=False)
            return local_id

    def get_wikidata_information(self, wikidata_id, recurse=False):
        """Function for pulling wikidata information

        Args:
            wikidata_id (str): wikidata id of the desired entity
            recurse (Bool): if claims should also be imported

        Returns: wikibase integrator entity or None, if the entity has no labels

        """
        if wikidata_id.startswith("Q"):
            entity = self.item.get(
                entity_id=wikidata_id, 
                mediawiki_api_url='https://www.wikidata.org/w/api.php'
            )
        elif wikidata_id.startswith("P"):
            entity = self.property.get(
                entity_id=wikidata_id, 
                mediawiki_api_url='https://www.wikidata.org/w/api.php'
            )
        else:
            raise Exception(
                f"Wrong ID format, should start with P, L or Q but ID is {wikidata_id}"
            )
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
            
            # make sure label != description (e.g. wdt:P121)
            for k in self.languages:
                if (label_dict.get(k) and 
                    label_dict.get(k) == description_dict.get(k)):
                    entity.descriptions.set(
                    language=k,
                    value=None
                )            

            # set aliases in desired languages
            alias_dict = {
                k: entity.aliases.aliases[k]
                for k in self.languages
                if k in entity.aliases.aliases
            }
            entity.aliases.aliases = alias_dict
        if not recurse:
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
            if prop_id not in self.excluded_properties:
                local_prop_id = self.import_claim_entities(wikidata_id=prop_id)
                if not local_prop_id:
                    print("Warning: local id skipped")
                    continue
                for c in claim_list:
                    c_dict = c.get_json()
                    if c_dict["mainsnak"]["datatype"] in entity_names:
                        if "datavalue" in c_dict["mainsnak"]:
                            local_mainsnak_id = self.import_claim_entities(
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
                    elif c_dict["mainsnak"]["datatype"] in ["wikibase-lexeme", "wikibase-sense", "wikibase-form"]:
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
                new_prop_id = self.import_claim_entities(
                    wikidata_id=prop_id,
                )
                if not new_prop_id:
                    continue
                for snak in snak_list:
                    if snak["datatype"] in entity_names:
                        if not "datavalue" in snak:
                            continue
                        new_snak_id = self.import_claim_entities(
                            wikidata_id=snak["datavalue"]["value"]["id"],
                        )
                        if not new_snak_id:
                            continue
                        snak["datavalue"]["value"]["id"] = new_snak_id
                        snak["datavalue"]["value"]["numeric-id"] = int(new_snak_id[1:])
                    elif snak["datatype"] in ["wikibase-lexeme", "wikibase-sense", "wikibase-form"]:
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
            new_qual_id = self.import_claim_entities(wikidata_id=qual_id)
            if not new_qual_id:
                continue
            new_qual_list = []
            for qual_val in qual_list:
                if qual_val["datatype"] in entity_names:
                    if not "datavalue" in qual_val:
                        continue
                    new_qual_val_id = self.import_claim_entities(
                        wikidata_id=qual_val["datavalue"]["value"]["id"],
                    )
                    if not new_qual_val_id:
                        continue
                    qual_val["datavalue"]["value"]["id"] = new_qual_val_id
                    qual_val["datavalue"]["value"]["numeric-id"] = int(
                        new_qual_val_id[1:]
                    )
                elif qual_val["datatype"] in ["wikibase-lexeme", "wikibase-sense", "wikibase-form"]:
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

    def convert_entity_links(self, snak):
        """Function for in-place conversion of unit for quantity 
        and globe for globecoordinate to a link to the local entity 
        instead of a link to the wikidata entity.

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
        elif snak["datatype"] == "globe-coordinate":
            #if "globe" in data:
            #    link_string = data["globe"]
            #    key_string = "globe"
            if not data["precision"]:
                data["precision"] = 1/3600
            return
        else:
            return
        if "www.wikidata.org/" in link_string:
            uid = link_string.split("/")[-1]
            local_id = self.import_claim_entities(
                wikidata_id=uid,
            )
            data[key_string] = wbi_config["WIKIBASE_URL"] + "/entity/" + local_id

    def query(self, parameter, wikidata_id):
        """Query the wb_id_mapping db table for a given parameter.

        The two important parameters are the local_id and whether the
        entity has already been imported with all claims

        Args:
            parameter (str): Either local_id or has_all_claims
            wikidata_id (str): Wikidata ID
        Returns:
            str or boolean: for local_id returns the local ID if it exists,
                otherwise None. For has_all_claims, a boolean is returned.
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", 
            metadata, 
            autoload_with=self.engine
        )
        if parameter in ['local_id', 'has_all_claims']:
            sql = db.select(table.columns[parameter]).where(
                table.columns.wikidata_id == wikidata_id,
            )
            with self.engine.connect() as connection:
                db_result = connection.execute(sql).fetchone()
            if db_result:
                return db_result[0]
            
    def query_with_local_id(self, parameter, local_id):
        """Query the wb_id_mapping db table for a given parameter.

        The two important parameters are the wikidata_id and whether the
        entity has already been imported with all claims

        Args:
            parameter (str): Either wikidata_id or has_all_claims
            local_id (str): local ID
        Returns:
            str or boolean: for wikidata_id returns the wikidata ID if it exists,
                otherwise None. For has_all_claims, a boolean is returned.
        """
        metadata = db.MetaData()
        table = db.Table(
            "wb_id_mapping", 
            metadata, 
            autoload_with=self.engine
        )
        if parameter in ['wikidata_id', 'has_all_claims']:
            sql = db.select(table.columns[parameter]).where(
                table.columns.local_id == local_id,
            )
            with self.engine.connect() as connection:
                db_result = connection.execute(sql).fetchone()
            if db_result:
                return db_result[0]

    def get_local_id_by_label(self, entity_str, entity_type):
        """Check if entity with a given label or wikidata PID/QID 
        exists in the local wikibase instance. 

        Args:
            entity_str (str): It can be a string label or a wikidata ID, 
                specified with the prefix wdt: for properties and wd:
                for items.
            entity_type (str): Either 'property' or 'item' to specify
                which type of entity to look for.

        Returns:
           str: Local ID of the entity, if found.
        """
        if re.match("^[PQ]\d+$", entity_str):
            return entity_str
        elif not entity_str.startswith("wdt:") and not entity_str.startswith("wd:"):
            if entity_type == "property":
                new_property = MardiPropertyEntity(api=self).new()
                new_property.labels.set(language='en', value=entity_str)
                return new_property.get_PID()
            elif entity_type == "item":
                new_item = MardiItemEntity(api=self).new()
                new_item.labels.set(language='en', value=entity_str)
                return new_item.get_QID()
        elif entity_str.startswith("wdt:"):
            wikidata_id = entity_str[4:]
        elif entity_str.startswith("wd:"):
            wikidata_id = entity_str[3:]

        with self.engine.connect() as connection:
            metadata = db.MetaData()
            table = db.Table(
                "wb_id_mapping", metadata, autoload_with=connection
            )
            sql = db.select(table.columns.local_id).where(
                table.columns.wikidata_id == wikidata_id,
            )
            db_result = connection.execute(sql).fetchone()
            if db_result:
                return db_result[0]

    def import_from_label(self, label):
        """
        Imports an entity from Wikidata just from a label

        Args:
            label (str): label to be imported from wikidata

        Returns:
            local_id (str): local id for the imported entity
        """
        results = search_entities(label, 
                                  dict_result=True,
                                  mediawiki_api_url='https://www.wikidata.org/w/api.php')
        for result in results:
            if label == result['label']:
                return self.import_entities(result['id'])
            if label.lower() == result['label'].lower():
                return self.import_entities(result['id'])
            if result['aliases']:
                if label.lower() == result['aliases'][0].lower():
                    return self.import_entities(result['id'])