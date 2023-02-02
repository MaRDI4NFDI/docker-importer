import re
import sqlalchemy as db
from sqlalchemy import and_

from wikibaseintegrator.entities.item import ItemEntity
from wikibaseintegrator.entities.property import PropertyEntity
from wikibaseintegrator.wbi_exceptions import ModificationFailed
from wikibaseintegrator.datatypes import ExternalID
from mardi_importer.importer.Importer import ImporterException

def handleModificationFailed(e):
    """Handle ModificationFailed Exception
    """
    messages = list(filter(lambda x: 'parameters' in x, e.messages))
    for message in messages:
        for parameter in message['parameters']:
            result = re.search(r"\[\[\w*:(\w\d+)\|\w\d+\]\]", parameter)
            if result:
                # Modify this to return an entity
                return result.group(1)


class MardiItemEntity(ItemEntity):

    def new(self, **kwargs):
        return MardiItemEntity(api=self.api, **kwargs)
    
    def write(self, **kwargs):
        try:
            entity = super().write(**kwargs)
            return entity
        except ModificationFailed as e:
            return handleModificationFailed(e)

    def get(self, **kwargs):
        entity = super().get(**kwargs)
        kwargs['entity_id'] = entity.id
        json_data = super(ItemEntity, self)._get(**kwargs)
        return MardiItemEntity(api=self.api).from_json(json_data=json_data['entities'][entity.id])

    def exists(self): 
        """Checks through the Wikibase DB if an item with same label
        and description already exists
        """

        description = ""
        if 'en' in self.descriptions.values:
            description = self.descriptions.values['en'].value

        # List of items with the same label
        QID_list = self.get_QID()

        # Check if there is an item with the same description
        for QID in QID_list:
            item = ItemEntity(api=self.api).new()
            item = item.get(QID)
            if description == item.descriptions.values.get('en'):
                return QID

    def add_claim(self, prop_nr, value=None, **kwargs):
        claim = self.api.get_claim(prop_nr, value, **kwargs)
        self.claims.add(claim)

    def is_instance_of(self, instance):
        """Checks if a given entity is an instance of 'instance' item 
        """
        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        instance_QID = self.api.get_local_id_by_label(instance, 'item')
        if type(instance_QID) is list: instance_QID = instance_QID[0]

        instance_of_PID = self.api.get_local_id_by_label('instance of', 'property')

        item_QID_list = self.get_QID()
        for QID in item_QID_list:
            item = self.api.item.get(QID)
            item_claims = item.get_json()['claims']
            if instance_of_PID in item_claims:
                if (instance_QID == 
                    item_claims[instance_of_PID][0]['mainsnak']['datavalue']['value']['id']):
                    return item.id
        return False

    def is_instance_of_with_property(self, instance, prop_str, value):
        item_QID = self.is_instance_of(instance)
        prop_nr = self.api.get_local_id_by_label(prop_str, 'property')
        if item_QID:
            item = self.api.item.get(item_QID)
            item_claims = item.get_json()['claims']
            values = self.__return_values(prop_nr, item_claims)
            if value in values: return item_QID

    def get_value(self, prop_str):
        QID = self.exists()
        if QID:
            item = ItemEntity(api=self.api).new()
            item = item.get(QID)
            item_claims = item.get_json()['claims']
            prop_nr = self.api.get_local_id_by_label(prop_str, 'property')
            return self.__return_values(prop_nr, item_claims)
    
    def __return_values(self, prop_nr, claims):
        values = []
        if prop_nr in claims:
            for mainsnak in claims[prop_nr]:
                if mainsnak['mainsnak']['datatype'] == 'string':
                    values.append(mainsnak['mainsnak']['datavalue']['value'])
                elif mainsnak['mainsnak']['datatype'] == 'wikibase-item':
                    values.append(mainsnak['mainsnak']['datavalue']['value']['id'])
                elif mainsnak['mainsnak']['datatype'] == 'time':
                    values.append(mainsnak['mainsnak']['datavalue']['value']['time'])
        return values

    def get_QID(self):
        """Creates a list of QID of all items in the local wikibase with the
        same label
        """

        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        with self.api.engine.connect() as connection:
            metadata = db.MetaData()
            try:
                wbt_item_terms = db.Table(
                    "wbt_item_terms", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_term_in_lang = db.Table(
                    "wbt_term_in_lang", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_text_in_lang = db.Table(
                    "wbt_text_in_lang", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_text = db.Table(
                    "wbt_text", metadata, autoload=True, autoload_with=self.api.engine
                )
                query = (db.select([wbt_item_terms.columns.wbit_item_id])
                        .join(wbt_term_in_lang, wbt_item_terms.columns.wbit_term_in_lang_id == wbt_term_in_lang.columns.wbtl_id)
                        .join(wbt_text_in_lang, wbt_term_in_lang.columns.wbtl_text_in_lang_id == wbt_text_in_lang.columns.wbxl_id)
                        .join(wbt_text, wbt_text.columns.wbx_id == wbt_text_in_lang.columns.wbxl_text_id)
                        .where(and_(wbt_text.columns.wbx_text == bytes(label, "utf-8"), wbt_term_in_lang.columns.wbtl_type_id == 1)))
                results = connection.execute(query).fetchall()
                entity_id = []
                if results:
                    for result in results:
                        entity_id.append(f"Q{str(result[0])}")

            except Exception as e:
                raise ImporterException(
                    "Error attempting to read mappings from database\n{}".format(e)
                )
            
            return entity_id

    def add_linker_claim(self, wikidata_id):
        """Function for in-place addition of a claim with the
        property that points to the wikidata id
        to the local entity

        Args:
            entity: WikibaseIntegrator entity whose claims
                    this should be added to
            wikidata_id: wikidata id of the wikidata item
        """
        claim = ExternalID(
            value=wikidata_id,
            prop_nr=self.api.wikidata_QID,
        )
        self.add_claims(claim)
    

class MardiPropertyEntity(PropertyEntity):
    def new(self, **kwargs):
        return MardiPropertyEntity(api=self.api, **kwargs)
    
    def write(self, **kwargs):
        try:
            entity = super().write(**kwargs)
            return entity
        except ModificationFailed as e:
            return handleModificationFailed(e)

    def get(self, **kwargs):
        entity = super().get(**kwargs)
        kwargs['entity_id'] = entity.id
        json_data = super(PropertyEntity, self)._get(**kwargs)
        return MardiPropertyEntity(api=self.api).from_json(json_data=json_data['entities'][entity.id])

    def exists(self): 
        """Checks through the Wikibase DB if a property with the 
        same label already exists.
        """
        return self.get_PID()

    def get_PID(self):
        """Returns the PID of the property with the same label
        """

        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        with self.api.engine.connect() as connection:
            metadata = db.MetaData()
            try:
                wbt_property_terms = db.Table(
                    "wbt_property_terms", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_term_in_lang = db.Table(
                    "wbt_term_in_lang", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_text_in_lang = db.Table(
                    "wbt_text_in_lang", metadata, autoload=True, autoload_with=self.api.engine
                )
                wbt_text = db.Table(
                    "wbt_text", metadata, autoload=True, autoload_with=self.api.engine
                )
                query = (db.select([wbt_property_terms.columns.wbpt_property_id])
                        .join(wbt_term_in_lang, wbt_term_in_lang.columns.wbtl_id == wbt_property_terms.columns.wbpt_term_in_lang_id)
                        .join(wbt_text_in_lang, wbt_term_in_lang.columns.wbtl_text_in_lang_id == wbt_text_in_lang.columns.wbxl_id)
                        .join(wbt_text, wbt_text.columns.wbx_id == wbt_text_in_lang.columns.wbxl_text_id)
                        .where(and_(wbt_text.columns.wbx_text == bytes(label, "utf-8"), wbt_term_in_lang.columns.wbtl_type_id == 1)))
                prefix = "P"
                results = connection.execute(query).fetchall()
                if results:
                    for result in results:
                        return f"P{str(result[0])}"

            except Exception as e:
                raise ImporterException(
                    "Error attempting to read mappings from database\n{}".format(e)
                )

    def add_linker_claim(self, wikidata_id):
        """Function for in-place addition of a claim with the
        property that points to the wikidata id
        to the local entity

        Args:
            entity: WikibaseIntegrator entity whose claims
                    this should be added to
            wikidata_id: wikidata id of the wikidata item
        """
        claim = ExternalID(
            value=wikidata_id,
            prop_nr=self.api.wikidata_PID,
        )
        self.add_claims(claim)
