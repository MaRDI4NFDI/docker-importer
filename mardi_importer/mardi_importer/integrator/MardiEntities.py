import re
import sqlalchemy as db
from sqlalchemy import and_

from mardiclient import MardiItem, MardiProperty
from wikibaseintegrator.wbi_exceptions import ModificationFailed
from wikibaseintegrator.datatypes import ExternalID
from wikibaseintegrator.wbi_enums import ActionIfExists
from mardi_importer.importer import ImporterException

class MardiItemEntity(MardiItem):

    def new(self, **kwargs):
        return MardiItemEntity(api=self.api, **kwargs)

    def get(self, entity_id, **kwargs):
        json_data = super(MardiItemEntity, self)._get(entity_id=entity_id, **kwargs)
        return MardiItemEntity(api=self.api).from_json(json_data=json_data['entities'][entity_id])

    def get_QID(self):
        """Creates a list of QID of all items in the local wikibase with the
        same label

        Returns:
            QIDs (list): List of QID
        """

        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        with self.api.engine.connect() as connection:
            metadata = db.MetaData()
            try:
                wbt_item_terms = db.Table(
                    "wbt_item_terms", metadata, autoload_with=connection
                )
                wbt_term_in_lang = db.Table(
                    "wbt_term_in_lang", metadata, autoload_with=connection
                )
                wbt_text_in_lang = db.Table(
                    "wbt_text_in_lang", metadata, autoload_with=connection
                )
                wbt_text = db.Table(
                    "wbt_text", metadata, autoload_with=connection
                )
                query = (db.select(wbt_item_terms.columns.wbit_item_id)
                        .join(wbt_term_in_lang, wbt_item_terms.columns.wbit_term_in_lang_id == wbt_term_in_lang.columns.wbtl_id)
                        .join(wbt_text_in_lang, wbt_term_in_lang.columns.wbtl_text_in_lang_id == wbt_text_in_lang.columns.wbxl_id)
                        .join(wbt_text, wbt_text.columns.wbx_id == wbt_text_in_lang.columns.wbxl_text_id)
                        .where(and_(wbt_text.columns.wbx_text == bytes(label, "utf-8"), 
                                    wbt_term_in_lang.columns.wbtl_type_id == 1,
                                    wbt_text_in_lang.columns.wbxl_language == bytes("en", "utf-8"))))
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
    

class MardiPropertyEntity(MardiProperty):

    def new(self, **kwargs):
        return MardiPropertyEntity(api=self.api, **kwargs)

    def get(self, entity_id, **kwargs):
        json_data = super(MardiPropertyEntity, self)._get(entity_id=entity_id, **kwargs)
        return MardiPropertyEntity(api=self.api).from_json(json_data=json_data['entities'][entity_id])

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
                    "wbt_property_terms", metadata, autoload_with=connection
                )
                wbt_term_in_lang = db.Table(
                    "wbt_term_in_lang", metadata, autoload_with=connection
                )
                wbt_text_in_lang = db.Table(
                    "wbt_text_in_lang", metadata, autoload_with=connection
                )
                wbt_text = db.Table(
                    "wbt_text", metadata, autoload_with=connection
                )
                query = (db.select(wbt_property_terms.columns.wbpt_property_id)
                        .join(wbt_term_in_lang, wbt_term_in_lang.columns.wbtl_id == wbt_property_terms.columns.wbpt_term_in_lang_id)
                        .join(wbt_text_in_lang, wbt_term_in_lang.columns.wbtl_text_in_lang_id == wbt_text_in_lang.columns.wbxl_id)
                        .join(wbt_text, wbt_text.columns.wbx_id == wbt_text_in_lang.columns.wbxl_text_id)
                        .where(and_(wbt_text.columns.wbx_text == bytes(label, "utf-8"), 
                                    wbt_term_in_lang.columns.wbtl_type_id == 1,
                                    wbt_text_in_lang.columns.wbxl_language == bytes("en", "utf-8"))))
                prefix = "P"
                results = connection.execute(query).fetchall()
                if results:
                    for result in results:
                        return f"P{str(result[0])}"

            except Exception as e:
                raise ImporterException(
                    "Error attempting to read mappings from database\n{}".format(e)
                )
