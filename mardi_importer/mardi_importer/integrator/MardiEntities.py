import re
import sqlalchemy as db
from sqlalchemy import and_, case

from mardiclient import MardiItem, MardiProperty
from wikibaseintegrator.wbi_exceptions import ModificationFailed
from wikibaseintegrator.datatypes import ExternalID
from wikibaseintegrator.wbi_enums import ActionIfExists

class MardiItemEntity(MardiItem):

    def new(self, **kwargs):
        return MardiItemEntity(api=self.api, **kwargs)

    def get(self, entity_id, **kwargs):
        json_data = super(MardiItemEntity, self)._get(entity_id=entity_id, **kwargs)
        return MardiItemEntity(api=self.api).from_json(json_data=json_data['entities'][entity_id])

    def get_QID(self, alias=False):
        """Creates a list of QID of all items in the local wikibase with the
        same label

        Returns:
            QIDs (list): List of QID
        """

        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value
        label = bytes(label, "utf-8")
        is_truncated = False
        if len(label) > 250:
            label = label[:250]
            is_truncated = True

        def query_wikidata_table(field_type):
            # field_type = 1 : Label
            # field_type = 2 : Alias
            # see: https://doc.wikimedia.org/Wikibase/REL1_41/php/docs_sql_wbt_type.html
            entity_id = []
            with self.api.mw_engine.connect() as connection:
                metadata = db.MetaData()
                
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
                        .where(and_(
                                    case(
                                        (is_truncated, wbt_text.columns.wbx_text.like(label + b"%")),
                                        else_=wbt_text.columns.wbx_text == label), 
                                    wbt_term_in_lang.columns.wbtl_type_id == field_type,
                                    wbt_text_in_lang.columns.wbxl_language == bytes("en", "utf-8"))))
                results = connection.execute(query).fetchall()
                return [f"Q{result[0]}" for result in results]
            
        entity_id = query_wikidata_table(field_type=1)
        if alias:
            entity_id += query_wikidata_table(field_type=2)
        
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

        with self.api.mw_engine.connect() as connection:
            metadata = db.MetaData()

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
            results = connection.execute(query).fetchall()
            if results:
                for result in results:
                    return f"P{str(result[0])}"
