import os
import re
from typing import List, Optional, Union, Literal
from functools import lru_cache
import sqlalchemy as db
from sqlalchemy import Engine
from sqlalchemy.engine import Connection

EntityType = Literal["property", "item"]

class Search():
    def __init__(self) -> None:
        self.engine = self.create_engine()

    def create_engine(self) -> Engine:
        """
        Creates SQLalchemy engine

        Returns:
            SQLalchemy engine
        """
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_host = os.environ["DB_HOST"]
        db_name = 'my_wiki'

        return db.create_engine(
            url="mariadb+mariadbconnector://{0}:{1}@{2}/{3}".format(
                db_user, db_pass, db_host, db_name
            )
        )

    def _get_QID(self, label: str, alias: bool = False) -> List[str]:
        """Creates a list of QID of all items in the local wikibase with the
        same label

        Args:
            label: The label to search for
            alias: Whether to also search in aliases

        Returns:
            List of QID strings
        """

        label_bytes: bytes = bytes(label, "utf-8")
        is_truncated: bool = False
        if len(label_bytes) > 250:
            label_bytes = label_bytes[:250]
            is_truncated = True

        def query_wikidata_table(field_type: int) -> List[str]:
            # field_type = 1 : Label
            # field_type = 2 : Alias
            # see: https://doc.wikimedia.org/Wikibase/REL1_41/php/docs_sql_wbt_type.html
            with self.engine.connect() as connection:
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
                        .where(db.and_(
                                    db.case(
                                        (is_truncated, wbt_text.columns.wbx_text.like(label_bytes + b"%")),
                                        else_=wbt_text.columns.wbx_text == label_bytes), 
                                    wbt_term_in_lang.columns.wbtl_type_id == field_type,
                                    wbt_text_in_lang.columns.wbxl_language == bytes("en", "utf-8"))))
                results = connection.execute(query).fetchall()
                return [f"Q{result[0]}" for result in results]
            
        entity_id: List[str] = query_wikidata_table(field_type=1)
        if alias:
            entity_id += query_wikidata_table(field_type=2)
        
        return entity_id
    

    def _get_PID(self, label: str) -> Optional[str]:
        """Returns the PID of the property with the same label
        
        Args:
            label: The property label to search for
            
        Returns:
            Property ID string if found, None otherwise
        """
        with self.engine.connect() as connection:
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
                    .where(db.and_(wbt_text.columns.wbx_text == bytes(label, "utf-8"), 
                                wbt_term_in_lang.columns.wbtl_type_id == 1,
                                wbt_text_in_lang.columns.wbxl_language == bytes("en", "utf-8"))))
            results = connection.execute(query).fetchall()
            if results:
                for result in results:
                    return f"P{str(result[0])}"
            return None

    def get_local_id_by_label(self, entity_str: str, entity_type: EntityType) -> Optional[str]:
        """Check if entity with a given label or wikidata PID/QID 
        exists in the local wikibase instance. 

        Args:
            entity_str: It can be a string label or a wikidata ID, 
                specified with the prefix wdt: for properties and wd:
                for items.
            entity_type: Either 'property' or 'item' to specify
                which type of entity to look for.

        Returns:
            Local ID of the entity, if found.
        """
        if re.match("^[PQ]\d+$", entity_str):
            return entity_str
        elif not entity_str.startswith("wdt:") and not entity_str.startswith("wd:"):
            if entity_type == "property":
                return self._get_PID(entity_str)
            elif entity_type == "item":
                qids = self._get_QID(entity_str)
                return qids[0] if qids else None
        elif entity_str.startswith("wdt:"):
            wikidata_id: str = entity_str[4:]
        elif entity_str.startswith("wd:"):
            wikidata_id = entity_str[3:]
        else:
            return None

        with self.engine.connect() as connection:
            metadata = db.MetaData()
            table_name: str = "items"
            if wikidata_id.startswith('P'):
                table_name = "properties"
            table = db.Table(
                table_name, 
                metadata,
                autoload_with=self.engine
            )
            sql = db.select(table.columns.local_id).where(
                table.columns.wikidata_id == wikidata_id[1:],
            )
            db_result = connection.execute(sql).fetchone()
            if db_result:
                if wikidata_id.startswith('Q'):
                    return f"Q{db_result[0]}"
                else:
                    return f"P{db_result[0]}"
        return None

@lru_cache(maxsize=1)
def get_search_instance() -> Search:
    """Returns a cached Search instance (singleton-like behavior)"""
    return Search()

def get_local_id_by_label(entity_str: str, entity_type: EntityType) -> Optional[str]:
    return get_search_instance().get_local_id_by_label(entity_str, entity_type)