"""
Snowflake関連のユーティリティ関数
SQLレスデータ抽出ツール用
"""

import streamlit as st
import pandas as pd
import hashlib
import json
import time
from snowflake.snowpark.context import get_active_session
from datetime import datetime


@st.cache_resource
def init_snowflake_session():
    """Snowflakeセッションを初期化"""
    try:
        session = get_active_session()
        return session
    except Exception as e:
        st.error(f"Snowflakeセッションの初期化に失敗しました: {str(e)}")
        return None


def get_user_context():
    """ユーザーコンテキストを取得"""
    try:
        session = init_snowflake_session()
        if not session:
            return "default_user"
            
        user_info = session.sql("SELECT CURRENT_USER() as user_name, CURRENT_ROLE() as user_role").collect()
        if user_info:
            user_name = user_info[0]['USER_NAME']
            user_role = user_info[0]['USER_ROLE']
            user_context = hashlib.md5(f"{user_name}_{user_role}".encode()).hexdigest()[:16]
            return user_context
        return "default_user"
    except Exception as e:
        st.warning(f"ユーザーコンテキストの取得に失敗: {str(e)}")
        return "default_user"


@st.cache_data(ttl=3600)
def get_snowflake_metadata(_session):
    """Snowflakeのメタデータを取得"""
    try:
        databases = _session.sql("SHOW DATABASES").collect()
        db_list = [row['name'] for row in databases]
        
        metadata = {}
        for db in db_list:
            try:
                schemas = _session.sql(f"SHOW SCHEMAS IN DATABASE {db}").collect()
                
                db_metadata = {
                    "name": f"{db}",
                    "schemas": {}
                }
                
                for schema in schemas:
                    schema_name = schema['name']
                    try:
                        tables = _session.sql(f"SHOW TABLES IN SCHEMA {db}.{schema_name}").collect()
                        table_list = [row['name'] for row in tables]
                        if table_list:
                            db_metadata["schemas"][schema_name] = table_list
                    except Exception as schema_error:
                        st.warning(f"スキーマ {db}.{schema_name} の取得に失敗: {str(schema_error)}")
                        continue
                
                if db_metadata["schemas"]:
                    metadata[db] = db_metadata
            except Exception as db_error:
                st.warning(f"データベース {db} の取得に失敗: {str(db_error)}")
                continue
        
        return metadata
    except Exception as e:
        st.error(f"メタデータの取得に失敗しました: {str(e)}")
        return {}


@st.cache_data(ttl=3600)
def get_table_schema(_session, database, schema, table):
    """テーブルのスキーマ情報を取得"""
    try:
        query = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM {database}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        
        schema_data = _session.sql(query).collect()
        
        columns = []
        for col in schema_data:
            column_info = [
                col['COLUMN_NAME'],
                col['DATA_TYPE'],
                "sample"
            ]
            columns.append(column_info)
        
        return columns
    except Exception as e:
        st.warning(f"テーブルスキーマの取得に失敗しました: {str(e)}")
        return []


def validate_table_columns(database, schema, table):
    """テーブルのカラム情報を検証"""
    try:
        session = init_snowflake_session()
        if not session:
            return False, "Snowflakeセッションが初期化されていません"
        
        schema_data = get_table_schema(session, database, schema, table)
        if not schema_data:
            return False, f"テーブル {table} のスキーマ情報を取得できません"
        return True, schema_data
    except Exception as e:
        return False, f"テーブル検証エラー: {str(e)}"


def get_dynamic_columns(table_name, database, schema):
    """テーブルに応じた動的なカラム情報を取得"""
    try:
        session = init_snowflake_session()
        if not session:
            return {}
            
        is_valid, schema_data = validate_table_columns(database, schema, table_name)
        if not is_valid:
            st.warning(f"テーブル {table_name} の情報取得に失敗: {schema_data}")
            return {}
        
        if not schema_data:
            return {}
        
        columns = {}
        for col in schema_data:
            col_name = col[0]
            col_type = col[1]
            
            if col_type in ['VARCHAR', 'CHAR', 'STRING', 'TEXT']:
                try:
                    if any(keyword in col_name.lower() for keyword in ["category", "region", "status", "type"]):
                        query = f"""
                        SELECT DISTINCT {col_name}
                        FROM {database}.{schema}.{table_name}
                        WHERE {col_name} IS NOT NULL
                        LIMIT 50
                        """
                        distinct_values = session.sql(query).collect()
                        columns[col_name] = [str(row[col_name]) for row in distinct_values if row[col_name] is not None]
                    else:
                        columns[col_name] = []
                except Exception as e:
                    st.warning(f"カラム {col_name} の選択肢取得に失敗: {str(e)}")
                    columns[col_name] = []
            elif col_type in ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ']:
                columns[col_name] = "date_range"
            elif col_type in ['NUMBER', 'DECIMAL', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE']:
                columns[col_name] = "numeric_range"
            else:
                columns[col_name] = []
        
        return columns
    except Exception as e:
        st.warning(f"カラム情報の取得に失敗しました: {str(e)}")
        return {}


def execute_snowflake_query(query):
    """Snowflakeクエリを実行"""
    try:
        session = init_snowflake_session()
        if not session:
            return None, "Snowflakeセッションが初期化されていません"
            
        start_time = time.time()
        result = session.sql(query).collect()
        execution_time = time.time() - start_time
        
        if result:
            df = pd.DataFrame(result)
            return df, execution_time
        return pd.DataFrame(), execution_time
        
    except Exception as e:
        return None, str(e)
