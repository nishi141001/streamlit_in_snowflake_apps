"""
設定管理モジュール
Snowflakeテーブルベースの永続化設定管理
"""

import streamlit as st
import json
import time
from datetime import datetime
from snowflake_utils import init_snowflake_session, get_user_context


# 設定テーブル名
CONFIG_TABLE_NAME = 'SQL_TOOL_USER_CONFIGS'


def check_config_table_exists():
    """設定テーブルの存在確認"""
    try:
        session = init_snowflake_session()
        if not session:
            return False
            
        current_schema_result = session.sql("SELECT CURRENT_SCHEMA() as schema_name").collect()
        current_schema = current_schema_result[0]['SCHEMA_NAME'] if current_schema_result else 'PUBLIC'
        
        check_query = f"""
        SELECT COUNT(*) as table_exists 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{current_schema}' 
        AND TABLE_NAME = '{CONFIG_TABLE_NAME}'
        """
        
        result = session.sql(check_query).collect()
        return result[0]['TABLE_EXISTS'] > 0
        
    except Exception as e:
        st.warning(f"テーブル存在確認エラー: {str(e)}")
        return False


def create_config_table():
    """設定保存用テーブルを作成"""
    try:
        session = init_snowflake_session()
        if not session:
            return False, "Snowflakeセッションが初期化されていません"
            
        create_query = f"""
        CREATE TABLE {CONFIG_TABLE_NAME} (
            config_id STRING PRIMARY KEY COMMENT '設定の一意ID',
            config_name STRING NOT NULL COMMENT '設定名',
            user_context STRING NOT NULL COMMENT 'ユーザーコンテキスト',
            config_data VARIANT NOT NULL COMMENT '設定データ（JSON形式）',
            description STRING COMMENT '設定の説明',
            tags ARRAY COMMENT 'タグ（検索用）',
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT '作成日時',
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT '更新日時',
            last_used TIMESTAMP_NTZ COMMENT '最終使用日時',
            is_active BOOLEAN DEFAULT TRUE COMMENT 'アクティブフラグ',
            version INTEGER DEFAULT 1 COMMENT '設定のバージョン'
        ) COMMENT = 'SQLレスデータ抽出ツール - ユーザー設定保存テーブル'
        """
        
        session.sql(create_query).collect()
        
        # インデックスを作成
        index_queries = [
            f"CREATE INDEX IF NOT EXISTS idx_{CONFIG_TABLE_NAME}_user_context ON {CONFIG_TABLE_NAME} (user_context)",
            f"CREATE INDEX IF NOT EXISTS idx_{CONFIG_TABLE_NAME}_config_name ON {CONFIG_TABLE_NAME} (config_name)",
            f"CREATE INDEX IF NOT EXISTS idx_{CONFIG_TABLE_NAME}_active ON {CONFIG_TABLE_NAME} (is_active)",
            f"CREATE INDEX IF NOT EXISTS idx_{CONFIG_TABLE_NAME}_updated_at ON {CONFIG_TABLE_NAME} (updated_at)"
        ]
        
        for idx_query in index_queries:
            try:
                session.sql(idx_query).collect()
            except Exception as idx_error:
                st.warning(f"インデックス作成で警告: {str(idx_error)}")
        
        return True, "テーブルが正常に作成されました"
        
    except Exception as e:
        return False, f"テーブル作成エラー: {str(e)}"


def load_persistent_configs():
    """Snowflakeテーブルから設定を読み込み"""
    try:
        if not check_config_table_exists():
            st.warning("設定保存テーブルが見つかりません。初期設定を完了してください。")
            st.session_state.persistent_configs_loaded = True
            return
            
        session = init_snowflake_session()
        if not session:
            return
            
        user_context = get_user_context()
        
        query = f"""
        SELECT config_name, config_data, description, 
               TO_VARCHAR(created_at) as created_at, 
               TO_VARCHAR(updated_at) as updated_at, 
               TO_VARCHAR(last_used) as last_used,
               tags, version
        FROM {CONFIG_TABLE_NAME}
        WHERE user_context = '{user_context}' 
        AND is_active = TRUE
        ORDER BY updated_at DESC
        """
        
        results = session.sql(query).collect()
        
        loaded_configs = {}
        for row in results:
            config_name = row['CONFIG_NAME']
            
            config_data_raw = row['CONFIG_DATA']
            if isinstance(config_data_raw, str):
                config_data = json.loads(config_data_raw)
            elif hasattr(config_data_raw, 'as_map'):
                config_data = dict(config_data_raw.as_map())
            else:
                config_data = config_data_raw
            
            config_data['created_at'] = row['CREATED_AT']
            config_data['updated_at'] = row['UPDATED_AT'] 
            config_data['last_used'] = row['LAST_USED']
            config_data['description'] = row['DESCRIPTION'] or ''
            
            # タグとバージョン情報
            if 'TAGS' in row and row['TAGS']:
                try:
                    tags_raw = row['TAGS']
                    if isinstance(tags_raw, str):
                        config_data['tags'] = json.loads(tags_raw)
                    elif hasattr(tags_raw, 'as_list'):
                        config_data['tags'] = list(tags_raw.as_list())
                    else:
                        config_data['tags'] = tags_raw or []
                except:
                    config_data['tags'] = []
            else:
                config_data['tags'] = []
            
            config_data['version'] = row.get('VERSION', 1)
            
            loaded_configs[config_name] = config_data
        
        if loaded_configs:
            st.session_state.saved_configs = loaded_configs
            st.success(f"✅ {len(loaded_configs)}件の設定を読み込みました")
        else:
            st.info("💡 保存済み設定がありません")
        
        st.session_state.persistent_configs_loaded = True
        
    except Exception as e:
        st.error(f"⚠️ 設定の読み込み中にエラー: {str(e)}")
        st.session_state.persistent_configs_loaded = True


def save_config_to_table(config_name, config_data, description="", tags=None):
    """設定をSnowflakeテーブルに保存"""
    try:
        if not check_config_table_exists():
            st.error("❌ 設定保存テーブルが存在しません。初期設定を完了してください。")
            return False
            
        session = init_snowflake_session()
        if not session:
            return False
            
        user_context = get_user_context()
        current_time = int(time.time())
        config_id = f"{user_context}_{config_name}_{current_time}"
        
        if tags is None:
            tags = []
        
        # 既存の設定を無効化
        try:
            deactivate_query = f"""
            UPDATE {CONFIG_TABLE_NAME}
            SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP()
            WHERE user_context = '{user_context}' AND config_name = '{config_name.replace("'", "''")}'
            """
            session.sql(deactivate_query).collect()
        except Exception as deactivate_error:
            st.warning(f"既存設定の無効化で警告: {str(deactivate_error)}")
        
        # 新しい設定を挿入
        config_json = json.dumps(config_data, ensure_ascii=False)
        tags_json = json.dumps(tags, ensure_ascii=False) if tags else "[]"
        
        safe_config_name = config_name.replace("'", "''")
        safe_description = description.replace("'", "''")
        safe_config_json = config_json.replace("'", "''")
        safe_tags_json = tags_json.replace("'", "''")
        
        try:
            insert_query = f"""
            INSERT INTO {CONFIG_TABLE_NAME} 
            (config_id, config_name, user_context, config_data, description, tags, created_at, updated_at, last_used, is_active, version)
            VALUES ('{config_id}', '{safe_config_name}', '{user_context}', 
                    PARSE_JSON('{safe_config_json}'), 
                    '{safe_description}',
                    PARSE_JSON('{safe_tags_json}'),
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE, 1)
            """
            session.sql(insert_query).collect()
            
        except Exception as insert_error:
            # 基本テーブル構造を試行
            try:
                basic_insert_query = f"""
                INSERT INTO {CONFIG_TABLE_NAME} 
                (config_id, config_name, user_context, config_data, description, created_at, updated_at, last_used, is_active)
                VALUES ('{config_id}', '{safe_config_name}', '{user_context}', 
                        PARSE_JSON('{safe_config_json}'), 
                        '{safe_description}', 
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE)
                """
                session.sql(basic_insert_query).collect()
            except Exception as basic_error:
                st.error(f"設定の保存に失敗: {str(basic_error)}")
                return False
        
        return True
        
    except Exception as e:
        st.error(f"❌ 設定の保存に失敗: {str(e)}")
        return False


def delete_config_from_table(config_name):
    """設定をテーブルから削除"""
    try:
        session = init_snowflake_session()
        if not session:
            return False
            
        user_context = get_user_context()
        safe_config_name = config_name.replace("'", "''")
        
        delete_query = f"""
        UPDATE {CONFIG_TABLE_NAME}
        SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP()
        WHERE user_context = '{user_context}' AND config_name = '{safe_config_name}'
        """
        
        session.sql(delete_query).collect()
        return True
        
    except Exception as e:
        st.error(f"❌ 設定の削除に失敗: {str(e)}")
        return False


def update_last_used(config_name):
    """最終使用日時を更新"""
    try:
        session = init_snowflake_session()
        if not session:
            return
            
        user_context = get_user_context()
        safe_config_name = config_name.replace("'", "''")
        
        update_query = f"""
        UPDATE {CONFIG_TABLE_NAME}
        SET last_used = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP()
        WHERE user_context = '{user_context}' AND config_name = '{safe_config_name}' AND is_active = TRUE
        """
        
        session.sql(update_query).collect()
        
    except Exception as e:
        st.warning(f"⚠️ 最終使用日時の更新に失敗: {str(e)}")


def force_reload_configs():
    """設定を強制的に再読み込み"""
    st.session_state.persistent_configs_loaded = False
    st.session_state.saved_configs = {}
    load_persistent_configs()


def get_table_statistics():
    """テーブルの統計情報を取得"""
    try:
        session = init_snowflake_session()
        if not session:
            return None
            
        user_context = get_user_context()
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_records,
            COUNT(CASE WHEN user_context = '{user_context}' THEN 1 END) as user_records,
            COUNT(DISTINCT user_context) as unique_users,
            MIN(created_at) as first_created,
            MAX(updated_at) as last_updated
        FROM {CONFIG_TABLE_NAME}
        """
        
        result = session.sql(stats_query).collect()
        return result[0] if result else None
        
    except Exception as e:
        st.warning(f"統計情報取得エラー: {str(e)}")
        return None


def insert_sample_data():
    """サンプルデータを挿入"""
    try:
        session = init_snowflake_session()
        if not session:
            return False, "Snowflakeセッションが初期化されていません"
            
        user_context = get_user_context()
        
        sample_configs = [
            {
                "name": "サンプル設定1",
                "description": "売上データ抽出用の設定例",
                "data": {
                    "db": "SAMPLE_DB",
                    "schema": "PUBLIC",
                    "table": "SALES_DATA",
                    "conditions": {
                        "date_range": {"from": "2024-01-01", "to": "2024-12-31"},
                        "group_by": ["REGION", "PRODUCT_CATEGORY"]
                    },
                    "join_conditions": [],
                    "filter_conditions": []
                },
                "tags": ["売上", "レポート", "月次"]
            },
            {
                "name": "サンプル設定2", 
                "description": "顧客データ分析用の設定例",
                "data": {
                    "db": "SAMPLE_DB",
                    "schema": "PUBLIC", 
                    "table": "CUSTOMER_DATA",
                    "conditions": {
                        "limit_rows": 1000,
                        "sort_column": "CREATED_DATE",
                        "sort_order": "DESC"
                    },
                    "join_conditions": [
                        {
                            "table": "ORDER_DATA",
                            "type": "LEFT JOIN",
                            "left_col": "CUSTOMER_ID",
                            "right_col": "CUSTOMER_ID"
                        }
                    ],
                    "filter_conditions": [
                        {
                            "column": "STATUS",
                            "type": "値を選択"
                        }
                    ]
                },
                "tags": ["顧客", "分析", "JOIN"]
            }
        ]
        
        inserted_count = 0
        for config in sample_configs:
            config_id = f"{user_context}_{config['name']}_{int(time.time())}_{inserted_count}"
            config_json = json.dumps(config['data'], ensure_ascii=False)
            tags_json = json.dumps(config['tags'], ensure_ascii=False)
            
            safe_name = config['name'].replace("'", "''")
            safe_description = config['description'].replace("'", "''")
            safe_config_json = config_json.replace("'", "''")
            safe_tags_json = tags_json.replace("'", "''")
            
            insert_query = f"""
            INSERT INTO {CONFIG_TABLE_NAME} 
            (config_id, config_name, user_context, config_data, description, tags, created_at, updated_at, last_used, is_active, version)
            VALUES (
                '{config_id}',
                '{safe_name}',
                '{user_context}',
                PARSE_JSON('{safe_config_json}'),
                '{safe_description}',
                PARSE_JSON('{safe_tags_json}'),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE,
                1
            )
            """
            
            session.sql(insert_query).collect()
            inserted_count += 1
        
        return True, f"{inserted_count}件のサンプルデータを挿入しました"
        
    except Exception as e:
        return False, f"サンプルデータ挿入エラー: {str(e)}"
