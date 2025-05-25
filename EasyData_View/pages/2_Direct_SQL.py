import streamlit as st
import pandas as pd
from utils import (
    get_snowpark_session,
    get_available_databases,
    get_available_schemas,
    get_available_tables,
    get_table_schema,
    display_sql,
)

# ---------------------------------------------------
# Streamlitアプリの基本設定
# ---------------------------------------------------
st.set_page_config(
    page_title="直接SQLクエリ - Snowflake Cortex Analyst",
    page_icon="💻",
    layout="wide",
)

# ===================================================
# 直接SQLクエリモードのメイン処理
# ---------------------------------------------------
st.title("💻 直接SQLクエリモード")

# Snowflake接続の確認
session = get_snowpark_session()
if not session:
    st.warning("Snowflake接続を確立できませんでした。")
    st.stop()

# 利用可能なデータベースの取得
databases = get_available_databases()
if not databases:
    st.warning("利用可能なデータベースが見つかりませんでした。")
    st.stop()

# サイドバーでデータベースを選択
current_db = session.get_current_database()
selected_db = st.sidebar.selectbox(
    "データベースを選択",
    options=databases,
    index=databases.index(current_db) if current_db in databases else 0,
)

# 選択されたデータベースのスキーマを取得
schemas = get_available_schemas(selected_db)
if not schemas:
    st.warning(f"データベース {selected_db} に利用可能なスキーマが見つかりませんでした。")
    st.stop()

# サイドバーでスキーマを選択
current_schema = session.get_current_schema()
selected_schema = st.sidebar.selectbox(
    "スキーマを選択",
    options=schemas,
    index=schemas.index(current_schema) if current_schema in schemas else 0,
)

# 選択されたスキーマのテーブルを取得
tables = get_available_tables(selected_db, selected_schema)
if not tables:
    st.warning(f"スキーマ {selected_schema} に利用可能なテーブルが見つかりませんでした。")
    st.stop()

# サイドバーでテーブルを選択
selected_table = st.sidebar.selectbox(
    "テーブルを選択",
    options=tables,
)

# 選択されたテーブルの構造を表示
if selected_table:
    schema_df = get_table_schema(selected_db, selected_schema, selected_table)
    if schema_df is not None:
        st.sidebar.subheader("テーブル構造")
        st.sidebar.dataframe(schema_df)

# デフォルトのSQLクエリテンプレート
default_query = f"""
SELECT *
FROM {selected_db}.{selected_schema}.{selected_table}
LIMIT 100
"""

# SQLクエリの入力
sql_query = st.text_area(
    "SQLクエリを入力",
    value=default_query,
    height=200,
)

# クエリ実行ボタン
if st.button("クエリを実行"):
    if sql_query:
        display_sql(sql_query)
    else:
        st.warning("SQLクエリを入力してください。") 