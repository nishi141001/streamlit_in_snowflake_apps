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

# サイドバーでデータベースを選択（リロード防止キー追加）
current_db = session.get_current_database()
try:
    default_db_index = databases.index(current_db) if current_db in databases else 0
except (ValueError, TypeError):
    default_db_index = 0

selected_db = st.sidebar.selectbox(
    "データベースを選択",
    options=databases,
    index=default_db_index,
    key="db_select_direct_sql"
)

# 選択されたデータベースのスキーマを取得
schemas = get_available_schemas(selected_db)
if not schemas:
    st.warning(f"データベース {selected_db} に利用可能なスキーマが見つかりませんでした。")
    st.stop()

# サイドバーでスキーマを選択（リロード防止キー追加）
current_schema = session.get_current_schema()
try:
    default_schema_index = schemas.index(current_schema) if current_schema in schemas else 0
except (ValueError, TypeError):
    default_schema_index = 0

selected_schema = st.sidebar.selectbox(
    "スキーマを選択",
    options=schemas,
    index=default_schema_index,
    key="schema_select_direct_sql"
)

# 選択されたスキーマのテーブルを取得
tables = get_available_tables(selected_db, selected_schema)
if not tables:
    st.warning(f"スキーマ {selected_schema} に利用可能なテーブルが見つかりませんでした。")
    st.stop()

# サイドバーでテーブルを選択（リロード防止キー追加）
selected_table = st.sidebar.selectbox(
    "テーブルを選択",
    options=tables,
    key="table_select_direct_sql"
)

# 選択されたテーブルの構造を表示
if selected_table:
    schema_df = get_table_schema(selected_db, selected_schema, selected_table)
    if schema_df is not None:
        st.sidebar.subheader("テーブル構造")
        st.sidebar.dataframe(schema_df)

# デフォルトのSQLクエリテンプレート
default_query = f"""SELECT *
FROM {selected_db}.{selected_schema}.{selected_table}
LIMIT 100"""

# SQLクエリの入力（リロード防止キー追加）
sql_query = st.text_area(
    "SQLクエリを入力",
    value=default_query,
    height=200,
    key="sql_query_input_direct_sql"
)

# クエリ実行ボタン（リロード防止キー追加）
if st.button("クエリを実行", key="execute_query_direct_sql"):
    if sql_query:
        # セッション状態にクエリ結果を保存
        with st.spinner("SQLクエリを実行中..."):
            try:
                session = get_snowpark_session()
                if not session:
                    st.error("Snowparkセッションを利用できません")
                else:
                    # SQL実行結果をPandas DataFrameに変換
                    df = session.sql(sql_query).to_pandas()
                    # セッション状態に結果を保存
                    st.session_state.query_result = df
                    st.session_state.executed_query = sql_query
                    st.session_state.query_executed = True
            except Exception as e:
                st.error(f"SQLクエリの実行中にエラーが発生しました: {e}")
                st.session_state.query_executed = False
    else:
        st.warning("SQLクエリを入力してください。")

# 結果表示（セッション状態から取得してタブ切り替え時のリロードを防止）
if st.session_state.get("query_executed", False) and "query_result" in st.session_state:
    df = st.session_state.query_result
    executed_query = st.session_state.get("executed_query", "")
    
    # SQLクエリ内容を展開部に表示
    with st.expander("実行されたSQL Query", expanded=False):
        st.code(executed_query, language="sql")
    
    # 結果表示エリア
    with st.expander("Results", expanded=True):
        # utils.pyの共通関数を使用
        from utils import display_sql_results
        display_sql_results(df, "direct_sql_query")