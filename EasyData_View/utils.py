import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session
import snowflake.connector
import _snowflake

# ---------------------------------------------------
# Snowflake接続・データ取得用関数群
# ---------------------------------------------------
@st.cache_resource
def get_snowpark_session():
    """
    SnowflakeのSnowparkセッションを取得する関数
    ※Snowflake環境下で実行される前提です
    """
    try:
        session = get_active_session()
        return session
    except Exception as e:
        st.error(f"Snowparkセッションの取得に失敗しました: {e}")
        return None

@st.cache_resource
def get_snowflake_connection():
    """
    Snowflakeコネクションを取得する関数
    ※SnowparkセッションからSnowflake接続情報を構築します
    """
    try:
        session = get_snowpark_session()
        if session:
            conn = snowflake.connector.connect(
                user=session.get_current_account(),
                account=session.get_current_account(),
                session_id=session.get_session_id()
            )
            return conn
        else:
            return None
    except Exception as e:
        st.error(f"Snowflake接続の取得に失敗しました: {e}")
        return None

@st.cache_data
def get_available_databases():
    """
    利用可能なデータベース一覧を取得する関数
    ※SQLのSHOW DATABASESコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]

@st.cache_data
def get_available_schemas(database):
    """
    指定したデータベース内のスキーマ一覧を取得する関数
    ※SQLのSHOW SCHEMASコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    result = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row["name"] for row in result]

@st.cache_data
def get_available_tables(database, schema):
    """
    指定したデータベースとスキーマ内のテーブル一覧を取得する関数
    ※SQLのSHOW TABLESコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
    return [row["name"] for row in result]

@st.cache_data
def get_available_stages(database, schema):
    """
    指定したデータベースとスキーマ内のステージ一覧を取得する関数
    ※SQLのSHOW STAGESコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    result = session.sql(f"SHOW STAGES IN {database}.{schema}").collect()
    return [row["name"] for row in result]

@st.cache_data
def get_files_in_stage(database, schema, stage):
    """
    指定したステージ内のファイル一覧を取得する関数
    ※LISTコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    try:
        result = session.sql(f"LIST @{database}.{schema}.{stage}").collect()
        return [row["name"] for row in result]
    except Exception as e:
        st.error(f"ステージ内のファイル取得に失敗しました: {e}")
        return []

@st.cache_data
def get_table_schema(database, schema, table):
    """
    指定したテーブルのカラム情報（スキーマ）を取得する関数
    ※DESCRIBE TABLEコマンドを使用
    """
    session = get_snowpark_session()
    if not session:
        return []
    result = session.sql(f"DESCRIBE TABLE {database}.{schema}.{table}").collect()
    columns = []
    for row in result:
        columns.append({
            "name": row["name"],
            "type": row["type"],
            "nullable": row["null?"]
        })
    return columns

# ---------------------------------------------------
# SQLクエリ実行と結果表示のための関数
# ---------------------------------------------------
def display_sql(sql: str) -> None:
    """
    渡されたSQLクエリを実行し、実行結果のテーブル・グラフを表示する関数
    """
    # SQLクエリ内容を展開部に表示
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
    # 結果表示エリア
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            try:
                session = get_snowpark_session()
                if not session:
                    st.error("Snowparkセッションを利用できません")
                    return
                # SQL実行結果をPandas DataFrameに変換
                df = session.sql(sql).to_pandas()
                if len(df.index) > 0:
                    # 結果の表示方法をタブで切り替え（表、線グラフ、棒グラフ）
                    data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
                    data_tab.dataframe(df)
                    if len(df.columns) > 1:
                        # 最初のカラムをインデックスに設定
                        chart_df = df.set_index(df.columns[0])
                        # 数値型カラムのみを抽出してグラフ化
                        numeric_cols = chart_df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            numeric_df = chart_df[numeric_cols]
                            with line_tab:
                                st.line_chart(numeric_df)
                            with bar_tab:
                                st.bar_chart(numeric_df)
                        else:
                            with line_tab:
                                st.info("グラフを表示するには、数値型のカラムが必要です。")
                            with bar_tab:
                                st.info("グラフを表示するには、数値型のカラムが必要です。")
                    else:
                        st.dataframe(df)
                else:
                    st.info("クエリは正常に実行されましたが、結果は空です。")
            except Exception as e:
                st.error(f"SQLクエリの実行中にエラーが発生しました: {e}")

# ---------------------------------------------------
# Cortex Analyst API連携用関数群
# ---------------------------------------------------
def send_message(prompt: str, database: str, schema: str, stage: str, file: str) -> dict:
    """
    Cortex Analyst APIを呼び出して、指定したSemantic Modelファイルに基づく応答を取得する関数
    """
    # ファイルパスのデバッグ情報
    st.sidebar.write("デバッグ情報:")
    st.sidebar.write(f"データベース: {database}")
    st.sidebar.write(f"スキーマ: {schema}")
    st.sidebar.write(f"ステージ: {stage}")
    st.sidebar.write(f"ファイル: {file}")

    # ファイルパスの構築
    semantic_model_file = f"@{database}.{schema}.{stage}/{file}"
    st.sidebar.write(f"構築されたパス: {semantic_model_file}")

    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": semantic_model_file,
    }

    try:
        resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
        if resp["status"] < 400:
            return json.loads(resp["content"])
        else:
            st.sidebar.error(f"APIレスポンス: {resp}")
            raise Exception(f"Failed request with status {resp['status']}: {resp}")
    except Exception as e:
        st.error(f"Cortex Analyst APIの呼び出しに失敗しました: {e}")
        return None

def process_message(prompt: str, database: str, schema: str, stage: str, file: str) -> dict:
    """
    ユーザーの質問を処理し、応答を生成する関数
    戻り値: 生成された応答のコンテンツ（テキスト、SQL、提案を含む）
    """
    with st.spinner("Generating response..."):
        response = send_message(prompt=prompt, database=database, schema=schema, stage=stage, file=file)
        if response:
            return response["message"]["content"]
        else:
            st.error("応答の生成中にエラーが発生しました。")
            return [{"type": "text", "text": "応答の生成中にエラーが発生しました。"}]

def display_content(content: list, message_index: int = None) -> None:
    """
    Cortex Analystの応答内容（テキスト、提案、SQL）を適切に表示する関数
    """
    message_index = message_index or len(st.session_state.get("messages", []))
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("提案された質問", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("SQLクエリを実行中..."):
                    try:
                        session = get_snowpark_session()
                        if not session:
                            st.error("Snowparkセッションを利用できません")
                            return
                        df = session.sql(item["statement"]).to_pandas()
                        if len(df.index) > 0:
                            data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
                            data_tab.dataframe(df)
                            if len(df.columns) > 1:
                                chart_df = df.copy()
                                index_col = chart_df.columns[0]
                                chart_df = chart_df.set_index(index_col)
                                numeric_cols = chart_df.select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    numeric_df = chart_df[numeric_cols]
                                    with line_tab:
                                        st.line_chart(numeric_df)
                                    with bar_tab:
                                        st.bar_chart(numeric_df)
                                else:
                                    with line_tab:
                                        st.info("グラフを表示するには、数値型のカラムが必要です。")
                                    with bar_tab:
                                        st.info("グラフを表示するには、数値型のカラムが必要です。")
                            else:
                                with line_tab:
                                    st.info("グラフを表示するには、複数のカラムが必要です。")
                                with bar_tab:
                                    st.info("グラフを表示するには、複数のカラムが必要です。")
                        else:
                            st.info("クエリは正常に実行されましたが、結果は空です。")
                    except Exception as e:
                        st.error(f"SQLクエリの実行中にエラーが発生しました: {e}") 