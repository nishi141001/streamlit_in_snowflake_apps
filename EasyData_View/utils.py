import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session
import snowflake.connector
import _snowflake
from datetime import datetime
import io

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

def render_download_section(data: pd.DataFrame, table_name: str = "data") -> None:
    """
    データのダウンロードセクションを表示する関数（リロード最小化版）
    
    Args:
        data (pd.DataFrame): ダウンロード対象のデータ
        table_name (str): ファイル名に使用するテーブル名
    """
    try:
        col1, col2 = st.columns(2)
        
        with col1:
            # ファイル形式の選択（リロードなし）
            format_choice = st.selectbox(
                "ファイル形式",
                ["CSV", "Excel (XLSX)"],
                key=f"export_format_{table_name}"
            )
            
            # タイムスタンプの設定（リロードなし）
            timestamp_enabled = st.checkbox(
                "タイムスタンプ付きファイル名",
                value=True,
                key=f"add_timestamp_{table_name}"
            )

        with col2:
            # CSVの場合のみエンコーディング選択を表示
            if format_choice == "CSV":
                encoding_choice = st.selectbox(
                    "エンコーディング",
                    ["UTF-8", "Shift_JIS"],
                    key=f"encoding_{table_name}"
                )

        # ダウンロード処理
        if format_choice == "CSV":
            # CSV処理
            csv_encoding = 'utf-8-sig' if encoding_choice == "UTF-8" else 'shift_jis'
            csv_data = data.to_csv(index=False, encoding=csv_encoding)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if timestamp_enabled else ""
            filename = f"{table_name}_{timestamp}.csv" if timestamp else f"{table_name}.csv"
            
            st.download_button(
                label="📥 CSVダウンロード",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                use_container_width=True,
                key=f"csv_download_{table_name}"
            )
        else:
            # Excel処理
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                data.to_excel(writer, sheet_name='データ', index=False)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if timestamp_enabled else ""
            filename = f"{table_name}_{timestamp}.xlsx" if timestamp else f"{table_name}.xlsx"
            
            st.download_button(
                label="📥 Excelダウンロード",
                data=buffer.getvalue(),
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"excel_download_{table_name}"
            )
    
    except Exception as e:
        st.error(f"ダウンロード機能でエラーが発生しました: {str(e)}")

def display_sql_results(df: pd.DataFrame, query_name: str = "query") -> None:
    """
    SQLクエリの結果を表示し、ダウンロード機能を提供する関数
    
    Args:
        df (pd.DataFrame): 表示するデータ
        query_name (str): クエリの識別名（ファイル名に使用）
    """
    if len(df.index) > 0:
        data_tab, line_tab, bar_tab, download_tab = st.tabs(["Data", "Line Chart", "Bar Chart", "Download"])
        
        with data_tab:
            st.dataframe(df)
        
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
        
        with download_tab:
            render_download_section(df, query_name)
    else:
        st.info("クエリは正常に実行されましたが、結果は空です。")

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
                    # ユニークなキーを生成してリロードを防止
                    button_key = f"suggestion_{message_index}_{suggestion_index}_{hash(suggestion) % 10000}"
                    if st.button(suggestion, key=button_key):
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
                        display_sql_results(df, f"query_{message_index}")
                    except Exception as e:
                        st.error(f"SQLクエリの実行中にエラーが発生しました: {e}")