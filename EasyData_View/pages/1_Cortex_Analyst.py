import streamlit as st
import pandas as pd
from utils import (
    get_snowpark_session,
    get_available_databases,
    get_available_schemas,
    get_available_stages,
    get_files_in_stage,
    process_message,
)

# ---------------------------------------------------
# Streamlitアプリの基本設定
# ---------------------------------------------------
st.set_page_config(
    page_title="Cortex Analyst - Snowflake Cortex Analyst",
    page_icon="📝",
    layout="wide",
)

# ===================================================
# Cortex Analystモードのメイン処理
# ---------------------------------------------------
st.title("📝 Cortex Analystモード")

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

# 選択されたスキーマのステージを取得
stages = get_available_stages(selected_db, selected_schema)
if not stages:
    st.warning(f"スキーマ {selected_schema} に利用可能なステージが見つかりませんでした。")
    st.stop()

# サイドバーでステージを選択
selected_stage = st.sidebar.selectbox(
    "ステージを選択",
    options=stages,
)

# 選択されたステージのファイルを取得
files = get_files_in_stage(selected_db, selected_schema, selected_stage)
if not files:
    st.warning(f"ステージ {selected_stage} にファイルが見つかりませんでした。")
    st.stop()

# YAMLファイルのみをフィルタリング
yaml_files = [f for f in files if f.endswith('.yaml')]
if not yaml_files:
    st.warning(f"ステージ {selected_stage} にYAMLファイルが見つかりませんでした。")
    st.stop()

# サイドバーでセマンティックモデルファイルを選択
selected_file = st.sidebar.selectbox(
    "セマンティックモデルファイルを選択",
    options=yaml_files,
)

# 選択されたファイル名を表示
st.info(f"選択されたセマンティックモデル: {selected_file}")

# チャット履歴の管理
if "messages" not in st.session_state:
    st.session_state.messages = []

# チャット履歴のクリアボタン
if st.sidebar.button("チャット履歴をクリア"):
    st.session_state.messages = []
    st.rerun()

# チャット履歴の表示
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# ユーザー入力の処理
if prompt := st.chat_input("質問を入力してください"):
    # ユーザーの質問を表示
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # アシスタントの応答を表示
    with st.chat_message("assistant"):
        response = process_message(prompt, selected_db, selected_schema, selected_stage, selected_file)
        st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})

# 提案された質問の表示
if st.session_state.messages:
    st.sidebar.subheader("提案された質問")
    for message in st.session_state.messages:
        if message["role"] == "assistant":
            # 提案された質問を抽出して表示
            if "提案された質問：" in message["content"]:
                suggestions = message["content"].split("提案された質問：")[1].split("\n")
                for suggestion in suggestions:
                    if suggestion.strip():
                        if st.sidebar.button(suggestion.strip()):
                            st.session_state.messages.append({"role": "user", "content": suggestion.strip()})
                            st.rerun() 