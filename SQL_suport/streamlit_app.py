import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

# ページ設定
st.set_page_config(
    page_title="SQLレスデータ抽出ツール",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッションの初期化
@st.cache_resource
def init_snowflake_session():
    """Snowflakeセッションを初期化"""
    try:
        # Streamlit in Snowflake環境では自動的にセッションを取得
        session = get_active_session()
        return session
    except Exception as e:
        st.error(f"Snowflakeセッションの初期化に失敗しました: {str(e)}")
        return None

# データベース・スキーマ・テーブル情報の取得
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_snowflake_metadata(_session):
    """Snowflakeのメタデータを取得"""
    try:
        # データベース一覧の取得
        databases = _session.sql("SHOW DATABASES").collect()
        db_list = [row['name'] for row in databases]
        
        # データベースごとのスキーマとテーブル情報を取得
        metadata = {}
        for db in db_list:
            try:
                # スキーマ一覧の取得（USE文を使用せずに）
                schemas = _session.sql(f"SHOW SCHEMAS IN DATABASE {db}").collect()
                
                db_metadata = {
                    "name": f"{db}",
                    "schemas": {}
                }
                
                for schema in schemas:
                    schema_name = schema['name']
                    try:
                        # テーブル一覧の取得（USE文を使用せずに）
                        tables = _session.sql(f"SHOW TABLES IN SCHEMA {db}.{schema_name}").collect()
                        
                        table_list = [row['name'] for row in tables]
                        if table_list:  # テーブルが存在する場合のみ追加
                            db_metadata["schemas"][schema_name] = table_list
                    except Exception as schema_error:
                        st.warning(f"スキーマ {db}.{schema_name} の取得に失敗: {str(schema_error)}")
                        continue
                
                if db_metadata["schemas"]:  # スキーマが存在する場合のみ追加
                    metadata[db] = db_metadata
            except Exception as db_error:
                st.warning(f"データベース {db} の取得に失敗: {str(db_error)}")
                continue
        
        return metadata
    except Exception as e:
        st.error(f"メタデータの取得に失敗しました: {str(e)}")
        return {}

# テーブルのスキーマ情報を取得
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
        
        # スキーマ情報を整形
        columns = []
        for col in schema_data:
            column_info = {
                "name": col['COLUMN_NAME'],
                "type": col['DATA_TYPE'],
                "sample": None  # サンプルデータは別途取得
            }
            columns.append(column_info)
        
        return columns
    except Exception as e:
        st.warning(f"テーブルスキーマの取得に失敗しました: {str(e)}")
        return []

# テーブルのサンプルデータを取得
@st.cache_data(ttl=3600)
def get_table_sample(_session, database, schema, table, limit=5):
    """テーブルのサンプルデータを取得"""
    try:
        query = f"SELECT * FROM {database}.{schema}.{table} LIMIT {limit}"
        sample_data = _session.sql(query).collect()
        
        if sample_data:
            # 最初のレコードをサンプルとして使用
            sample = sample_data[0]
            return {col: str(sample[col]) for col in sample.keys()}
        return None
    except Exception as e:
        st.warning(f"サンプルデータの取得に失敗しました: {str(e)}")
        return None

# カスタムCSS（モックと同じ）
st.markdown("""
<style>
/* 基本設定 */
.main > div {
    padding-top: 1rem;
}

/* ヘッダー */
.header-title {
    font-size: 2.2rem;
    font-weight: bold;
    background: linear-gradient(135deg, #63C0F6 0%, #1FAEFF 50%, #0C7EC5 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.5rem;
}

.header-subtitle {
    color: #475569;
    font-size: 1rem;
    margin-bottom: 1.5rem;
}

/* カード風レイアウト */
.card {
    background: white;
    border: 1px solid #DAF1FF;
    border-radius: 10px;
    padding: 1.5rem;
    margin: 1rem 0;
    box-shadow: 0 2px 10px rgba(99, 192, 246, 0.1);
    border-left: 4px solid #63C0F6;
}

.card-header {
    color: #1e40af;
    font-size: 1.2rem;
    font-weight: bold;
    margin-bottom: 1rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

/* 結果サマリー */
.result-summary {
    background: linear-gradient(135deg, #F6FAFE 0%, #DAF1FF 100%);
    border-radius: 10px;
    padding: 1.5rem;
    margin: 1rem 0;
    display: flex;
    justify-content: space-around;
    text-align: center;
}

.summary-item {
    flex: 1;
}

.summary-value {
    font-size: 1.8rem;
    font-weight: bold;
    color: #1e40af;
}

.summary-label {
    color: #475569;
    font-size: 0.9rem;
}

/* ボタンスタイル */
.stButton > button {
    background: linear-gradient(135deg, #63C0F6 0%, #1FAEFF 100%);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 0.75rem 2rem;
    font-weight: bold;
    transition: all 0.3s;
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 15px rgba(99, 192, 246, 0.3);
}

/* エラーメッセージ */
.error-message {
    background: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 8px;
    padding: 1rem;
    margin: 0.5rem 0;
    color: #dc2626;
    font-size: 0.9rem;
}

/* 警告メッセージ */
.warning-message {
    background: #fffbeb;
    border: 1px solid #fde68a;
    border-radius: 8px;
    padding: 1rem;
    margin: 0.5rem 0;
    color: #d97706;
    font-size: 0.9rem;
}

/* 実行ボタン */
.execute-button {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%) !important;
}

/* セレクトボックス */
.stSelectbox > div > div {
    border-radius: 8px;
    border: 2px solid #DAF1FF;
}

.stSelectbox > div > div:focus-within {
    border-color: #63C0F6;
    box-shadow: 0 0 0 2px rgba(99, 192, 246, 0.1);
}

/* タブ */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
}

.stTabs [data-baseweb="tab"] {
    background: #F6FAFE;
    border-radius: 8px 8px 0 0;
    color: #475569;
    font-weight: 500;
}

.stTabs [aria-selected="true"] {
    background: white;
    color: #1e40af;
    border-bottom: 3px solid #63C0F6;
}

/* データフレーム */
.stDataFrame {
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid #DAF1FF;
}

/* サイドバー */
.css-1d391kg {
    background: linear-gradient(180deg, #F6FAFE 0%, #DAF1FF 100%);
}

/* 選択状況表示 */
.selection-status {
    background: white;
    border: 1px solid #DAF1FF;
    border-radius: 8px;
    padding: 1rem;
    margin: 0.5rem 0;
}

.selection-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0.3rem 0;
    font-size: 0.9rem;
}

.selection-item.selected {
    color: #1e40af;
    font-weight: 500;
}

.selection-item.unselected {
    color: #94a3b8;
}

/* 保存済み設定 */
.saved-config {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 0.75rem;
    margin: 0.25rem 0;
    cursor: pointer;
    transition: all 0.2s;
}

.saved-config:hover {
    background: #e2e8f0;
    border-color: #63C0F6;
}

.saved-config.selected {
    background: #DAF1FF;
    border-color: #63C0F6;
}

/* JOIN表示 */
.join-preview {
    background: #f1f5f9;
    border-left: 4px solid #1FAEFF;
    padding: 1rem;
    border-radius: 0 8px 8px 0;
    margin: 0.5rem 0;
    font-family: monospace;
    font-size: 0.9rem;
    color: #334155;
}

/* 区切り線 */
.divider {
    border-top: 2px solid #DAF1FF;
    margin: 2rem 0;
}

/* テーブル構造カード */
.table-structure-card {
    background: white;
    border: 1px solid #DAF1FF;
    border-radius: 10px;
    padding: 1rem;
    margin: 0.5rem 0;
    box-shadow: 0 2px 10px rgba(99, 192, 246, 0.1);
}

.table-structure-header {
    color: #1e40af;
    font-size: 1.1rem;
    font-weight: bold;
    margin-bottom: 0.8rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
</style>
""", unsafe_allow_html=True)

# セッション状態の初期化
def init_session_state():
    """セッション状態を初期化"""
    default_values = {
        'selected_db': None,
        'selected_schema': None,
        'selected_table': None,
        'join_conditions': [],  # 複数のJOINに対応
        'query_conditions': {},
        'result_data': None,
        'query_executed': False,
        'execution_time': 0,
        'saved_configs': {},
        'filter_conditions': [],
        'last_error': None,  # エラー情報を保存
        'query_validation_errors': []  # クエリ検証エラー
    }
    
    for key, value in default_values.items():
        if key not in st.session_state:
            st.session_state[key] = value

# セッション状態の初期化を実行
init_session_state()

# Snowflakeセッションの初期化
session = init_snowflake_session()
if session is None:
    st.error("Snowflakeセッションの初期化に失敗しました。アプリケーションを再起動してください。")
    st.stop()

# メタデータの取得
snowflake_metadata = get_snowflake_metadata(session)
if not snowflake_metadata:
    st.error("Snowflakeのメタデータの取得に失敗しました。")
    st.stop()

def load_saved_config(config_name):
    """保存済み設定を読み込み"""
    try:
        if config_name in st.session_state.saved_configs:
            config = st.session_state.saved_configs[config_name]
            st.session_state.selected_db = config["db"]
            st.session_state.selected_schema = config["schema"]
            st.session_state.selected_table = config["table"]
            st.session_state.query_conditions = config["conditions"]
            st.session_state.join_conditions = config.get("join_conditions", [])
            st.session_state.filter_conditions = config.get("filter_conditions", [])
            st.session_state.last_error = None
            st.session_state.query_validation_errors = []
            st.rerun()
    except Exception as e:
        st.error(f"設定の読み込みに失敗しました: {str(e)}")

def save_current_config():
    """現在の設定を保存"""
    if st.session_state.selected_table:
        config_name = st.text_input("設定名を入力", key="new_config_name")
        description = st.text_input("説明（オプション）", key="new_config_desc")
        
        if st.button("💾 設定を保存"):
            if config_name:
                st.session_state.saved_configs[config_name] = {
                    "db": st.session_state.selected_db,
                    "schema": st.session_state.selected_schema,
                    "table": st.session_state.selected_table,
                    "description": description,
                    "conditions": st.session_state.query_conditions,
                    "join_conditions": st.session_state.join_conditions,
                    "filter_conditions": st.session_state.filter_conditions
                }
                st.success(f"設定「{config_name}」を保存しました")
                st.rerun()
            else:
                st.error("設定名を入力してください")

def validate_table_columns(database, schema, table):
    """テーブルのカラム情報を検証"""
    try:
        schema_data = get_table_schema(session, database, schema, table)
        if not schema_data:
            return False, f"テーブル {table} のスキーマ情報を取得できません"
        return True, schema_data
    except Exception as e:
        return False, f"テーブル検証エラー: {str(e)}"

def get_dynamic_columns(table_name, database, schema):
    """テーブルに応じた動的なカラム情報を取得（エラーハンドリング強化）"""
    try:
        # まずテーブルの存在を確認
        is_valid, schema_data = validate_table_columns(database, schema, table_name)
        if not is_valid:
            st.warning(f"テーブル {table_name} の情報取得に失敗: {schema_data}")
            return {}
        
        if not schema_data:
            return {}
        
        columns = {}
        for col in schema_data:
            col_name = col["name"]
            col_type = col["type"]
            
            # データ型に応じた選択肢を設定
            if col_type in ['VARCHAR', 'CHAR', 'STRING', 'TEXT']:
                # カテゴリや地域などの列名パターンに基づいて選択肢を設定
                try:
                    if any(keyword in col_name.lower() for keyword in ["category", "region", "status", "type"]):
                        # 実際のデータから一意の値を取得（制限付き）
                        query = f"""
                        SELECT DISTINCT {col_name}
                        FROM {database}.{schema}.{table_name}
                        WHERE {col_name} IS NOT NULL
                        LIMIT 50
                        """
                        distinct_values = session.sql(query).collect()
                        columns[col_name] = [str(row[col_name]) for row in distinct_values if row[col_name] is not None]
                    else:
                        columns[col_name] = []  # 自由入力
                except Exception as e:
                    st.warning(f"カラム {col_name} の選択肢取得に失敗: {str(e)}")
                    columns[col_name] = []  # 自由入力にフォールバック
            elif col_type in ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ']:
                columns[col_name] = "date_range"
            elif col_type in ['NUMBER', 'DECIMAL', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE']:
                columns[col_name] = "numeric_range"
            else:
                columns[col_name] = []  # 自由入力
        
        return columns
    except Exception as e:
        st.warning(f"カラム情報の取得に失敗しました: {str(e)}")
        return {}

def render_dynamic_filters():
    """選択されたテーブルに応じた動的フィルターを表示"""
    if not st.session_state.selected_table:
        st.info("テーブルを選択すると、そのテーブルに応じた絞り込み条件が表示されます")
        return
    
    st.markdown("### 🔍 絞り込み条件")
    
    # テーブルの動的カラム情報を取得
    dynamic_columns = get_dynamic_columns(
        st.session_state.selected_table,
        st.session_state.selected_db,
        st.session_state.selected_schema
    )
    
    if not dynamic_columns:
        st.warning("カラム情報を取得できませんでした。テーブルの選択を確認してください。")
        return
    
    # 新しい条件を追加するセクション
    with st.expander("➕ 条件を追加", expanded=False):
        selected_column = st.selectbox(
            "カラムを選択",
            list(dynamic_columns.keys()),
            key="new_filter_column"
        )
        
        if selected_column:
            condition_type = st.selectbox(
                "条件タイプ",
                ["値を選択", "範囲指定", "カスタム条件"],
                key="new_filter_type"
            )
        
        if selected_column and st.button("条件を追加", key="add_filter"):
            st.session_state.filter_conditions.append({
                "column": selected_column,
                "type": condition_type
            })
            st.rerun()
    
    # 既存の条件を表示・編集
    conditions = {}
    for i, condition in enumerate(st.session_state.filter_conditions):
        col_name = condition["column"]
        
        # カラムが現在のテーブルに存在するかチェック
        if col_name not in dynamic_columns:
            st.warning(f"カラム '{col_name}' は現在のテーブルに存在しません。条件を削除することをお勧めします。")
            col_config = []  # 安全なフォールバック
        else:
            col_config = dynamic_columns[col_name]
        
        with st.expander(f"🔧 {col_name}", expanded=True):
            # 条件タイプ選択
            condition_type = st.selectbox(
                "条件タイプ",
                ["値を選択", "範囲指定", "カスタム条件"],
                index=["値を選択", "範囲指定", "カスタム条件"].index(condition["type"]),
                key=f"condition_type_{i}"
            )
            
            try:
                if condition_type == "値を選択":
                    if isinstance(col_config, list) and len(col_config) > 0:
                        # 定義済みの選択肢がある場合
                        selected_values = st.multiselect(
                            f"{col_name}の値",
                            col_config,
                            key=f"select_{i}"
                        )
                        if selected_values:
                            conditions[f"{col_name}_in"] = selected_values
                    else:
                        # 自由入力の選択肢
                        input_values = st.text_input(
                            f"{col_name}の値（カンマ区切り）",
                            placeholder="例: 値1, 値2, 値3",
                            key=f"input_{i}"
                        )
                        if input_values:
                            values_list = [v.strip() for v in input_values.split(",") if v.strip()]
                            conditions[f"{col_name}_in"] = values_list
                
                elif condition_type == "範囲指定":
                    if col_config == "date_range":
                        # 日付範囲
                        date_from = st.date_input(f"{col_name} 開始", key=f"date_from_{i}")
                        date_to = st.date_input(f"{col_name} 終了", key=f"date_to_{i}")
                        
                        if date_from or date_to:
                            conditions[f"{col_name}_range"] = {"from": date_from, "to": date_to}
                    
                    elif col_config == "numeric_range":
                        # 数値範囲
                        min_val = st.number_input(f"{col_name} 最小値", key=f"min_{i}")
                        max_val = st.number_input(f"{col_name} 最大値", key=f"max_{i}")
                        
                        if min_val != 0 or max_val != 0:
                            conditions[f"{col_name}_range"] = {"min": min_val, "max": max_val}
                    else:
                        # 文字列の範囲（前方一致、後方一致など）
                        range_type = st.selectbox(
                            "範囲タイプ",
                            ["前方一致", "後方一致", "部分一致"],
                            key=f"range_type_{i}"
                        )
                        range_value = st.text_input(
                            f"検索文字列",
                            key=f"range_value_{i}"
                        )
                        if range_value:
                            conditions[f"{col_name}_like"] = {"type": range_type, "value": range_value}
                
                elif condition_type == "カスタム条件":
                    # SQL条件の直接入力
                    custom_condition = st.text_area(
                        f"{col_name}のカスタム条件",
                        placeholder=f"例: {col_name} > 1000 OR {col_name} IS NULL",
                        key=f"custom_{i}"
                    )
                    if custom_condition:
                        conditions[f"{col_name}_custom"] = custom_condition
            
            except Exception as e:
                st.error(f"条件設定エラー: {str(e)}")
            
            # 削除ボタン
            if st.button("🗑️ 条件を削除", key=f"delete_{i}"):
                st.session_state.filter_conditions.pop(i)
                st.rerun()
    
    # 集計設定
    with st.expander("📊 集計設定"):
        # 利用可能なカラムからグループ化対象を動的生成
        available_group_columns = list(dynamic_columns.keys())
        
        # JOIN設定がある場合は結合テーブルのカラムも追加
        if st.session_state.join_conditions:
            for join_info in st.session_state.join_conditions:
                join_table = join_info["table"]
                try:
                    join_columns = get_dynamic_columns(
                        join_table,
                        st.session_state.selected_db,
                        st.session_state.selected_schema
                    )
                    for col in join_columns.keys():
                        available_group_columns.append(f"{join_table}.{col}")
                except Exception as e:
                    st.warning(f"結合テーブル {join_table} のカラム情報取得に失敗: {str(e)}")
        
        group_by_columns = st.multiselect(
            "グループ化するカラム",
            available_group_columns,
            key="group_by"
        )
        
        # ソート設定
        sort_column = st.selectbox(
            "ソートカラム",
            ["指定しない"] + available_group_columns,
            key="sort_column"
        )
        
        sort_order = "DESC"
        if sort_column != "指定しない":
            sort_order = st.selectbox(
                "ソート順",
                ["DESC (降順)", "ASC (昇順)"],
                key="sort_order"
            ).split()[0]
        
        limit_rows = st.number_input(
            "取得件数制限",
            min_value=1,
            value=1000,
            key="limit_rows"
        )
        
        conditions["group_by"] = group_by_columns
        conditions["sort_column"] = sort_column if sort_column != "指定しない" else None
        conditions["sort_order"] = sort_order
        conditions["limit_rows"] = limit_rows
    
    st.session_state.query_conditions = conditions

def render_join_config():
    """JOIN設定を表示（エラーハンドリング強化）"""
    st.markdown("### 🔗 テーブル結合")
    
    if not st.session_state.selected_table:
        st.info("メインテーブルを選択してください")
        return
    
    # 結合可能なテーブル一覧
    available_tables = []
    try:
        if st.session_state.selected_db and st.session_state.selected_schema:
            schema_tables = snowflake_metadata.get(st.session_state.selected_db, {}).get("schemas", {}).get(st.session_state.selected_schema, [])
            available_tables = [t for t in schema_tables if t != st.session_state.selected_table]
    except Exception as e:
        st.warning(f"利用可能なテーブル一覧の取得に失敗: {str(e)}")
    
    if not available_tables:
        st.info("結合可能なテーブルがありません")
        return
    
    # 新しいJOINを追加するセクション
    with st.expander("➕ JOINを追加", expanded=False):
        join_table = st.selectbox(
            "結合するテーブル",
            available_tables,
            key="new_join_table"
        )
        
        if join_table:
            join_type = st.selectbox(
                "結合タイプ", 
                ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
                key="new_join_type"
            )
            
            # カラム選択（エラーハンドリング付き）
            try:
                left_table_cols = list(get_dynamic_columns(
                    st.session_state.selected_table,
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                ).keys())
                
                right_table_cols = list(get_dynamic_columns(
                    join_table,
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                ).keys())
                
                if not left_table_cols or not right_table_cols:
                    st.warning("カラム情報の取得に失敗しました。テーブルを確認してください。")
                else:
                    left_column = st.selectbox(
                        f"{st.session_state.selected_table} のカラム", 
                        left_table_cols,
                        key="new_left_column"
                    )
                    
                    right_column = st.selectbox(
                        f"{join_table} のカラム", 
                        right_table_cols,
                        key="new_right_column"
                    )
                    
                    if st.button("JOINを追加", key="add_join"):
                        new_join = {
                            "table": join_table,
                            "type": join_type,
                            "left_col": left_column,
                            "right_col": right_column
                        }
                        st.session_state.join_conditions.append(new_join)
                        st.success(f"JOIN設定を追加しました: {join_type} {join_table}")
                        st.rerun()
            
            except Exception as e:
                st.error(f"JOIN設定でエラーが発生しました: {str(e)}")
                st.info("テーブルの選択を確認してください。")
    
    # 既存のJOIN設定を表示
    for i, join_info in enumerate(st.session_state.join_conditions):
        with st.expander(f"🔗 {join_info['table']} ({join_info['type']})", expanded=True):
            try:
                # 結合タイプ
                join_type = st.selectbox(
                    "結合タイプ", 
                    ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
                    index=["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"].index(join_info['type']),
                    key=f"join_type_{i}"
                )
                
                # カラム選択（エラーハンドリング付き）
                left_table_cols = list(get_dynamic_columns(
                    st.session_state.selected_table,
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                ).keys())
                
                right_table_cols = list(get_dynamic_columns(
                    join_info['table'],
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                ).keys())
                
                if not left_table_cols or not right_table_cols:
                    st.warning("カラム情報の取得に失敗しました。この結合設定を削除することをお勧めします。")
                    left_column = join_info.get('left_col', 'ERROR')
                    right_column = join_info.get('right_col', 'ERROR')
                else:
                    # 現在の選択値のインデックスを安全に取得
                    left_index = 0
                    if join_info.get('left_col') and join_info['left_col'] in left_table_cols:
                        left_index = left_table_cols.index(join_info['left_col'])
                    
                    right_index = 0
                    if join_info.get('right_col') and join_info['right_col'] in right_table_cols:
                        right_index = right_table_cols.index(join_info['right_col'])
                    
                    left_column = st.selectbox(
                        f"{st.session_state.selected_table} のカラム", 
                        left_table_cols,
                        index=left_index,
                        key=f"left_column_{i}"
                    )
                    
                    right_column = st.selectbox(
                        f"{join_info['table']} のカラム", 
                        right_table_cols,
                        index=right_index,
                        key=f"right_column_{i}"
                    )
                
                # JOIN条件を更新
                st.session_state.join_conditions[i].update({
                    "type": join_type,
                    "left_col": left_column,
                    "right_col": right_column
                })
                
                # SQL プレビュー
                join_sql = f"""
{join_type} {st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']} 
  ON {st.session_state.selected_table}.{left_column} = {join_info['table']}.{right_column}
                """.strip()
                
                st.markdown("**生成されるJOIN句:**")
                st.code(join_sql, language="sql")
                
            except Exception as e:
                st.error(f"JOIN設定 {i+1} でエラーが発生: {str(e)}")
                st.info("この結合設定に問題があります。削除して再作成することをお勧めします。")
            
            # 削除ボタン
            if st.button("🗑️ JOINを削除", key=f"delete_join_{i}"):
                st.session_state.join_conditions.pop(i)
                st.success("JOIN設定を削除しました")
                st.rerun()

def validate_query_before_execution():
    """クエリ実行前の検証"""
    errors = []
    warnings = []
    
    # 基本的な設定チェック
    if not st.session_state.selected_table:
        errors.append("メインテーブルが選択されていません")
    
    # JOIN設定の検証
    for i, join_info in enumerate(st.session_state.join_conditions):
        if not join_info.get('table'):
            errors.append(f"JOIN {i+1}: 結合テーブルが指定されていません")
        if not join_info.get('left_col') or not join_info.get('right_col'):
            errors.append(f"JOIN {i+1}: 結合カラムが指定されていません")
        
        # テーブルとカラムの存在確認
        try:
            # 結合テーブルのカラム情報を取得してみる
            join_columns = get_dynamic_columns(
                join_info['table'],
                st.session_state.selected_db,
                st.session_state.selected_schema
            )
            if not join_columns:
                warnings.append(f"JOIN {i+1}: テーブル {join_info['table']} のカラム情報を取得できません")
            elif join_info.get('right_col') and join_info['right_col'] not in join_columns:
                errors.append(f"JOIN {i+1}: カラム {join_info['right_col']} がテーブル {join_info['table']} に存在しません")
        except Exception as e:
            warnings.append(f"JOIN {i+1}: テーブル検証中にエラー - {str(e)}")
    
    # フィルター条件の検証
    main_columns = get_dynamic_columns(
        st.session_state.selected_table,
        st.session_state.selected_db,
        st.session_state.selected_schema
    )
    
    for condition in st.session_state.filter_conditions:
        col_name = condition.get('column')
        if col_name and col_name not in main_columns:
            # JOIN先のテーブルのカラムかもしれないので、警告レベルに
            warnings.append(f"フィルター条件: カラム '{col_name}' がメインテーブルに見つかりません")
    
    return errors, warnings

def generate_sql_query():
    """SQLクエリを生成（エラーハンドリング強化）"""
    try:
        # ベースクエリ
        base_table = f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
        
        # SELECT句（GROUP BYがある場合は集計関数を使用）
        group_by_cols = st.session_state.query_conditions.get('group_by', [])
        if group_by_cols:
            select_cols = group_by_cols + ["COUNT(*) as record_count"]
            # 数値カラムの合計を追加
            try:
                numeric_cols = [col for col in get_dynamic_columns(
                    st.session_state.selected_table,
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                ).keys() if get_dynamic_columns(
                    st.session_state.selected_table,
                    st.session_state.selected_db,
                    st.session_state.selected_schema
                )[col] == "numeric_range"]
                
                for col in numeric_cols:
                    select_cols.append(f"SUM({col}) as {col}_total")
            except Exception as e:
                st.warning(f"数値カラムの検出に失敗: {str(e)}")
            
            sql_parts = [f"SELECT {', '.join(select_cols)}"]
        else:
            sql_parts = [f"SELECT *"]
        
        # FROM句とJOIN
        sql_parts.append(f"FROM {base_table}")
        
        # 複数のJOINを処理
        if st.session_state.join_conditions:
            for join_info in st.session_state.join_conditions:
                join_table = f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']}"
                sql_parts.append(f"{join_info['type']} {join_table}")
                sql_parts.append(f"  ON {st.session_state.selected_table}.{join_info['left_col']} = {join_info['table']}.{join_info['right_col']}")
        
        # WHERE句
        where_conditions = []
        
        for key, value in st.session_state.query_conditions.items():
            if not value or key in ['group_by', 'sort_column', 'sort_order', 'limit_rows']:
                continue
            
            try:
                # IN条件
                if key.endswith('_in'):
                    col_name = key.replace('_in', '')
                    if isinstance(value, list) and value:
                        # SQLインジェクション対策：シングルクォートをエスケープ
                        escaped_values = [str(v).replace("'", "''") for v in value]
                        values_str = "', '".join(escaped_values)
                        where_conditions.append(f"{col_name} IN ('{values_str}')")
                
                # 範囲条件
                elif key.endswith('_range'):
                    col_name = key.replace('_range', '')
                    if isinstance(value, dict):
                        if 'from' in value and value['from']:
                            where_conditions.append(f"{col_name} >= '{value['from']}'")
                        if 'to' in value and value['to']:
                            where_conditions.append(f"{col_name} <= '{value['to']}'")
                        if 'min' in value and value['min'] != 0:
                            where_conditions.append(f"{col_name} >= {value['min']}")
                        if 'max' in value and value['max'] != 0:
                            where_conditions.append(f"{col_name} <= {value['max']}")
                
                # LIKE条件
                elif key.endswith('_like'):
                    col_name = key.replace('_like', '')
                    if isinstance(value, dict) and 'value' in value:
                        search_value = str(value['value']).replace("'", "''")  # エスケープ
                        like_type = value['type']
                        
                        if like_type == "前方一致":
                            where_conditions.append(f"{col_name} LIKE '{search_value}%'")
                        elif like_type == "後方一致":
                            where_conditions.append(f"{col_name} LIKE '%{search_value}'")
                        elif like_type == "部分一致":
                            where_conditions.append(f"{col_name} LIKE '%{search_value}%'")
                
                # カスタム条件
                elif key.endswith('_custom'):
                    # カスタム条件は基本的な検証のみ
                    if value and isinstance(value, str):
                        where_conditions.append(f"({value})")
            
            except Exception as e:
                st.warning(f"条件 {key} の処理中にエラー: {str(e)}")
        
        if where_conditions:
            sql_parts.append("WHERE " + "\n  AND ".join(where_conditions))
        
        # GROUP BY
        if group_by_cols:
            sql_parts.append(f"GROUP BY {', '.join(group_by_cols)}")
        
        # ORDER BY
        if st.session_state.query_conditions.get('sort_column'):
            sort_col = st.session_state.query_conditions['sort_column']
            sort_order = st.session_state.query_conditions.get('sort_order', 'DESC')
            sql_parts.append(f"ORDER BY {sort_col} {sort_order}")
        
        # LIMIT
        limit_val = st.session_state.query_conditions.get('limit_rows', 1000)
        if limit_val and limit_val < 10000:
            sql_parts.append(f"LIMIT {limit_val}")
        
        return "\n".join(sql_parts)
    
    except Exception as e:
        st.error(f"SQLクエリの生成に失敗しました: {str(e)}")
        return None

def execute_query():
    """クエリを実行して結果を取得（エラーハンドリング強化）"""
    try:
        # クエリ実行前の検証
        errors, warnings = validate_query_before_execution()
        
        if errors:
            st.error("以下のエラーを修正してください：")
            for error in errors:
                st.error(f"• {error}")
            return None, None
        
        if warnings:
            st.warning("以下の警告があります：")
            for warning in warnings:
                st.warning(f"• {warning}")
        
        query = generate_sql_query()
        if not query:
            return None, None
        
        start_time = time.time()
        
        # クエリ実行
        result = session.sql(query).collect()
        
        # 実行時間の計算
        execution_time = time.time() - start_time
        
        # 結果をDataFrameに変換
        if result:
            df = pd.DataFrame(result)
            st.session_state.last_error = None  # エラーをクリア
            return df, execution_time
        return pd.DataFrame(), execution_time
    
    except Exception as e:
        error_msg = str(e)
        st.session_state.last_error = error_msg
        st.error(f"クエリの実行に失敗しました: {error_msg}")
        
        # エラーの詳細情報を表示
        with st.expander("🔍 エラーの詳細情報", expanded=False):
            st.text(error_msg)
            
        # 生成されたクエリを表示
        try:
            query = generate_sql_query()
            if query:
                with st.expander("📝 実行されたSQL", expanded=False):
                    st.code(query, language="sql")
        except:
            pass
        
        return None, None

def render_charts(data):
    """グラフ表示"""
    if len(data) == 0:
        st.warning("表示するデータがありません")
        return
    
    try:
        # 日付カラムの特定
        date_columns = [col for col in data.columns if any(date_type in str(data[col].dtype).lower() 
                                                         for date_type in ['date', 'timestamp'])]
        
        # 数値カラムの特定
        numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
        
        # カテゴリカラムの特定（文字列型で一意の値が少ないもの）
        category_columns = []
        for col in data.select_dtypes(include=['object']).columns:
            if data[col].nunique() < 20:  # 一意の値が20未満のカラムをカテゴリとして扱う
                category_columns.append(col)
        
        # 時系列グラフ
        if date_columns and numeric_columns:
            st.subheader("📈 時系列推移")
            date_col = st.selectbox("日付カラム", date_columns, key="chart_date")
            value_col = st.selectbox("値カラム", numeric_columns, key="chart_value")
            
            if date_col and value_col:
                # 日付をdatetimeに変換
                chart_data = data.copy()
                chart_data[date_col] = pd.to_datetime(chart_data[date_col])
                
                # 時系列グラフ
                fig_line = px.line(chart_data, x=date_col, y=value_col, title=f"{date_col}別{value_col}推移")
                fig_line.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    title_font_color="#1e40af",
                    font_color="#475569"
                )
                fig_line.update_traces(line_color="#1FAEFF")
                st.plotly_chart(fig_line, use_container_width=True)
        
        # カテゴリ別分析
        if category_columns and numeric_columns:
            st.subheader("📊 カテゴリ別分析")
            col_a, col_b = st.columns(2)
            
            with col_a:
                category_col = st.selectbox("カテゴリカラム", category_columns, key="chart_category")
                value_col = st.selectbox("値カラム", numeric_columns, key="chart_category_value")
                
                if category_col and value_col:
                    # 棒グラフ
                    category_data = data.groupby(category_col)[value_col].sum().reset_index()
                    fig_bar = px.bar(category_data, x=category_col, y=value_col)
                    fig_bar.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        title_font_color="#1e40af",
                        font_color="#475569",
                        showlegend=False
                    )
                    fig_bar.update_traces(marker_color="#63C0F6")
                    st.plotly_chart(fig_bar, use_container_width=True)
            
            with col_b:
                if category_col and value_col:
                    # 円グラフ
                    fig_pie = px.pie(
                        category_data, 
                        values=value_col, 
                        names=category_col, 
                        hole=0.4
                    )
                    fig_pie.update_layout(
                        title_font_color="#1e40af",
                        font_color="#475569",
                        showlegend=True
                    )
                    fig_pie.update_traces(
                        textposition='inside', 
                        textinfo='percent+label',
                        marker_colors=["#63C0F6", "#1FAEFF", "#0C7EC5", "#A9DFFF"]
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
    
    except Exception as e:
        st.error(f"グラフの生成に失敗しました: {str(e)}")

def render_download_section(data):
    """ダウンロードセクション"""
    st.subheader("💾 データエクスポート")
    
    try:
        # エクスポート設定
        col1, col2 = st.columns(2)
        with col1:
            export_format = st.selectbox("ファイル形式", ["CSV", "Excel (XLSX)"], key="export_format")
            add_timestamp = st.checkbox("タイムスタンプ付きファイル名", value=True, key="add_timestamp")
        
        with col2:
            if export_format == "CSV":
                encoding = st.selectbox("エンコーディング", ["UTF-8", "Shift_JIS"], key="encoding")
            else:
                include_charts = st.checkbox("サマリーシートを含める", key="include_charts")
        
        # ダウンロードボタン
        st.markdown("### ダウンロード")
        
        if export_format == "CSV":
            csv_encoding = 'utf-8-sig' if encoding == "UTF-8" else 'shift_jis'
            csv_data = data.to_csv(index=False, encoding=csv_encoding)
            
            table_name = st.session_state.selected_table or "data"
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if add_timestamp else ""
            filename = f"{table_name}_{timestamp}.csv" if timestamp else f"{table_name}.csv"
            
            st.download_button(
                label="📥 CSVダウンロード",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                use_container_width=True
            )
        else:
            # Excel出力
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                data.to_excel(writer, sheet_name='データ', index=False)
                
                if include_charts:
                    # サマリーシート追加
                    summary_data = pd.DataFrame({
                        "項目": ["総レコード数", "データ抽出日時"],
                        "値": [len(data), datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
                    
                    # 数値カラムがある場合は統計情報も追加
                    numeric_cols = data.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        for col in numeric_cols:
                            summary_data = pd.concat([summary_data, pd.DataFrame({
                                "項目": [f"{col}_合計", f"{col}_平均"],
                                "値": [data[col].sum(), data[col].mean()]
                            })], ignore_index=True)
                    
                    summary_data.to_excel(writer, sheet_name='サマリー', index=False)
            
            table_name = st.session_state.selected_table or "data"
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') if add_timestamp else ""
            filename = f"{table_name}_{timestamp}.xlsx" if timestamp else f"{table_name}.xlsx"
            
            st.download_button(
                label="📥 Excelダウンロード",
                data=buffer.getvalue(),
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    except Exception as e:
        st.error(f"ダウンロード機能でエラーが発生しました: {str(e)}")

def render_table_structures():
    """テーブル構造を表示（メインテーブルとJOINテーブル）"""
    if not st.session_state.selected_table:
        return
    
    st.markdown('<div class="card-header">📊 テーブル構造</div>', unsafe_allow_html=True)
    
    try:
        # メインテーブルと結合テーブルを並べて表示
        if st.session_state.join_conditions:
            # JOINがある場合：並列表示
            join_tables = [join_info['table'] for join_info in st.session_state.join_conditions]
            
            # メインテーブルと最初のJOINテーブルを表示
            col1, col2 = st.columns(2)
            
            # メインテーブル
            with col1:
                st.markdown(f"""
                <div class="table-structure-card">
                    <div class="table-structure-header">
                        📋 メインテーブル: {st.session_state.selected_table}
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                schema_data = get_table_schema(
                    session,
                    st.session_state.selected_db,
                    st.session_state.selected_schema,
                    st.session_state.selected_table
                )
                if schema_data:
                    df_schema = pd.DataFrame(schema_data)
                    df_schema.columns = ["カラム名", "データ型", "サンプル"]
                    st.dataframe(df_schema, use_container_width=True, hide_index=True)
                else:
                    st.warning("メインテーブルのスキーマ情報を取得できませんでした")
            
            # 結合テーブル（複数ある場合は最初のもの）
            with col2:
                first_join_table = join_tables[0]
                st.markdown(f"""
                <div class="table-structure-card">
                    <div class="table-structure-header">
                        🔗 結合テーブル: {first_join_table}
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                join_schema_data = get_table_schema(
                    session,
                    st.session_state.selected_db,
                    st.session_state.selected_schema,
                    first_join_table
                )
                if join_schema_data:
                    df_join_schema = pd.DataFrame(join_schema_data)
                    df_join_schema.columns = ["カラム名", "データ型", "サンプル"]
                    st.dataframe(df_join_schema, use_container_width=True, hide_index=True)
                else:
                    st.warning(f"結合テーブル {first_join_table} のスキーマ情報を取得できませんでした")
            
            # 追加の結合テーブルがある場合
            if len(join_tables) > 1:
                st.markdown("### 📋 追加の結合テーブル")
                for additional_table in join_tables[1:]:
                    with st.expander(f"🔗 {additional_table}", expanded=False):
                        additional_schema_data = get_table_schema(
                            session,
                            st.session_state.selected_db,
                            st.session_state.selected_schema,
                            additional_table
                        )
                        if additional_schema_data:
                            df_additional_schema = pd.DataFrame(additional_schema_data)
                            df_additional_schema.columns = ["カラム名", "データ型", "サンプル"]
                            st.dataframe(df_additional_schema, use_container_width=True, hide_index=True)
                        else:
                            st.warning(f"テーブル {additional_table} のスキーマ情報を取得できませんでした")
            
            # JOIN条件の表示
            st.markdown("### 🔗 結合条件")
            for i, join_info in enumerate(st.session_state.join_conditions):
                st.markdown(f"""
                <div class="join-preview">
                    <strong>JOIN {i+1}:</strong><br>
                    {join_info['type']} {st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']}<br>
                    ON {st.session_state.selected_table}.{join_info['left_col']} = {join_info['table']}.{join_info['right_col']}
                </div>
                """, unsafe_allow_html=True)
        
        else:
            # JOINがない場合：メインテーブルのみ
            st.markdown(f"""
            <div class="table-structure-card">
                <div class="table-structure-header">
                    📋 メインテーブル: {st.session_state.selected_table}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            schema_data = get_table_schema(
                session,
                st.session_state.selected_db,
                st.session_state.selected_schema,
                st.session_state.selected_table
            )
            if schema_data:
                df_schema = pd.DataFrame(schema_data)
                df_schema.columns = ["カラム名", "データ型", "サンプル"]
                st.dataframe(df_schema, use_container_width=True, hide_index=True)
            else:
                st.warning("テーブルのスキーマ情報を取得できませんでした")
    
    except Exception as e:
        st.error(f"テーブル構造の表示でエラーが発生しました: {str(e)}")

def main():
    """メイン処理"""
    
    # ヘッダー
    st.markdown('<div class="header-title">🗂️ SQLレスデータ抽出ツール</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">直感的な操作でデータを抽出・分析</div>', unsafe_allow_html=True)
    
    # エラー表示エリア
    if st.session_state.last_error:
        st.markdown(f"""
        <div class="error-message">
            <strong>⚠️ 前回のエラー:</strong> {st.session_state.last_error}
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("❌ エラーを消去"):
            st.session_state.last_error = None
            st.rerun()
    
    # サイドバー - 設定エリア
    with st.sidebar:
        st.markdown("## 🔧 データ設定")
        
        # === 保存済み設定 ===
        st.markdown("### 💾 保存済み設定")
        
        if st.session_state.saved_configs:
            for config_name, config in st.session_state.saved_configs.items():
                config_display = f"**{config_name}**"
                if config.get("description"):
                    config_display += f"\n_{config['description']}_"
                config_display += f"\n`{config['db']}.{config['schema']}.{config['table']}`"
                
                if st.button(config_display, key=f"load_{config_name}", use_container_width=True):
                    load_saved_config(config_name)
        else:
            st.info("保存済み設定がありません")
        
        # 新規設定保存
        with st.expander("💾 現在の設定を保存", expanded=False):
            save_current_config()
        
        st.markdown("---")
        
        # === データソース選択 ===
        st.markdown("### 🗄️ データソース")
        
        # データベース選択
        db_options = [(key, value["name"]) for key, value in snowflake_metadata.items()]
        db_labels = [label for _, label in db_options]
        db_keys = [key for key, _ in db_options]
        
        current_db_index = 0
        if st.session_state.selected_db and st.session_state.selected_db in db_keys:
            current_db_index = db_keys.index(st.session_state.selected_db)
        
        selected_db_index = st.selectbox(
            "データベース",
            range(len(db_labels)),
            format_func=lambda x: db_labels[x],
            index=current_db_index,
            key="db_select"
        )
        
        st.session_state.selected_db = db_keys[selected_db_index]
        
        # スキーマ選択
        if st.session_state.selected_db:
            schema_options = list(snowflake_metadata[st.session_state.selected_db]["schemas"].keys())
            current_schema_index = 0
            if st.session_state.selected_schema and st.session_state.selected_schema in schema_options:
                current_schema_index = schema_options.index(st.session_state.selected_schema)
            
            selected_schema = st.selectbox(
                "スキーマ", 
                schema_options, 
                index=current_schema_index,
                key="schema_select"
            )
            st.session_state.selected_schema = selected_schema
            
            # テーブル選択
            if selected_schema:
                table_options = snowflake_metadata[st.session_state.selected_db]["schemas"][selected_schema]
                current_table_index = 0
                if st.session_state.selected_table and st.session_state.selected_table in table_options:
                    current_table_index = table_options.index(st.session_state.selected_table)
                
                selected_table = st.selectbox(
                    "テーブル", 
                    table_options, 
                    index=current_table_index,
                    key="table_select"
                )
                st.session_state.selected_table = selected_table
        
        st.markdown("---")
        
        # === 動的フィルター ===
        render_dynamic_filters()
        
        st.markdown("---")
        
        # === JOIN設定 ===
        render_join_config()
        
        st.markdown("---")
        
        # === 実行ボタン ===
        if st.session_state.selected_table:
            # クエリ検証ボタン
            if st.button("🔍 クエリを検証", use_container_width=True):
                errors, warnings = validate_query_before_execution()
                if errors:
                    st.error("❌ 検証エラー:")
                    for error in errors:
                        st.error(f"• {error}")
                elif warnings:
                    st.warning("⚠️ 検証警告:")
                    for warning in warnings:
                        st.warning(f"• {warning}")
                    st.success("✅ 警告はありますが実行可能です")
                else:
                    st.success("✅ 検証に合格しました")
            
            # 実行ボタン
            if st.button("🔍 データ抽出実行", use_container_width=True, type="primary"):
                with st.spinner("データを抽出中..."):
                    result_data, execution_time = execute_query()
                    if result_data is not None:
                        st.session_state.result_data = result_data
                        st.session_state.query_executed = True
                        st.session_state.execution_time = execution_time
                        st.success(f"✅ データ抽出完了: {len(result_data)}件のレコードを取得")
                        st.rerun()
        else:
            st.button("🔍 データ抽出実行", disabled=True, use_container_width=True, help="テーブルを選択してください")
        
        # リセットボタン
        if st.button("🔄 設定リセット", use_container_width=True):
            for key in list(st.session_state.keys()):
                if key.startswith(('selected_', 'query_', 'result_data', 'query_executed', 'join_', 'filter_', 'last_error')):
                    del st.session_state[key]
            init_session_state()  # 初期値で再初期化
            st.rerun()
        
        st.markdown("---")
        
        # === 現在の設定状況 ===
        st.markdown("### 📋 設定状況")
        
        if st.session_state.selected_table:
            st.markdown(f"""
            <div class="selection-status">
                <div class="selection-item selected">
                    <span>🗄️</span>
                    <span>{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # JOIN設定表示
        if st.session_state.join_conditions:
            for i, join_info in enumerate(st.session_state.join_conditions):
                st.markdown(f"""
                <div class="selection-status">
                    <div class="selection-item selected">
                        <span>🔗</span>
                        <span>JOIN {i+1}: {join_info['type']} {join_info['table']}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        # 条件カウント表示
        conditions_count = sum(1 for k, v in st.session_state.query_conditions.items() 
                             if v and k not in ['group_by', 'limit_rows', 'sort_column', 'sort_order'])
        if conditions_count > 0:
            st.markdown(f"""
            <div class="selection-status">
                <div class="selection-item selected">
                    <span>🔍</span>
                    <span>絞り込み条件: {conditions_count}件</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # === メインエリア ===
    
    # テーブル構造表示（上部）
    if st.session_state.selected_table:
        render_table_structures()
        
        # 区切り線
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # 結果表示エリア（下部）
    if st.session_state.query_executed and st.session_state.result_data is not None:
        st.markdown('<div class="card-header">📋 抽出結果</div>', unsafe_allow_html=True)
        
        # 結果サマリー
        result_data = st.session_state.result_data
        
        st.markdown(f"""
        <div class="result-summary">
            <div class="summary-item">
                <div class="summary-value">{len(result_data):,}</div>
                <div class="summary-label">レコード数</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">{st.session_state.execution_time:.1f}秒</div>
                <div class="summary-label">実行時間</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">{len(result_data.columns)}</div>
                <div class="summary-label">カラム数</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # 結果タブ
        tab1, tab2, tab3, tab4 = st.tabs(["📋 データ", "📊 グラフ", "💾 ダウンロード", "📝 SQL"])
        
        with tab1:
            # データの表示オプション
            col1, col2, col3 = st.columns(3)
            with col1:
                show_rows = st.selectbox("表示行数", [50, 100, 500, 1000, "全て"], index=1, key="display_rows")
            with col2:
                show_info = st.checkbox("データ型情報を表示", key="show_info")
            with col3:
                show_stats = st.checkbox("基本統計を表示", key="show_stats")
            
            # データ表示
            display_data = result_data
            if show_rows != "全て":
                display_data = result_data.head(show_rows)
            
            st.dataframe(display_data, use_container_width=True, hide_index=True)
            
            # データ型情報
            if show_info:
                st.subheader("📊 データ型情報")
                info_df = pd.DataFrame({
                    'カラム名': result_data.columns,
                    'データ型': result_data.dtypes.astype(str),
                    'NULL数': result_data.isnull().sum(),
                    'ユニーク数': result_data.nunique()
                })
                st.dataframe(info_df, use_container_width=True, hide_index=True)
            
            # 基本統計
            if show_stats and len(result_data.select_dtypes(include=['number']).columns) > 0:
                st.subheader("📈 基本統計")
                st.dataframe(result_data.describe(), use_container_width=True)
        
        with tab2:
            render_charts(result_data)
        
        with tab3:
            render_download_section(result_data)
        
        with tab4:
            st.subheader("📝 実行されたSQL")
            try:
                executed_sql = generate_sql_query()
                if executed_sql:
                    st.code(executed_sql, language="sql")
                    
                    # SQLをコピーするボタン
                    st.download_button(
                        label="📋 SQLをファイルとしてダウンロード",
                        data=executed_sql,
                        file_name=f"query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                        mime="text/plain",
                        use_container_width=True
                    )
                else:
                    st.error("SQLの生成に失敗しました")
            except Exception as e:
                st.error(f"SQLの表示中にエラーが発生しました: {str(e)}")
    
    elif st.session_state.selected_table:
        # テーブル選択済みだが未実行の場合
        st.info("📋 左側のサイドバーで条件を設定して「データ抽出実行」ボタンを押してください")
        
        # 生成されるSQLのプレビュー
        if st.session_state.query_conditions or st.session_state.join_conditions:
            with st.expander("🔍 生成されるSQL（プレビュー）", expanded=False):
                try:
                    sql_preview = generate_sql_query()
                    if sql_preview:
                        st.code(sql_preview, language="sql")
                    else:
                        st.warning("SQLの生成に失敗しました。設定を確認してください。")
                except Exception as e:
                    st.error(f"SQLプレビューの生成でエラー: {str(e)}")
    
    else:
        # 何も選択されていない場合
        st.info("🗄️ 左側のサイドバーからデータソースを選択するか、保存済み設定を読み込んでください")
        
        # 機能説明
        st.markdown("""
        ### 🚀 使い方
        
        1. **データソース選択**: サイドバーでデータベース・スキーマ・テーブルを選択
        2. **結合設定**: 必要に応じて他のテーブルとのJOINを設定
        3. **絞り込み条件**: データの絞り込み条件を設定
        4. **実行**: 「データ抽出実行」ボタンでクエリを実行
        5. **分析**: 結果を表やグラフで確認、CSVやExcelでダウンロード
        
        ### ✨ 主な機能
        
        - **SQLレス操作**: SQLを書かずに直感的にデータを抽出
        - **複数テーブル結合**: 複雑なJOINもGUIで簡単設定
        - **動的フィルタリング**: テーブル構造に応じた適切なフィルター
        - **設定保存**: よく使う設定を保存して再利用
        - **エラーハンドリング**: 問題がある設定は事前に検証・警告
        - **多彩な出力**: CSV、Excel、グラフによる可視化
        """)

# アプリの実行
if __name__ == "__main__":
    main()
    
    # フッター
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #475569; font-size: 0.9rem; padding: 1rem;">
        🗂️ SQLレスデータ抽出ツール v2.0 | エラーハンドリング強化版
    </div>
    """, unsafe_allow_html=True)