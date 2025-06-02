import streamlit as st
import pandas as pd
import json
import time
from snowflake.snowpark.context import get_active_session
import snowflake.connector
import _snowflake  # Cortex Analyst API呼び出し用モジュール

# ---------------------------------------------------
# Streamlitアプリの基本設定
# ---------------------------------------------------
st.set_page_config(
    page_title="Snowflake Cortex Analyst",
    page_icon="❄️",
    layout="wide",
)

# ===================================================
# タブによるレイアウト設定
# ===================================================
# タブ１：アプリケーション仕様（UI/UXの説明やグラフィカルなデザイン）
# タブ２：実際に動作するアプリケーション本体
tabs = st.tabs(["アプリケーション仕様", "アプリケーション本体"])

# ===================================================
# タブ１：アプリケーション仕様（UI/UX説明）
# ---------------------------------------------------
with tabs[0]:
    st.markdown(
        """
<style>
/* 手書き風の日本語フォント（外部読み込みが必要な場合があります） */
@import url('https://fonts.googleapis.com/css2?family=Kaisei+Decol&display=swap');

/* 全体背景色を薄いブルー系に設定 */
body, .reportview-container {
  background-color: #F6FAFE !important; 
}

/* 文章部分の設定 */
.article {
  font-family: 'Kaisei Decol', sans-serif;
  width: 100%;
  margin: 0 auto;
  padding: 1em;
  color: #334155;
  line-height: 1.6;
}

/* タイトルの装飾 */
h1 {
  font-size: 32px;
  font-weight: bold;
  background: linear-gradient(to right, #63C0F6, #1FAEFF);
  -webkit-background-clip: text;
  color: transparent;
  margin-bottom: 0.2em;
  display: inline-block;
  padding-left: 0.2em;
}

/* セクション見出しの設定 - シンプルに */
h3 {
  font-size: 20px;
  color: #1e40af;
  margin-top: 1.4em;
  margin-bottom: 0.5em;
  font-weight: bold;
}

/* 見出しアイコンのスタイル */
.section-icon {
  margin-right: 0.5em;
  font-size: 20px;
}

/* カード風の装飾 */
.section-card {
  background-color: #FFFFFF;
  border-radius: 8px;
  box-shadow: 0 2px 6px rgba(0,0,0,0.08);
  margin: 1em 0;
  padding: 1.2em 1.5em;
  border-left: 6px solid #63C0F6;
}

/* 強調用クラス */
.note {
  display: inline-block;
  background-color: #DAF1FF;
  border-radius: 4px;
  padding: 2px 6px;
  margin: 0 4px;
}
.marker {
  background-color: #A9DFFF;
  padding: 0 4px;
  border-radius: 2px;
}
.arrow {
  color: #1B95E0; 
  font-weight: bold;
}

/* 3カラムレイアウト用のカード - アイコンレイアウト修正 */
.icon-card {
  display: flex;
  align-items: center;
  margin: 0.6em 0;
  background-color: #DAF1FF;
  border-radius: 6px;
  padding: 0.8em 1em; /* パディングを増やして余裕を持たせる */
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  min-height: 2.5em; /* 最小高さを設定 */
}
.icon-circle {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 2em; /* サイズを少し大きく */
  height: 2em;
  margin-right: 1em; /* 右マージンを増やして間隔を広げる */
  font-size: 1.2em;
  flex-shrink: 0; /* アイコン部分が縮まないようにする */
  background-color: rgba(255, 255, 255, 0.8);
  border-radius: 50%;
}

/* カラム内の見出しを調整 */
.stColumns h4 {
  font-size: 16px;
  margin-bottom: 1em;
  color: #1e40af;
  font-weight: bold;
}

/* フッターの設定 */
footer {
  text-align: right;
  font-size: 12px;
  color: #475569;
  margin-top: 2em;
  opacity: 0.7;
}

/* リストのアイコンスタイル調整 */
.section-card ul li {
  margin-bottom: 0.5em;
  display: flex;
  align-items: flex-start;
}

.section-card .arrow {
  margin-right: 0.5em;
  margin-top: 0.2em;
  flex-shrink: 0;
}
</style>

<div class="article">
  <!-- ヘッダー部分 -->
  <header style="display:flex; flex-direction:column; align-items:flex-start;">
    <h1>❄️ Snowflake Cortex Analyst</h1>
  </header>

  <!-- アプリケーション概要 -->
  <div class="section-card">
    <h3><span class="section-icon">📋</span>アプリケーション概要</h3>
    <p>
      Snowflake上で動作する <strong>Streamlitアプリ</strong> で、
      データアナリストやビジネスユーザーが
      <span class="note">自然言語</span> を使って
      データを簡単に探索・分析できるツールです。
      専門的なSQL知識がなくても、
      <span class="marker">「日本語での質問」</span> を
      そのままSQLに変換し、結果を即座に取得できます。
    </p>
  </div>

  <!-- 主な機能の紹介 -->
  <div class="section-card">
    <h3><span class="section-icon">⚙️</span>主な機能</h3>
    <p>
      ユーザーが直感的に操作できるように、
      <span class="marker">アイコン</span> や <span class="marker">色分け</span> を活用し、
      <strong>分かりやすく可視化</strong>しています。
    </p>
""",
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("<h4>1. インタラクティブなデータ探索</h4>", unsafe_allow_html=True)
        st.markdown(
            """
<div class="icon-card">
  <span class="icon-circle">🔎</span>
  <span>データベース選択</span>
</div>
<div class="icon-card">
  <span class="icon-circle">🗂</span>
  <span>スキーマ・テーブル選択</span>
</div>
<div class="icon-card">
  <span class="icon-circle">❓</span>
  <span>テーブル構造表示</span>
</div>
            """, unsafe_allow_html=True)
    with col2:
        st.markdown("<h4>2. 2種類のクエリモード</h4>", unsafe_allow_html=True)
        st.markdown(
            """
<div class="icon-card">
  <span class="icon-circle">💻</span>
  <span>直接SQLモード</span>
</div>
<div class="icon-card">
  <span class="icon-circle">📝</span>
  <span>自然言語モード <em>(Cortex Analyst)</em></span>
</div>
            """, unsafe_allow_html=True)
    with col3:
        st.markdown("<h4>3. 多角的なデータ可視化</h4>", unsafe_allow_html=True)
        st.markdown(
            """
<div class="icon-card">
  <span class="icon-circle">📋</span>
  <span>クエリ結果テーブル表示</span>
</div>
<div class="icon-card">
  <span class="icon-circle">📈</span>
  <span>線グラフ・棒グラフの自動生成</span>
</div>
<div class="icon-card">
  <span class="icon-circle">🔀</span>
  <span>タブ切り替えで表/グラフを簡単比較</span>
</div>
            """, unsafe_allow_html=True)

    st.markdown(
        """
  </div>

  <!-- 利用シナリオ例 -->
  <div class="section-card">
    <h3>利用シナリオ例</h3>
    <p>
      🧩 <strong>ビジネスユーザー</strong>：  
       「先月の地域別売上トップ5を教えて」  
       → 自動SQL生成 → 結果をグラフ表示  
    </p>
    <p>
      📊 <strong>データアナリスト</strong>：  
       複雑なSQLを直接実行 → 結果を即座にグラフ化  
       → 分析効率アップ  
    </p>
  </div>

  <!-- 特長と利点 -->
  <div class="section-card">
    <h3>特長と利点</h3>
    <ul style="margin-left:1em;">
      <li><span class="arrow">✔</span> <strong>簡単な操作性</strong>：技術知識が不要</li>
      <li><span class="arrow">✔</span> <strong>Snowflakeと統合</strong>：データ移動なしで分析</li>
      <li><span class="arrow">✔</span> <strong>迅速な分析サイクル</strong>：質問→結果→意思決定が早い</li>
      <li><span class="arrow">✔</span> <strong>柔軟なアクセス</strong>：SQL経験者も自然言語利用者も同じツールで</li>
    </ul>
  </div>

  <!-- 使用方法 -->
  <div class="section-card">
    <h3>使用方法</h3>
    <p>
      <strong>1.</strong> Snowflake環境のStreamlitアプリにアクセス<br>
      <strong>2.</strong> サイドバーで <em>データベース / スキーマ / テーブル</em> を選択<br>
      <strong>3.</strong> 「直接SQL」または「自然言語」モードを選択<br>
      <strong>4.</strong> クエリまたは質問を入力 → 実行ボタン<br>
      <strong>5.</strong> 結果を表やグラフで可視化して分析
    </p>
  </div>

  <!-- 技術的特徴 -->
  <div class="section-card">
    <h3>技術的特徴</h3>
    <ul style="margin-left:1em;">
      <li>🔧 Snowflakeセッション管理を自動化</li>
      <li>🔧 リアルタイムSQLクエリ生成/実行</li>
      <li>🔧 インタラクティブなビジュアライゼーション</li>
      <li>🔧 データキャッシングで高速レスポンス</li>
    </ul>
    <p>
      <span class="marker">ビジネスユーザーがSQLを覚えなくても</span><br>
      <strong>データドリブンな意思決定</strong>が可能です！
    </p>
  </div>

  <footer>
    © 2025 Snowflake Cortex Analyst — All Rights Reserved
  </footer>
</div>
""",
        unsafe_allow_html=True
    )

# ===================================================
# タブ２：アプリケーション本体（実際の処理ロジック）
# ---------------------------------------------------
with tabs[1]:
    # -----------------------------------------------
    # Snowflake接続・データ取得用関数群
    # -----------------------------------------------
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

    # -----------------------------------------------
    # SQLクエリ実行と結果表示のための関数
    # -----------------------------------------------
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

    # -----------------------------------------------
    # Cortex Analyst API連携用関数群
    # -----------------------------------------------
    def send_message(prompt: str, database: str, schema: str, stage: str, file: str) -> dict:
        """
        Cortex Analyst APIを呼び出して、指定したSemantic Modelファイルに基づく応答を取得する関数
        """
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
            "semantic_model_file": f"@{database}.{schema}.{stage}/{file}",
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
                raise Exception(f"Failed request with status {resp['status']}: {resp}")
        except Exception as e:
            st.error(f"Cortex Analyst APIの呼び出しに失敗しました: {e}")
            return None

    def process_message(prompt: str, database: str, schema: str, stage: str, file: str) -> None:
        """
        ユーザーの質問を処理し、チャット形式で応答を表示する関数
        """
        # セッション状態の初期化を改善
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "active_suggestion" not in st.session_state:
            st.session_state.active_suggestion = None
            
        st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating response..."):
                response = send_message(prompt=prompt, database=database, schema=schema, stage=stage, file=file)
                if response:
                    content = response["message"]["content"]
                    display_content(content=content)
                    st.session_state.messages.append({"role": "assistant", "content": content})
                else:
                    st.error("応答の生成中にエラーが発生しました。")

    def display_content(content: list, message_index: int = None) -> None:
        """
        Cortex Analystの応答内容（テキスト、提案、SQL）を適切に表示する関数
        """
        message_index = message_index or len(st.session_state.get("messages", []))
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "suggestions":
                with st.expander("提案", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        # ユニークなキーを生成
                        button_key = f"suggestion_{message_index}_{suggestion_index}_{hash(suggestion) % 10000}"
                        if st.button(suggestion, key=button_key):
                            st.session_state.active_suggestion = suggestion
                            # 即座にリロードせず、次のループで処理される
            elif item["type"] == "sql":
                with st.expander("SQL Query", expanded=False):
                    st.code(item["statement"], language="sql")
                with st.expander("Results", expanded=True):
                    with st.spinner("Running SQL..."):
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

    # -----------------------------------------------
    # メインアプリケーション処理（サイドバーでモード選択）
    # -----------------------------------------------
    def main():
        """
        アプリケーション本体のメイン処理
        ・サイドバーによりモードや接続情報、データベース・スキーマ・テーブル等を選択
        ・選択に応じてCortex Analystモードまたは直接SQLクエリモードを実行
        """
        st.sidebar.title("ナビゲーション")
        # モード選択（リロードなし）
        page = st.sidebar.radio("モードを選択", ["Cortex Analyst", "直接SQLクエリ"], key="mode_selection")
        
        session = get_snowpark_session()
        if not session:
            st.warning("Snowflake接続を確立できませんでした。")
            return
        
        try:
            current_db = session.get_current_database()
            current_schema = session.get_current_schema()
            current_warehouse = session.get_current_warehouse()
            
            st.sidebar.subheader("現在の接続情報")
            st.sidebar.info(
                f"データベース: {current_db}\n"
                f"スキーマ: {current_schema}\n"
                f"ウェアハウス: {current_warehouse}"
            )
            
            databases = get_available_databases()
            if not databases:
                st.warning("データベースを取得できませんでした。")
                return
            
            # デフォルトインデックスの設定を改善
            try:
                default_db_index = databases.index(current_db) if current_db in databases else 0
            except (ValueError, TypeError):
                default_db_index = 0
                
            selected_db = st.sidebar.selectbox(
                "データベースを選択", 
                databases, 
                index=default_db_index,
                key="database_selection"
            )
            
            if selected_db:
                schemas = get_available_schemas(selected_db)
                if not schemas:
                    st.warning(f"データベース {selected_db} にスキーマが見つかりません。")
                    return
                
                try:
                    default_schema_index = schemas.index(current_schema) if current_schema in schemas else 0
                except (ValueError, TypeError):
                    default_schema_index = 0
                    
                selected_schema = st.sidebar.selectbox(
                    "スキーマを選択", 
                    schemas, 
                    index=default_schema_index,
                    key="schema_selection"
                )
                
                if selected_schema:
                    if page == "Cortex Analyst":
                        st.subheader("Cortex Analystモード")
                        stages = get_available_stages(selected_db, selected_schema)
                        if not stages:
                            st.warning(f"スキーマ {selected_schema} にステージが見つかりません。")
                            return
                        
                        selected_stage = st.sidebar.selectbox(
                            "ステージを選択", 
                            stages,
                            key="stage_selection"
                        )
                        
                        if selected_stage:
                            files = get_files_in_stage(selected_db, selected_schema, selected_stage)
                            if not files:
                                st.warning(f"ステージ {selected_stage} にファイルが見つかりません。")
                                return
                            
                            yaml_files = [f for f in files if f.endswith('.yaml') or f.endswith('.yml')]
                            if not yaml_files:
                                st.warning(f"ステージ {selected_stage} にYAMLファイルが見つかりません。")
                                return
                            
                            selected_file = st.sidebar.selectbox(
                                "Semantic Model (.yaml)を選択", 
                                yaml_files,
                                key="file_selection"
                            )
                            
                            if selected_file:
                                file_name = selected_file.split('/')[-1]
                                st.markdown(f"Semantic Model: `{file_name}`")
                                
                                # チャット履歴クリアボタン
                                if st.button("チャット履歴をクリア", key="clear_chat_history"):
                                    st.session_state.messages = []
                                    st.session_state.active_suggestion = None
                                    st.rerun()  # この場合のみリロードが必要
                                
                                # セッション状態の初期化（改善版）
                                if "messages" not in st.session_state:
                                    st.session_state.messages = []
                                if "active_suggestion" not in st.session_state:
                                    st.session_state.active_suggestion = None
                                
                                # チャット履歴の表示
                                for message_index, message in enumerate(st.session_state.messages):
                                    with st.chat_message(message["role"]):
                                        display_content(content=message["content"], message_index=message_index)
                                
                                # ユーザー入力の処理
                                if user_input := st.chat_input("何か質問がありますか？"):
                                    process_message(
                                        prompt=user_input,
                                        database=selected_db,
                                        schema=selected_schema,
                                        stage=selected_stage,
                                        file=file_name
                                    )
                                
                                # アクティブな提案の処理（リロードなし）
                                if st.session_state.get("active_suggestion"):
                                    process_message(
                                        prompt=st.session_state.active_suggestion,
                                        database=selected_db,
                                        schema=selected_schema,
                                        stage=selected_stage,
                                        file=file_name
                                    )
                                    st.session_state.active_suggestion = None
                    
                    elif page == "直接SQLクエリ":
                        st.subheader("直接SQLクエリモード")
                        tables = get_available_tables(selected_db, selected_schema)
                        if not tables:
                            st.warning(f"スキーマ {selected_schema} にテーブルが見つかりません。")
                            return
                        
                        selected_table = st.sidebar.selectbox(
                            "テーブルを選択", 
                            tables,
                            key="table_selection"
                        )
                        
                        if selected_table:
                            columns = get_table_schema(selected_db, selected_schema, selected_table)
                            with st.expander("テーブル構造", expanded=False):
                                table_info = pd.DataFrame({
                                    "カラム名": [col["name"] for col in columns],
                                    "データ型": [col["type"] for col in columns],
                                    "NULL許可": [col["nullable"] for col in columns],
                                })
                                st.dataframe(table_info)
                            
                            default_query = f"SELECT * FROM {selected_db}.{selected_schema}.{selected_table} LIMIT 100"
                            sql_query = st.text_area(
                                "SQLクエリを入力してください", 
                                value=default_query, 
                                height=200,
                                key="sql_query_input"
                            )
                            
                            if st.button("クエリを実行", key="execute_sql_query"):
                                display_sql(sql_query)
        
        except Exception as e:
            st.error(f"アプリ実行中にエラーが発生しました: {e}")
            st.info("Snowflake環境に正しく接続されていることを確認してください。")

    # -----------------------------------------------
    # メイン処理の呼び出し
    # -----------------------------------------------
    main()