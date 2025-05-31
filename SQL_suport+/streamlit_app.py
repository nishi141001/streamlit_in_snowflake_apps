"""
メインアプリケーション
SQLレスデータ抽出ツール
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from snowflake_utils import init_snowflake_session, get_snowflake_metadata
from config_manager import check_config_table_exists, load_persistent_configs
from query_engine import validate_query_before_execution, generate_sql_query, execute_query
from ui_components import (
    get_custom_css, render_dynamic_filters, render_join_config, 
    render_table_structures, render_charts, render_download_section,
    save_current_config, render_saved_configs
)


# ページ設定
st.set_page_config(
    page_title="SQLレスデータ抽出ツール",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown(get_custom_css(), unsafe_allow_html=True)


def init_session_state():
    """セッション状態を初期化"""
    default_values = {
        'selected_db': None,
        'selected_schema': None,
        'selected_table': None,
        'join_conditions': [],
        'query_conditions': {},
        'result_data': None,
        'query_executed': False,
        'execution_time': 0,
        'saved_configs': {},
        'filter_conditions': [],
        'last_error': None,
        'query_validation_errors': [],
        'persistent_configs_loaded': False
    }
    
    for key, value in default_values.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main():
    """メイン処理"""
    
    # セッション状態の初期化
    init_session_state()
    
    # Snowflakeセッション初期化
    session = init_snowflake_session()
    if session is None:
        st.error("Snowflakeセッションの初期化に失敗しました。アプリケーションを再起動してください。")
        st.stop()
    
    # メタデータの取得
    snowflake_metadata = get_snowflake_metadata(session)
    if not snowflake_metadata:
        st.error("Snowflakeのメタデータの取得に失敗しました。")
        st.stop()
    
    # メタデータをセッション状態に保存
    st.session_state.snowflake_metadata = snowflake_metadata
    
    # 永続化設定の読み込み（初回のみ）
    if not st.session_state.get('persistent_configs_loaded', False):
        load_persistent_configs()
    
    # ヘッダー
    # ヘッダー
    st.markdown('<div class="header-title">🗂️ SQLレスデータ抽出ツール</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">直感的な操作でデータを抽出・分析（Snowflakeテーブル永続化対応）</div>', unsafe_allow_html=True)
    
    # 設定テーブルの存在確認
    if not check_config_table_exists():
        st.markdown("""
        <div style="background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color: white; padding: 1.5rem; border-radius: 10px; margin: 1rem 0;">
            <h3>❌ 初期設定が必要です</h3>
            <p>設定保存機能を使用するには、初期設定ページでテーブルを作成してください。</p>
            <ul>
                <li>📋 初期設定ページでSnowflake接続を確認</li>
                <li>🗄️ 設定保存テーブルを作成</li>
                <li>📝 サンプルデータを挿入（オプション）</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.info("💡 初期設定完了後、このページを再読み込みしてください。")
    
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
        
        if check_config_table_exists():
            # === 保存済み設定 ===
            render_saved_configs()
            
            # 新規設定保存
            with st.expander("💾 現在の設定を保存", expanded=False):
                save_current_config()
        else:
            st.markdown("""
            <div style="background: #fef2f2; border: 1px solid #fecaca; padding: 1rem; border-radius: 8px; margin: 1rem 0;">
                <strong>⚠️ 設定保存機能が利用できません</strong><br>
                初期設定を完了してください。
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # === データソース選択 ===
        st.markdown("### 🗄️ データソース")
        
        # データベース選択
        db_options = [(key, value["name"]) for key, value in snowflake_metadata.items()]
        db_labels = [label for _, label in db_options]
        db_keys = [key for key, _ in db_options]
        
        if db_keys:
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
                if schema_options:
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
                        if table_options:
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
            init_session_state()
            st.rerun()
        
        st.markdown("---")
        
        # === 現在の設定状況 ===
        st.markdown("### 📋 設定状況")
        
        if st.session_state.selected_table:
            st.markdown(f"""
            <div style="background: white; border: 1px solid #DAF1FF; border-radius: 8px; padding: 1rem; margin: 0.5rem 0;">
                <div style="display: flex; align-items: center; gap: 0.5rem; margin: 0.3rem 0; font-size: 0.9rem; color: #1e40af; font-weight: 500;">
                    <span>🗄️</span>
                    <span>{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # JOIN設定表示
        if st.session_state.join_conditions:
            for i, join_info in enumerate(st.session_state.join_conditions):
                st.markdown(f"""
                <div style="background: white; border: 1px solid #DAF1FF; border-radius: 8px; padding: 1rem; margin: 0.5rem 0;">
                    <div style="display: flex; align-items: center; gap: 0.5rem; margin: 0.3rem 0; font-size: 0.9rem; color: #1e40af; font-weight: 500;">
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
            <div style="background: white; border: 1px solid #DAF1FF; border-radius: 8px; padding: 1rem; margin: 0.5rem 0;">
                <div style="display: flex; align-items: center; gap: 0.5rem; margin: 0.3rem 0; font-size: 0.9rem; color: #1e40af; font-weight: 500;">
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
        tab1, tab2, tab3 = st.tabs(["📋 データ", "📊 グラフ", "💾 ダウンロード"])
        
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
    
    # フッター
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #475569; font-size: 0.9rem; padding: 1rem;">
        🗂️ SQLレスデータ抽出ツール v3.0 モジュール分割版 | 全機能搭載
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()