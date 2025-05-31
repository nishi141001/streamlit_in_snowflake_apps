"""
初期設定画面
SQLレスデータ抽出ツール用
"""

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
from snowflake_utils import init_snowflake_session, get_user_context
from config_manager import (
    check_config_table_exists, create_config_table, get_table_statistics,
    insert_sample_data, CONFIG_TABLE_NAME
)
from ui_components import get_custom_css


# ページ設定
st.set_page_config(
    page_title="SQLレスデータ抽出ツール - 初期設定",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown(get_custom_css(), unsafe_allow_html=True)

# 追加のCSS（初期設定画面用）
st.markdown("""
<style>
.setup-card {
    background: white;
    border: 1px solid #DAF1FF;
    border-radius: 15px;
    padding: 2rem;
    margin: 1.5rem 0;
    box-shadow: 0 4px 20px rgba(99, 192, 246, 0.15);
    border-left: 6px solid #63C0F6;
}

.setup-card-header {
    color: #1e40af;
    font-size: 1.4rem;
    font-weight: bold;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    gap: 0.8rem;
}

.status-success {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
    color: white;
    padding: 1rem 1.5rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: bold;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.status-warning {
    background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
    color: white;
    padding: 1rem 1.5rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: bold;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.status-error {
    background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
    color: white;
    padding: 1rem 1.5rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: bold;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.status-info {
    background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
    color: white;
    padding: 1rem 1.5rem;
    border-radius: 10px;
    margin: 1rem 0;
    font-weight: bold;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.table-info {
    background: linear-gradient(135deg, #F6FAFE 0%, #DAF1FF 100%);
    border-radius: 10px;
    padding: 1.5rem;
    margin: 1rem 0;
}

.table-info-header {
    font-size: 1.1rem;
    font-weight: bold;
    color: #1e40af;
    margin-bottom: 1rem;
}

.section-divider {
    border-top: 3px solid #DAF1FF;
    margin: 3rem 0;
    position: relative;
}

.section-divider::after {
    content: "⚙️";
    position: absolute;
    top: -15px;
    left: 50%;
    transform: translateX(-50%);
    background: white;
    padding: 0 1rem;
    font-size: 1.5rem;
}

.progress-container {
    background: #f1f5f9;
    border-radius: 10px;
    padding: 0.5rem;
    margin: 1rem 0;
}

.progress-bar {
    background: linear-gradient(135deg, #63C0F6 0%, #1FAEFF 100%);
    height: 1rem;
    border-radius: 8px;
    transition: width 0.5s ease;
}
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """セッション状態の初期化"""
    default_values = {
        'setup_completed': False,
        'setup_step': 1,
        'connection_verified': False,
        'table_created': False,
        'sample_data_inserted': False
    }
    
    for key, value in default_values.items():
        if key not in st.session_state:
            st.session_state[key] = value


def check_snowflake_connection():
    """Snowflake接続を確認"""
    try:
        session = init_snowflake_session()
        if not session:
            return False, "セッションの初期化に失敗"
            
        result = session.sql("SELECT CURRENT_VERSION() as version, CURRENT_USER() as user_name, CURRENT_DATABASE() as db_name, CURRENT_SCHEMA() as schema_name").collect()
        if result:
            return True, result[0]
        return False, None
    except Exception as e:
        return False, str(e)


def render_connection_check():
    """接続確認セクション"""
    st.markdown("""
    <div class="setup-card">
        <div class="setup-card-header">
            🔌 Snowflake接続確認
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("🔍 接続を確認", key="check_connection", use_container_width=True):
        with st.spinner("Snowflake接続を確認中..."):
            success, result = check_snowflake_connection()
            
            if success:
                st.session_state.connection_verified = True
                user_context_info = get_user_context()
                
                st.markdown(f"""
                <div class="status-success">
                    ✅ Snowflake接続が正常に確認されました
                </div>
                """, unsafe_allow_html=True)
                
                # 接続情報を表示
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**接続情報:**")
                    st.write(f"🏢 データベース: `{result['DB_NAME']}`")
                    st.write(f"📂 スキーマ: `{result['SCHEMA_NAME']}`")
                    st.write(f"👤 ユーザー: `{result['USER_NAME']}`")
                
                with col2:
                    st.markdown("**システム情報:**")
                    st.write(f"🔢 Snowflakeバージョン: `{result['VERSION']}`")
                    st.write(f"🆔 ユーザーコンテキスト: `{user_context_info}`")
                
                st.session_state.setup_step = 2
            else:
                st.markdown(f"""
                <div class="status-error">
                    ❌ Snowflake接続に失敗しました: {result}
                </div>
                """, unsafe_allow_html=True)
    
    elif st.session_state.connection_verified:
        st.markdown("""
        <div class="status-success">
            ✅ 接続確認済み
        </div>
        """, unsafe_allow_html=True)


def render_table_setup():
    """テーブル設定セクション"""
    if not st.session_state.connection_verified:
        st.markdown("""
        <div class="status-warning">
            ⚠️ 最初にSnowflake接続を確認してください
        </div>
        """, unsafe_allow_html=True)
        return
    
    st.markdown("""
    <div class="setup-card">
        <div class="setup-card-header">
            🗄️ 設定保存テーブルの作成
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # テーブル名の表示
    st.info(f"📋 作成するテーブル名: `{CONFIG_TABLE_NAME}`")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 テーブル存在確認", key="check_table", use_container_width=True):
            with st.spinner("テーブルを確認中..."):
                exists = check_config_table_exists()
                
                if exists:
                    st.markdown(f"""
                    <div class="status-info">
                        ℹ️ テーブル '{CONFIG_TABLE_NAME}' は既に存在します
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # 統計情報を取得
                    stats = get_table_statistics()
                    if stats:
                        st.markdown(f"""
                        <div class="table-info">
                            <div class="table-info-header">📊 テーブル統計情報</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("総レコード数", stats['TOTAL_RECORDS'])
                        with col2:
                            st.metric("アクティブレコード", stats['ACTIVE_RECORDS'])
                        with col3:
                            st.metric("ユニークユーザー", stats['UNIQUE_USERS'])
                    
                    st.session_state.table_created = True
                else:
                    st.markdown(f"""
                    <div class="status-warning">
                        ⚠️ テーブル '{CONFIG_TABLE_NAME}' は存在しません
                    </div>
                    """, unsafe_allow_html=True)
    
    with col2:
        if st.button("🔨 テーブルを作成", key="create_table", use_container_width=True):
            with st.spinner("テーブルを作成中..."):
                success, message = create_config_table()
                
                if success:
                    st.session_state.table_created = True
                    st.session_state.setup_step = 3
                    
                    st.markdown(f"""
                    <div class="status-success">
                        ✅ {message}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.balloons()
                else:
                    st.markdown(f"""
                    <div class="status-error">
                        ❌ {message}
                    </div>
                    """, unsafe_allow_html=True)


def render_sample_data():
    """サンプルデータ挿入セクション"""
    if not st.session_state.table_created:
        st.markdown("""
        <div class="status-warning">
            ⚠️ 最初にテーブルを作成してください
        </div>
        """, unsafe_allow_html=True)
        return
    
    st.markdown("""
    <div class="setup-card">
        <div class="setup-card-header">
            📝 サンプルデータの挿入
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    **サンプルデータについて:**
    - SQLレスデータ抽出ツールの使用例となる設定データ
    - 売上データ抽出と顧客データ分析の2つの設定例
    - 実際の使用方法を学ぶためのリファレンス
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 サンプルデータを挿入", key="insert_sample", use_container_width=True):
            with st.spinner("サンプルデータを挿入中..."):
                success, message = insert_sample_data()
                
                if success:
                    st.session_state.sample_data_inserted = True
                    st.markdown(f"""
                    <div class="status-success">
                        ✅ {message}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="status-error">
                        ❌ {message}
                    </div>
                    """, unsafe_allow_html=True)
    
    with col2:
        if st.button("🗑️ サンプルデータを削除", key="delete_sample", use_container_width=True):
            if st.session_state.get("confirm_delete_sample", False):
                with st.spinner("サンプルデータを削除中..."):
                    try:
                        session = init_snowflake_session()
                        user_context = get_user_context()
                        
                        if session and user_context:
                            delete_query = f"""
                            DELETE FROM {CONFIG_TABLE_NAME}
                            WHERE user_context = '{user_context}'
                            AND config_name LIKE 'サンプル設定%'
                            """
                            session.sql(delete_query).collect()
                            
                            st.markdown("""
                            <div class="status-success">
                                ✅ サンプルデータを削除しました
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.session_state.confirm_delete_sample = False
                    except Exception as e:
                        st.markdown(f"""
                        <div class="status-error">
                            ❌ サンプルデータの削除に失敗: {str(e)}
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.session_state.confirm_delete_sample = True
                st.warning("⚠️ サンプルデータを削除しますか？もう一度ボタンを押してください。")
                st.rerun()


def render_completion():
    """セットアップ完了セクション"""
    if not all([st.session_state.connection_verified, st.session_state.table_created]):
        return
    
    st.markdown("""
    <div class="setup-card">
        <div class="setup-card-header">
            🎉 初期設定完了
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(f"""
    <div class="status-success">
        ✅ 初期設定が完了しました！
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    **セットアップ完了項目:**
    - ✅ Snowflake接続確認
    - ✅ 設定保存テーブル作成
    - ✅ インデックス作成
    """)
    
    if st.session_state.sample_data_inserted:
        st.markdown("- ✅ サンプルデータ挿入")
    
    # 設定情報の保存
    setup_config = {
        "table_name": CONFIG_TABLE_NAME,
        "setup_completed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "user_context": get_user_context()
    }
    
    # 次のステップ案内
    st.markdown("""
    ---
    ### 🚀 次のステップ
    
    1. **メインアプリケーションにアクセス**
    2. **データソースを選択**
    3. **条件を設定して保存**
    4. **設定を読み込んで再利用**
    
    💡 **ヒント**: サンプルデータを挿入した場合は、メインアプリで「設定を再読み込み」を実行してください。
    """)
    
    # 設定ファイルのエクスポート
    if st.button("💾 初期設定をエクスポート", key="export_setup", use_container_width=True):
        setup_export = {
            "setup_info": setup_config,
            "table_structure": {
                "table_name": CONFIG_TABLE_NAME,
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "features": [
                    "JSON設定保存",
                    "ユーザー分離",
                    "バージョン管理",
                    "タグ機能",
                    "インデックス最適化"
                ]
            }
        }
        
        setup_json = json.dumps(setup_export, ensure_ascii=False, indent=2)
        st.download_button(
            label="📥 設定ファイルをダウンロード",
            data=setup_json,
            file_name=f"sql_tool_setup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )
    
    st.session_state.setup_completed = True


def render_progress_tracker():
    """進捗追跡バー"""
    progress_steps = [
        ("接続確認", st.session_state.connection_verified),
        ("テーブル作成", st.session_state.table_created),
        ("完了", st.session_state.setup_completed)
    ]
    
    completed_steps = sum(1 for _, completed in progress_steps if completed)
    progress_percent = (completed_steps / len(progress_steps)) * 100
    
    st.markdown(f"""
    <div class="progress-container">
        <div class="progress-bar" style="width: {progress_percent}%"></div>
    </div>
    <p style="text-align: center; color: #475569; margin-top: 0.5rem;">
        進捗: {completed_steps}/{len(progress_steps)} ステップ完了 ({progress_percent:.0f}%)
    </p>
    """, unsafe_allow_html=True)
    
    # ステップ詳細
    col1, col2, col3 = st.columns(3)
    
    with col1:
        icon = "✅" if progress_steps[0][1] else "⏳"
        st.markdown(f"**{icon} 1. {progress_steps[0][0]}**")
    
    with col2:
        icon = "✅" if progress_steps[1][1] else "⏳"
        st.markdown(f"**{icon} 2. {progress_steps[1][0]}**")
    
    with col3:
        icon = "✅" if progress_steps[2][1] else "⏳"
        st.markdown(f"**{icon} 3. {progress_steps[2][0]}**")


def render_troubleshooting():
    """トラブルシューティングセクション"""
    with st.expander("🔧 トラブルシューティング", expanded=False):
        st.markdown("""
        ### よくある問題と解決方法
        
        #### 🔌 接続エラー
        **問題**: Snowflake接続に失敗する
        **解決方法**:
        - Streamlit in Snowflake環境で実行していることを確認
        - 適切なデータベースとスキーマが選択されていることを確認
        - 必要な権限があることを確認
        
        #### 🗄️ テーブル作成エラー
        **問題**: テーブル作成に失敗する
        **解決方法**:
        - CREATE TABLE権限があることを確認
        - スキーマへの書き込み権限があることを確認
        - テーブル名が既存のオブジェクトと重複していないか確認
        
        #### 📝 データ挿入エラー
        **問題**: サンプルデータの挿入に失敗する
        **解決方法**:
        - INSERT権限があることを確認
        - テーブルが正常に作成されていることを確認
        - JSON形式のデータが正しいことを確認
        
        #### 🔄 権限確認コマンド
        ```sql
        -- 現在の権限を確認
        SHOW GRANTS TO USER CURRENT_USER();
        
        -- スキーマの権限を確認
        SHOW GRANTS ON SCHEMA CURRENT_SCHEMA();
        ```
        """)


def main():
    """メイン処理"""
    # セッション初期化
    init_session_state()
    
    # Snowflakeセッション取得
    session = init_snowflake_session()
    
    if session is None:
        st.error("🚫 Snowflakeセッションの初期化に失敗しました")
        st.markdown("""
        **解決方法:**
        - Streamlit in Snowflake環境で実行していることを確認してください
        - 適切なデータベースとスキーマが選択されていることを確認してください
        """)
        st.stop()
    
    # ヘッダー
    st.markdown('<div class="header-title">⚙️ SQLレスデータ抽出ツール</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">初期設定 - 設定保存テーブルのセットアップ</div>', unsafe_allow_html=True)
    
    # 進捗追跡
    render_progress_tracker()
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # セットアップフロー
    render_connection_check()
    
    if st.session_state.setup_step >= 2:
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        render_table_setup()
    
    if st.session_state.setup_step >= 3:
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        render_sample_data()
        
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        render_completion()
    
    # サイドバー
    with st.sidebar:
        st.markdown("## 📋 セットアップガイド")
        
        st.markdown("""
        ### 🎯 セットアップの目的
        SQLレスデータ抽出ツールの設定を保存するための
        Snowflakeテーブルを作成します。
        
        ### 📋 セットアップ手順
        1. **接続確認**: Snowflakeへの接続を確認
        2. **テーブル作成**: 設定保存用テーブルを作成
        3. **サンプルデータ**: 使用例となるデータを挿入（オプション）
        4. **完了**: メインアプリケーションで使用開始
        
        ### 💡 重要なポイント
        - テーブルは現在のスキーマに作成されます
        - 各ユーザーの設定は分離されて保存されます
        - JSON形式で柔軟な設定保存が可能
        - インデックスによる高速検索をサポート
        """)
        
        st.markdown("---")
        
        # 現在の状態表示
        st.markdown("### 📊 現在の状態")
        
        status_items = [
            ("🔌 接続確認", st.session_state.connection_verified),
            ("🗄️ テーブル作成", st.session_state.table_created),
            ("📝 サンプルデータ", st.session_state.sample_data_inserted),
            ("✅ セットアップ完了", st.session_state.setup_completed)
        ]
        
        for item_name, status in status_items:
            icon = "✅" if status else "⏳"
            st.markdown(f"{icon} {item_name}")
        
        st.markdown("---")
        
        # リセット機能
        if st.button("🔄 セットアップをリセット", use_container_width=True):
            if st.session_state.get("confirm_reset", False):
                # セッション状態をリセット
                for key in ['setup_completed', 'connection_verified', 'table_created', 'sample_data_inserted']:
                    st.session_state[key] = False
                st.session_state.setup_step = 1
                st.session_state.confirm_reset = False
                st.success("✅ セットアップをリセットしました")
                st.rerun()
            else:
                st.session_state.confirm_reset = True
                st.warning("⚠️ セットアップをリセットしますか？もう一度ボタンを押してください。")
                st.rerun()
    
    # フッターセクション
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # トラブルシューティング
    render_troubleshooting()
    
    # フッター
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #475569; font-size: 0.9rem; padding: 1rem;">
        ⚙️ SQLレスデータ抽出ツール - 初期設定ページ v1.0<br>
        Snowflakeテーブルベース永続化システム
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()