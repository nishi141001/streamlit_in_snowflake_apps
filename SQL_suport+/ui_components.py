"""
UIコンポーネント
SQLレスデータ抽出ツール用
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import json
import time
from datetime import datetime
from snowflake_utils import get_dynamic_columns, get_table_schema, init_snowflake_session
from config_manager import (
    save_config_to_table, delete_config_from_table, update_last_used, 
    force_reload_configs, check_config_table_exists
)


def get_custom_css():
    """カスタムCSSを返す"""
    return """
    <style>
    .main > div {
        padding-top: 1rem;
    }

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

    .error-message {
        background: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        color: #dc2626;
        font-size: 0.9rem;
    }

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

    .divider {
        border-top: 2px solid #DAF1FF;
        margin: 2rem 0;
    }

    .saved-config {
        background: white;
        border: 1px solid #DAF1FF;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 10px rgba(99, 192, 246, 0.1);
    }
    </style>
    """


def render_dynamic_filters():
    """動的フィルターUI"""
    if not st.session_state.selected_table:
        st.info("テーブルを選択すると、そのテーブルに応じた絞り込み条件が表示されます")
        return
    
    st.markdown("### 🔍 絞り込み条件")
    
    dynamic_columns = get_dynamic_columns(
        st.session_state.selected_table,
        st.session_state.selected_db,
        st.session_state.selected_schema
    )
    
    if not dynamic_columns:
        st.warning("カラム情報を取得できませんでした。テーブルの選択を確認してください。")
        return
    
    # 新しい条件を追加
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
        
        if col_name not in dynamic_columns:
            st.warning(f"カラム '{col_name}' は現在のテーブルに存在しません。")
            col_config = []
        else:
            col_config = dynamic_columns[col_name]
        
        with st.expander(f"🔧 {col_name}", expanded=True):
            condition_type = st.selectbox(
                "条件タイプ",
                ["値を選択", "範囲指定", "カスタム条件"],
                index=["値を選択", "範囲指定", "カスタム条件"].index(condition["type"]),
                key=f"condition_type_{i}"
            )
            
            try:
                if condition_type == "値を選択":
                    if isinstance(col_config, list) and len(col_config) > 0:
                        selected_values = st.multiselect(
                            f"{col_name}の値",
                            col_config,
                            key=f"select_{i}"
                        )
                        if selected_values:
                            conditions[f"{col_name}_in"] = selected_values
                    else:
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
                        date_from = st.date_input(f"{col_name} 開始", key=f"date_from_{i}")
                        date_to = st.date_input(f"{col_name} 終了", key=f"date_to_{i}")
                        if date_from or date_to:
                            conditions[f"{col_name}_range"] = {"from": date_from, "to": date_to}
                    
                    elif col_config == "numeric_range":
                        min_val = st.number_input(f"{col_name} 最小値", key=f"min_{i}")
                        max_val = st.number_input(f"{col_name} 最大値", key=f"max_{i}")
                        if min_val != 0 or max_val != 0:
                            conditions[f"{col_name}_range"] = {"min": min_val, "max": max_val}
                    else:
                        range_type = st.selectbox(
                            "範囲タイプ",
                            ["前方一致", "後方一致", "部分一致"],
                            key=f"range_type_{i}"
                        )
                        range_value = st.text_input(f"検索文字列", key=f"range_value_{i}")
                        if range_value:
                            conditions[f"{col_name}_like"] = {"type": range_type, "value": range_value}
                
                elif condition_type == "カスタム条件":
                    custom_condition = st.text_area(
                        f"{col_name}のカスタム条件",
                        placeholder=f"例: {col_name} > 1000 OR {col_name} IS NULL",
                        key=f"custom_{i}"
                    )
                    if custom_condition:
                        conditions[f"{col_name}_custom"] = custom_condition
            
            except Exception as e:
                st.error(f"条件設定エラー: {str(e)}")
            
            if st.button("🗑️ 条件を削除", key=f"delete_{i}"):
                st.session_state.filter_conditions.pop(i)
                st.rerun()
    
    # 集計設定
    with st.expander("📊 集計設定"):
        available_group_columns = list(dynamic_columns.keys())
        
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
    """JOIN設定UI"""
    st.markdown("### 🔗 テーブル結合")
    
    if not st.session_state.selected_table:
        st.info("メインテーブルを選択してください")
        return
    
    # 利用可能なテーブル取得
    available_tables = []
    try:
        if st.session_state.selected_db and st.session_state.selected_schema:
            # セッション状態からメタデータを取得
            snowflake_metadata = st.session_state.get('snowflake_metadata', {})
            if not snowflake_metadata:
                # メタデータがセッションにない場合は取得
                session = init_snowflake_session()
                if session:
                    from snowflake_utils import get_snowflake_metadata
                    snowflake_metadata = get_snowflake_metadata(session)
                    if snowflake_metadata:
                        st.session_state.snowflake_metadata = snowflake_metadata
            
            schema_tables = snowflake_metadata.get(st.session_state.selected_db, {}).get("schemas", {}).get(st.session_state.selected_schema, [])
            available_tables = [t for t in schema_tables if t != st.session_state.selected_table]
    except Exception as e:
        st.warning(f"利用可能なテーブル一覧の取得に失敗: {str(e)}")
    
    if not available_tables:
        st.info("結合可能なテーブルがありません")
        return
    
    # 新しいJOINを追加
    with st.expander("➕ JOINを追加", expanded=False):
        join_table = st.selectbox("結合するテーブル", available_tables, key="new_join_table")
        
        if join_table:
            join_type = st.selectbox(
                "結合タイプ", 
                ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
                key="new_join_type"
            )
            
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
                    st.warning("カラム情報の取得に失敗しました。")
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
    
    # 既存のJOIN設定を表示
    for i, join_info in enumerate(st.session_state.join_conditions):
        with st.expander(f"🔗 {join_info['table']} ({join_info['type']})", expanded=True):
            try:
                join_type = st.selectbox(
                    "結合タイプ", 
                    ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
                    index=["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"].index(join_info['type']),
                    key=f"join_type_{i}"
                )
                
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
                    st.warning("カラム情報の取得に失敗しました。")
                    left_column = join_info.get('left_col', 'ERROR')
                    right_column = join_info.get('right_col', 'ERROR')
                else:
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
                
                st.session_state.join_conditions[i].update({
                    "type": join_type,
                    "left_col": left_column,
                    "right_col": right_column
                })
                
                join_sql = f"""
{join_type} {st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']} 
  ON {st.session_state.selected_table}.{left_column} = {join_info['table']}.{right_column}
                """.strip()
                
                st.markdown("**生成されるJOIN句:**")
                st.code(join_sql, language="sql")
                
            except Exception as e:
                st.error(f"JOIN設定 {i+1} でエラーが発生: {str(e)}")
            
            if st.button("🗑️ JOINを削除", key=f"delete_join_{i}"):
                st.session_state.join_conditions.pop(i)
                st.success("JOIN設定を削除しました")
                st.rerun()


def save_current_config():
    """現在の設定を保存"""
    if st.session_state.selected_table:
        config_name = st.text_input("設定名を入力", key="new_config_name")
        description = st.text_input("説明（オプション）", key="new_config_desc")
        
        tags_input = st.text_input(
            "タグ（カンマ区切り）", 
            key="new_config_tags",
            help="例: 売上,月次,レポート",
            placeholder="タグをカンマ区切りで入力"
        )
        
        tags = [tag.strip() for tag in tags_input.split(",") if tag.strip()] if tags_input else []
        
        if tags:
            st.markdown("**設定されるタグ:**")
            for tag in tags:
                st.write(f"🏷️ {tag}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 設定を保存", key="save_config_btn"):
                if config_name:
                    with st.spinner("設定を保存中..."):
                        new_config = {
                            "db": st.session_state.selected_db,
                            "schema": st.session_state.selected_schema,
                            "table": st.session_state.selected_table,
                            "conditions": st.session_state.query_conditions.copy(),
                            "join_conditions": st.session_state.join_conditions.copy(),
                            "filter_conditions": st.session_state.filter_conditions.copy()
                        }
                        
                        if save_config_to_table(config_name, new_config, description, tags):
                            force_reload_configs()
                            st.success(f"✅ 設定「{config_name}」をデータベースに保存しました")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("❌ 設定の保存に失敗しました")
                else:
                    st.error("❌ 設定名を入力してください")
        
        with col2:
            if st.button("📤 設定をエクスポート", key="export_config_btn"):
                if config_name:
                    export_config = {
                        config_name: {
                            "db": st.session_state.selected_db,
                            "schema": st.session_state.selected_schema,
                            "table": st.session_state.selected_table,
                            "description": description,
                            "tags": tags,
                            "conditions": st.session_state.query_conditions.copy(),
                            "join_conditions": st.session_state.join_conditions.copy(),
                            "filter_conditions": st.session_state.filter_conditions.copy(),
                            "exported_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                    }
                    
                    config_json = json.dumps(export_config, ensure_ascii=False, indent=2)
                    st.download_button(
                        label="💾 JSONファイルをダウンロード",
                        data=config_json,
                        file_name=f"sql_tool_config_{config_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                else:
                    st.info("設定名を入力してからエクスポートしてください")
    else:
        st.warning("⚠️ テーブルを選択してから設定を保存してください")


def render_charts(data):
    """グラフ表示"""
    if len(data) == 0:
        st.warning("表示するデータがありません")
        return
    
    try:
        date_columns = [col for col in data.columns if any(date_type in str(data[col].dtype).lower() 
                                                         for date_type in ['date', 'timestamp'])]
        
        numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
        
        category_columns = []
        for col in data.select_dtypes(include=['object']).columns:
            if data[col].nunique() < 20:
                category_columns.append(col)
        
        if date_columns and numeric_columns:
            st.subheader("📈 時系列推移")
            date_col = st.selectbox("日付カラム", date_columns, key="chart_date")
            value_col = st.selectbox("値カラム", numeric_columns, key="chart_value")
            
            if date_col and value_col:
                chart_data = data.copy()
                chart_data[date_col] = pd.to_datetime(chart_data[date_col])
                
                fig_line = px.line(chart_data, x=date_col, y=value_col, title=f"{date_col}別{value_col}推移")
                fig_line.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    title_font_color="#1e40af",
                    font_color="#475569"
                )
                fig_line.update_traces(line_color="#1FAEFF")
                st.plotly_chart(fig_line, use_container_width=True)
        
        if category_columns and numeric_columns:
            st.subheader("📊 カテゴリ別分析")
            col_a, col_b = st.columns(2)
            
            with col_a:
                category_col = st.selectbox("カテゴリカラム", category_columns, key="chart_category")
                value_col = st.selectbox("値カラム", numeric_columns, key="chart_category_value")
                
                if category_col and value_col:
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
        col1, col2 = st.columns(2)
        with col1:
            export_format = st.selectbox("ファイル形式", ["CSV", "Excel (XLSX)"], key="export_format")
            add_timestamp = st.checkbox("タイムスタンプ付きファイル名", value=True, key="add_timestamp")
        
        with col2:
            if export_format == "CSV":
                encoding = st.selectbox("エンコーディング", ["UTF-8", "Shift_JIS"], key="encoding")
            else:
                include_charts = st.checkbox("サマリーシートを含める", key="include_charts")
        
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
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                data.to_excel(writer, sheet_name='データ', index=False)
                
                if include_charts:
                    summary_data = pd.DataFrame({
                        "項目": ["総レコード数", "データ抽出日時"],
                        "値": [len(data), datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
                    
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
    """テーブル構造を表示"""
    if not st.session_state.selected_table:
        return
    
    st.markdown("### 📊 テーブル構造")
    
    try:
        session = init_snowflake_session()
        if not session:
            st.error("Snowflakeセッションが初期化されていません")
            return
            
        if st.session_state.join_conditions:
            join_tables = [join_info['table'] for join_info in st.session_state.join_conditions]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**📋 メインテーブル: {st.session_state.selected_table}**")
                
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
            
            with col2:
                first_join_table = join_tables[0]
                st.markdown(f"**🔗 結合テーブル: {first_join_table}**")
                
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
            
            st.markdown("### 🔗 結合条件")
            for i, join_info in enumerate(st.session_state.join_conditions):
                st.markdown(f"""
                <div style="background: #f1f5f9; border-left: 4px solid #1FAEFF; padding: 1rem; border-radius: 0 8px 8px 0; margin: 0.5rem 0; font-family: monospace; font-size: 0.9rem; color: #334155;">
                    <strong>JOIN {i+1}:</strong><br>
                    {join_info['type']} {st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']}<br>
                    ON {st.session_state.selected_table}.{join_info['left_col']} = {join_info['table']}.{join_info['right_col']}
                </div>
                """, unsafe_allow_html=True)
        
        else:
            st.markdown(f"**📋 メインテーブル: {st.session_state.selected_table}**")
            
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


def load_saved_config(config_name):
    """保存済み設定を読み込み"""
    try:
        if config_name in st.session_state.saved_configs:
            config = st.session_state.saved_configs[config_name]
            
            st.session_state.selected_db = config.get("db")
            st.session_state.selected_schema = config.get("schema")
            st.session_state.selected_table = config.get("table")
            st.session_state.query_conditions = config.get("conditions", {})
            st.session_state.join_conditions = config.get("join_conditions", [])
            st.session_state.filter_conditions = config.get("filter_conditions", [])
            
            update_last_used(config_name)
            st.session_state.saved_configs[config_name]["last_used"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            st.session_state.last_error = None
            st.session_state.query_validation_errors = []
            
            st.success(f"✅ 設定「{config_name}」を読み込みました")
            time.sleep(1)
            st.rerun()
        else:
            st.error(f"❌ 設定「{config_name}」が見つかりません")
    except Exception as e:
        st.error(f"❌ 設定の読み込みに失敗しました: {str(e)}")


def delete_saved_config(config_name):
    """設定を削除"""
    try:
        if config_name in st.session_state.saved_configs:
            if delete_config_from_table(config_name):
                del st.session_state.saved_configs[config_name]
                st.success(f"✅ 設定「{config_name}」を削除しました")
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"❌ 設定「{config_name}」の削除に失敗しました")
        else:
            st.warning(f"⚠️ 設定「{config_name}」が見つかりません")
    except Exception as e:
        st.error(f"❌ 設定削除中にエラー: {str(e)}")


def render_saved_configs():
    """保存済み設定の表示と管理"""
    st.markdown("### 💾 保存済み設定")
    
    if not check_config_table_exists():
        st.markdown("""
        <div style="background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color: white; padding: 1.5rem; border-radius: 10px; margin: 1rem 0;">
            <h3>❌ 初期設定が必要です</h3>
            <p>設定保存機能を使用するには、初期設定ページでテーブルを作成してください。</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 設定を再読み込み", key="reload_configs", use_container_width=True):
            with st.spinner("設定を読み込み中..."):
                force_reload_configs()
                st.success("設定を再読み込みしました")
                st.rerun()
    
    with col2:
        if st.session_state.saved_configs:
            total_configs = len(st.session_state.saved_configs)
            active_configs = sum(1 for config in st.session_state.saved_configs.values() 
                               if config.get('last_used'))
            st.metric("設定数", total_configs)
            st.caption(f"使用済み: {active_configs}件")
    
    if st.session_state.saved_configs:
        # 検索・フィルター機能
        with st.expander("🔍 検索・フィルター", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                search_term = st.text_input(
                    "設定名で検索",
                    key="config_search",
                    placeholder="設定名を入力..."
                )
            
            with col2:
                all_tags = set()
                for config in st.session_state.saved_configs.values():
                    if 'tags' in config and config['tags']:
                        all_tags.update(config['tags'])
                
                selected_tags = st.multiselect(
                    "タグでフィルター",
                    list(all_tags),
                    key="tag_filter"
                )
        
        # 設定一覧の表示オプション
        col1, col2 = st.columns(2)
        
        with col1:
            view_mode = st.selectbox(
                "表示形式",
                ["リスト表示", "カード表示"],
                key="config_view_mode"
            )
        
        with col2:
            sort_option = st.selectbox(
                "並び順",
                ["更新日時（新しい順）", "更新日時（古い順）", "設定名（A-Z）", "最終使用日時"],
                key="config_sort_option"
            )
        
        # フィルタリング
        filtered_configs = {}
        for name, config in st.session_state.saved_configs.items():
            if search_term and search_term.lower() not in name.lower():
                continue
            
            if selected_tags:
                config_tags = config.get('tags', [])
                if not any(tag in config_tags for tag in selected_tags):
                    continue
            
            filtered_configs[name] = config
        
        # ソート処理
        configs_list = list(filtered_configs.items())
        
        if sort_option == "更新日時（新しい順）":
            configs_list.sort(key=lambda x: x[1].get('updated_at', ''), reverse=True)
        elif sort_option == "更新日時（古い順）":
            configs_list.sort(key=lambda x: x[1].get('updated_at', ''))
        elif sort_option == "設定名（A-Z）":
            configs_list.sort(key=lambda x: x[0])
        elif sort_option == "最終使用日時":
            configs_list.sort(key=lambda x: x[1].get('last_used', ''), reverse=True)
        
        if not configs_list:
            st.info("🔍 条件に一致する設定が見つかりませんでした")
            return
        
        st.info(f"📊 {len(configs_list)}件の設定が見つかりました")
        
        # 設定の表示
        if view_mode == "リスト表示":
            for config_name, config in configs_list:
                with st.container():
                    col1, col2, col3, col4 = st.columns([4, 2, 1, 1])
                    
                    with col1:
                        st.markdown(f"**📋 {config_name}**")
                        if config.get('description'):
                            st.caption(f"説明: {config['description']}")
                        
                        if config.get('tags'):
                            tag_html = " ".join([f'<span style="background: #e3f2fd; color: #1976d2; padding: 0.2rem 0.5rem; border-radius: 1rem; font-size: 0.75rem; margin-right: 0.3rem;">{tag}</span>' for tag in config['tags']])
                            st.markdown(tag_html, unsafe_allow_html=True)
                        
                        st.caption(f"テーブル: {config.get('db', 'N/A')}.{config.get('schema', 'N/A')}.{config.get('table', 'N/A')}")
                    
                    with col2:
                        created_at = config.get('created_at', '不明')
                        if created_at and len(str(created_at)) > 10:
                            created_at = str(created_at)[:10]
                        st.caption(f"作成: {created_at}")
                        
                        version = config.get('version', 1)
                        st.caption(f"v{version}")
                    
                    with col3:
                        if st.button("📂", key=f"load_{config_name}_list", help="読み込み"):
                            with st.spinner(f"設定「{config_name}」を読み込み中..."):
                                load_saved_config(config_name)
                    
                    with col4:
                        if st.button("🗑️", key=f"delete_{config_name}_list", help="削除"):
                            if st.session_state.get(f"confirm_delete_{config_name}", False):
                                delete_saved_config(config_name)
                                st.session_state[f"confirm_delete_{config_name}"] = False
                            else:
                                st.session_state[f"confirm_delete_{config_name}"] = True
                                st.warning(f"「{config_name}」を削除しますか？もう一度🗑️ボタンを押してください。")
                                st.rerun()
                    
                    st.markdown("---")
        
        else:  # カード表示
            for i in range(0, len(configs_list), 2):
                col1, col2 = st.columns(2)
                
                for j, col in enumerate([col1, col2]):
                    if i + j < len(configs_list):
                        config_name, config = configs_list[i + j]
                        
                        with col:
                            with st.container():
                                st.markdown(f"""
                                <div class="saved-config">
                                    <h4 style="color: #1e40af; margin-bottom: 0.5rem;">📋 {config_name}</h4>
                                    <p><strong>テーブル:</strong> {config.get('db', 'N/A')}.{config.get('schema', 'N/A')}.{config.get('table', 'N/A')}</p>
                                    <p><strong>説明:</strong> {config.get('description', '説明なし')}</p>
                                    <p><strong>作成日時:</strong> {str(config.get('created_at', '不明'))[:19] if config.get('created_at') else '不明'}</p>
                                    <p><strong>最終使用:</strong> {str(config.get('last_used', '未使用'))[:19] if config.get('last_used') else '未使用'}</p>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                if config.get('tags'):
                                    st.markdown("**タグ:**")
                                    for tag in config['tags']:
                                        st.write(f"🏷️ {tag}")
                                
                                button_col1, button_col2, button_col3 = st.columns(3)
                                
                                with button_col1:
                                    if st.button(f"📂", key=f"load_{config_name}_card_{i}_{j}", help="読み込み"):
                                        with st.spinner(f"設定「{config_name}」を読み込み中..."):
                                            load_saved_config(config_name)
                                
                                with button_col2:
                                    if st.button(f"📤", key=f"export_{config_name}_card_{i}_{j}", help="エクスポート"):
                                        export_single_config(config_name, config)
                                
                                with button_col3:
                                    if st.button(f"🗑️", key=f"delete_{config_name}_card_{i}_{j}", help="削除"):
                                        delete_saved_config(config_name)
        
        # 一括操作
        st.markdown("---")
        st.markdown("#### 🔧 一括操作")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📤 全設定をエクスポート", use_container_width=True):
                export_all_configs()
        
        with col2:
            if st.button("🗑️ 全設定を削除", use_container_width=True):
                if st.session_state.get("confirm_delete_all", False):
                    delete_all_configs()
                    st.session_state.confirm_delete_all = False
                else:
                    st.session_state.confirm_delete_all = True
                    st.warning("⚠️ 全ての設定を削除しますか？もう一度ボタンを押してください。")
                    st.rerun()
        
        with col3:
            render_import_section()
    
    else:
        st.info("💡 保存済み設定がありません")
        st.markdown("""
        **設定を保存するには:**
        1. データソースとフィルター条件を設定
        2. 下部の「現在の設定を保存」で設定名を入力
        3. 「設定を保存」ボタンを押下
        """)
        
        st.markdown("---")
        render_import_section()


def render_import_section():
    """インポート機能のセクション"""
    st.markdown("#### 📥 設定のインポート")
    
    uploaded_file = st.file_uploader(
        "JSONファイルから設定を読み込み",
        type="json",
        key="import_config_file",
        help="エクスポートしたJSONファイルから設定を読み込みます"
    )
    
    if uploaded_file is not None:
        try:
            config_data = json.load(uploaded_file)
            
            if not isinstance(config_data, dict):
                st.error("❌ 無効なファイル形式です")
                return
            
            st.markdown("**インポートする設定:**")
            
            import_selections = {}
            for name, config in config_data.items():
                if isinstance(config, dict) and 'db' in config:
                    import_selections[name] = st.checkbox(
                        f"📋 {name} - {config.get('db', 'N/A')}.{config.get('schema', 'N/A')}.{config.get('table', 'N/A')}",
                        value=True,
                        key=f"import_select_{name}"
                    )
                    if config.get('description'):
                        st.caption(f"説明: {config['description']}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("📥 選択した設定をインポート", key="execute_import"):
                    imported_count = 0
                    failed_count = 0
                    
                    with st.spinner("設定をインポート中..."):
                        for name, should_import in import_selections.items():
                            if should_import and name in config_data:
                                config = config_data[name]
                                description = config.get('description', '')
                                tags = config.get('tags', [])
                                
                                clean_config = {
                                    "db": config.get('db'),
                                    "schema": config.get('schema'),
                                    "table": config.get('table'),
                                    "conditions": config.get('conditions', {}),
                                    "join_conditions": config.get('join_conditions', []),
                                    "filter_conditions": config.get('filter_conditions', [])
                                }
                                
                                if save_config_to_table(name, clean_config, description, tags):
                                    imported_count += 1
                                else:
                                    failed_count += 1
                    
                    if imported_count > 0:
                        force_reload_configs()
                        
                        st.success(f"✅ {imported_count}件の設定をインポートしました")
                        if failed_count > 0:
                            st.warning(f"⚠️ {failed_count}件の設定のインポートに失敗しました")
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error("❌ インポートに失敗しました")
            
            with col2:
                if st.button("❌ キャンセル", key="cancel_import"):
                    st.rerun()
                    
        except json.JSONDecodeError:
            st.error("❌ JSONファイルの形式が正しくありません")
        except Exception as e:
            st.error(f"❌ インポート処理中にエラー: {str(e)}")


def export_single_config(config_name, config):
    """単一設定のエクスポート"""
    export_data = {config_name: config}
    config_json = json.dumps(export_data, ensure_ascii=False, indent=2)
    
    st.download_button(
        label=f"📥 {config_name}.json をダウンロード",
        data=config_json,
        file_name=f"sql_tool_config_{config_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json",
        key=f"download_{config_name}",
        use_container_width=True
    )


def export_all_configs():
    """全設定のエクスポート"""
    if st.session_state.saved_configs:
        config_json = json.dumps(st.session_state.saved_configs, ensure_ascii=False, indent=2)
        
        st.download_button(
            label="📥 全設定をダウンロード",
            data=config_json,
            file_name=f"sql_tool_all_configs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            key="download_all_configs",
            use_container_width=True
        )


def delete_all_configs():
    """全設定を削除"""
    try:
        from config_manager import get_user_context, CONFIG_TABLE_NAME
        from snowflake_utils import init_snowflake_session
        
        session = init_snowflake_session()
        if not session:
            st.error("Snowflakeセッションが初期化されていません")
            return
            
        user_context = get_user_context()
        
        delete_query = f"""
        UPDATE {CONFIG_TABLE_NAME}
        SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP()
        WHERE user_context = '{user_context}'
        """
        
        session.sql(delete_query).collect()
        st.session_state.saved_configs = {}
        st.success("✅ 全ての設定を削除しました")
        time.sleep(1)
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ 全設定の削除に失敗: {str(e)}")