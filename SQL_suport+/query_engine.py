"""
クエリ生成・実行エンジン
SQLレスデータ抽出ツール用
"""

import streamlit as st
import pandas as pd
import time
from snowflake_utils import init_snowflake_session, get_dynamic_columns


def validate_query_before_execution():
    """クエリ実行前の検証"""
    errors = []
    warnings = []
    
    if not st.session_state.selected_table:
        errors.append("メインテーブルが選択されていません")
    
    for i, join_info in enumerate(st.session_state.join_conditions):
        if not join_info.get('table'):
            errors.append(f"JOIN {i+1}: 結合テーブルが指定されていません")
        if not join_info.get('left_col') or not join_info.get('right_col'):
            errors.append(f"JOIN {i+1}: 結合カラムが指定されていません")
        
        try:
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
    
    main_columns = get_dynamic_columns(
        st.session_state.selected_table,
        st.session_state.selected_db,
        st.session_state.selected_schema
    )
    
    for condition in st.session_state.filter_conditions:
        col_name = condition.get('column')
        if col_name and col_name not in main_columns:
            warnings.append(f"フィルター条件: カラム '{col_name}' がメインテーブルに見つかりません")
    
    return errors, warnings


def generate_sql_query():
    """SQLクエリを生成"""
    try:
        base_table = f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
        
        group_by_cols = st.session_state.query_conditions.get('group_by', [])
        if group_by_cols:
            select_cols = group_by_cols + ["COUNT(*) as record_count"]
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
        
        sql_parts.append(f"FROM {base_table}")
        
        if st.session_state.join_conditions:
            for join_info in st.session_state.join_conditions:
                join_table = f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{join_info['table']}"
                sql_parts.append(f"{join_info['type']} {join_table}")
                sql_parts.append(f"  ON {st.session_state.selected_table}.{join_info['left_col']} = {join_info['table']}.{join_info['right_col']}")
        
        where_conditions = []
        
        for key, value in st.session_state.query_conditions.items():
            if not value or key in ['group_by', 'sort_column', 'sort_order', 'limit_rows']:
                continue
            
            try:
                if key.endswith('_in'):
                    col_name = key.replace('_in', '')
                    if isinstance(value, list) and value:
                        escaped_values = [str(v).replace("'", "''") for v in value]
                        values_str = "', '".join(escaped_values)
                        where_conditions.append(f"{col_name} IN ('{values_str}')")
                
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
                
                elif key.endswith('_like'):
                    col_name = key.replace('_like', '')
                    if isinstance(value, dict) and 'value' in value:
                        search_value = str(value['value']).replace("'", "''")
                        like_type = value['type']
                        
                        if like_type == "前方一致":
                            where_conditions.append(f"{col_name} LIKE '{search_value}%'")
                        elif like_type == "後方一致":
                            where_conditions.append(f"{col_name} LIKE '%{search_value}'")
                        elif like_type == "部分一致":
                            where_conditions.append(f"{col_name} LIKE '%{search_value}%'")
                
                elif key.endswith('_custom'):
                    if value and isinstance(value, str):
                        where_conditions.append(f"({value})")
            
            except Exception as e:
                st.warning(f"条件 {key} の処理中にエラー: {str(e)}")
        
        if where_conditions:
            sql_parts.append("WHERE " + "\n  AND ".join(where_conditions))
        
        if group_by_cols:
            sql_parts.append(f"GROUP BY {', '.join(group_by_cols)}")
        
        if st.session_state.query_conditions.get('sort_column'):
            sort_col = st.session_state.query_conditions['sort_column']
            sort_order = st.session_state.query_conditions.get('sort_order', 'DESC')
            sql_parts.append(f"ORDER BY {sort_col} {sort_order}")
        
        limit_val = st.session_state.query_conditions.get('limit_rows', 1000)
        if limit_val and limit_val < 10000:
            sql_parts.append(f"LIMIT {limit_val}")
        
        return "\n".join(sql_parts)
    
    except Exception as e:
        st.error(f"SQLクエリの生成に失敗しました: {str(e)}")
        return None


def execute_query():
    """クエリを実行して結果を取得"""
    try:
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
        
        session = init_snowflake_session()
        if not session:
            st.error("Snowflakeセッションが初期化されていません")
            return None, None
        
        start_time = time.time()
        result = session.sql(query).collect()
        execution_time = time.time() - start_time
        
        if result:
            df = pd.DataFrame(result)
            st.session_state.last_error = None
            return df, execution_time
        return pd.DataFrame(), execution_time
    
    except Exception as e:
        error_msg = str(e)
        st.session_state.last_error = error_msg
        st.error(f"クエリの実行に失敗しました: {error_msg}")
        
        with st.expander("🔍 エラーの詳細情報", expanded=False):
            st.text(error_msg)
            
        try:
            query = generate_sql_query()
            if query:
                with st.expander("📝 実行されたSQL", expanded=False):
                    st.code(query, language="sql")
        except:
            pass
        
        return None, None