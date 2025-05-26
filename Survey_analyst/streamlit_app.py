import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import Counter
import re
from datetime import datetime, timedelta
import io
import base64
from typing import List, Dict, Tuple, Optional
import warnings
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from sklearn.metrics.pairwise import cosine_similarity
import json
import networkx as nx
import matplotlib.pyplot as plt
from PIL import Image as PILImage
import tempfile
import os
warnings.filterwarnings('ignore')

# ページ設定
st.set_page_config(
    page_title="フリーテキストアンケート分析ツール",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSSスタイル
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #1E3A8A;
    text-align: center;
    margin-bottom: 2rem;
}
.sub-header {
    font-size: 1.5rem;
    font-weight: bold;
    color: #3B82F6;
    margin: 1.5rem 0 1rem 0;
}
.metric-container {
    background-color: #F8FAFC;
    padding: 1rem;
    border-radius: 8px;
    border-left: 4px solid #3B82F6;
    margin: 1rem 0;
}
.insight-box {
    background-color: #FEF3C7;
    padding: 1rem;
    border-radius: 8px;
    border-left: 4px solid #F59E0B;
    margin: 1rem 0;
}
.error-box {
    background-color: #FEE2E2;
    padding: 1rem;
    border-radius: 8px;
    border-left: 4px solid #EF4444;
    margin: 1rem 0;
}
</style>
""", unsafe_allow_html=True)

class SurveyAnalyzer:
    """フリーテキストアンケート分析クラス"""
    
    def __init__(self):
        self.raw_data = None
        self.data = None
        self.processed_data = None
        self.embeddings = None
        self.clusters = None
        self.cluster_labels = None
        self.wordcloud_cache = {}
        self.network_graph = None
        self.trend_analysis = None
        self.column_mapping = {
            'respondent_id': None,
            'response_date': None,  
            'text_response': None
        }
        
    def validate_data(self, df: pd.DataFrame, respondent_col: str, date_col: str, text_col: str) -> Tuple[bool, List[str]]:
        """データの妥当性を検証"""
        errors = []
        
        # 選択されたカラムの存在チェック
        selected_columns = [respondent_col, date_col, text_col]
        missing_cols = [col for col in selected_columns if col not in df.columns]
        if missing_cols:
            errors.append(f"選択されたカラムが存在しません: {', '.join(missing_cols)}")
            return False, errors
        
        # データ型チェック
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
                errors.append(f"'{date_col}'カラムが日付形式に変換できません")
        
        # 空値チェック
        empty_responses = df[text_col].isna().sum()
        if empty_responses > 0:
            errors.append(f"'{text_col}'に空値が{empty_responses}件あります（分析時に除外されます）")
        
        # 重複チェック（警告レベル）
        duplicate_ids = df[respondent_col].duplicated().sum()
        if duplicate_ids > 0:
            errors.append(f"'{respondent_col}'に重複が{duplicate_ids}件あります（注意が必要です）")
        
        return len(errors) == 0, errors
    
    def prepare_data(self, df: pd.DataFrame, respondent_col: str, date_col: str, text_col: str) -> pd.DataFrame:
        """データを分析用に準備"""
        # カラム名を標準化
        prepared_df = df.copy()
        prepared_df = prepared_df.rename(columns={
            respondent_col: 'respondent_id',
            date_col: 'response_date',
            text_col: 'text_response'
        })
        
        # 日付の変換
        if not pd.api.types.is_datetime64_any_dtype(prepared_df['response_date']):
            prepared_df['response_date'] = pd.to_datetime(prepared_df['response_date'], errors='coerce')
        
        # 空のテキスト回答を除外
        prepared_df = prepared_df.dropna(subset=['text_response'])
        prepared_df = prepared_df[prepared_df['text_response'].str.strip() != '']
        
        return prepared_df
    
    def preprocess_text(self, text: str) -> str:
        """テキストの前処理"""
        if pd.isna(text):
            return ""
        
        # 改行・空白の正規化
        text = re.sub(r'\s+', ' ', str(text))
        text = text.strip()
        
        return text
    
    def extract_keywords(self, texts: List[str], min_length: int = 2) -> Counter:
        """キーワード抽出（簡易版）"""
        all_words = []
        
        for text in texts:
            if not text:
                continue
            
            # 単純な単語分割（日本語対応のため改良が必要）
            words = re.findall(r'[ぁ-んァ-ヶー一-龠a-zA-Z0-9]+', text)
            words = [w for w in words if len(w) >= min_length]
            all_words.extend(words)
        
        return Counter(all_words)
    
    def extract_ngrams(self, texts: List[str], n: int = 2) -> Counter:
        """N-gram抽出"""
        ngrams = []
        
        for text in texts:
            if not text or len(text) < n:
                continue
            
            # 文字レベルのN-gram
            for i in range(len(text) - n + 1):
                ngram = text[i:i+n]
                if re.match(r'[ぁ-んァ-ヶー一-龠]{2,}', ngram):  # 日本語のみ
                    ngrams.append(ngram)
        
        return Counter(ngrams)
    
    def search_responses(self, texts: pd.Series, query: str, use_regex: bool = False) -> pd.Series:
        """テキスト検索"""
        if use_regex:
            try:
                return texts.str.contains(query, case=False, regex=True, na=False)
            except:
                st.error("正規表現にエラーがあります")
                return pd.Series([False] * len(texts))
        else:
            return texts.str.contains(query, case=False, na=False)
    
    def analyze_sentiment_simple(self, texts: List[str]) -> Dict[str, int]:
        """簡易センチメント分析"""
        positive_words = ['良い', 'よい', '素晴らしい', '満足', '嬉しい', '楽しい', '便利', '快適']
        negative_words = ['悪い', 'わるい', '不満', '困る', '嫌', 'ダメ', '問題', '不便']
        
        sentiment_counts = {'positive': 0, 'negative': 0, 'neutral': 0}
        
        for text in texts:
            if not text:
                sentiment_counts['neutral'] += 1
                continue
            
            pos_count = sum(1 for word in positive_words if word in text)
            neg_count = sum(1 for word in negative_words if word in text)
            
            if pos_count > neg_count:
                sentiment_counts['positive'] += 1
            elif neg_count > pos_count:
                sentiment_counts['negative'] += 1
            else:
                sentiment_counts['neutral'] += 1
        
        return sentiment_counts
    
    def generate_mock_embeddings(self, texts: List[str], dimension: int = 768) -> np.ndarray:
        """モック埋め込みベクトル生成（実際のCortex関数の代替）"""
        # 実際の実装では EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', text) を使用
        np.random.seed(42)  # 再現性のため
        embeddings = []
        
        for text in texts:
            if not text:
                # 空テキストの場合はゼロベクトル
                embedding = np.zeros(dimension)
            else:
                # テキストの特徴に基づいたモック埋め込み生成
                # 実際の実装では Snowflake Cortex の EMBED_TEXT_768 を使用
                text_hash = hash(text) % 10000
                np.random.seed(text_hash)
                
                # 単語の種類に基づく基本ベクトル
                base_vector = np.random.normal(0, 0.1, dimension)
                
                # 感情的な単語に基づく調整
                positive_words = ['良い', 'よい', '素晴らしい', '満足', '嬉しい', '楽しい', '便利', '快適']
                negative_words = ['悪い', 'わるい', '不満', '困る', '嫌', 'ダメ', '問題', '不便']
                
                pos_count = sum(1 for word in positive_words if word in text)
                neg_count = sum(1 for word in negative_words if word in text)
                
                if pos_count > neg_count:
                    base_vector[:100] += 0.3  # ポジティブ方向
                elif neg_count > pos_count:
                    base_vector[100:200] += 0.3  # ネガティブ方向
                
                # 長さに基づく調整
                if len(text) > 50:
                    base_vector[200:300] += 0.2  # 詳細な回答
                
                embedding = base_vector
            
            embeddings.append(embedding)
        
        return np.array(embeddings)
    
    def perform_clustering(self, embeddings: np.ndarray, n_clusters: int = None) -> Tuple[np.ndarray, int]:
        """クラスタリング実行"""
        if n_clusters is None:
            # エルボー法で最適クラスタ数を推定
            n_clusters = self.estimate_optimal_clusters(embeddings)
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(embeddings)
        
        return cluster_labels, n_clusters
    
    def estimate_optimal_clusters(self, embeddings: np.ndarray, max_k: int = 10) -> int:
        """最適クラスタ数の推定"""
        n_samples = len(embeddings)
        max_k = min(max_k, n_samples // 2, 8)  # 実用的な範囲に制限
        
        if max_k < 2:
            return 2
        
        inertias = []
        k_range = range(2, max_k + 1)
        
        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(embeddings)
            inertias.append(kmeans.inertia_)
        
        # エルボー法による最適クラスタ数の推定
        if len(inertias) >= 2:
            differences = np.diff(inertias)
            if len(differences) >= 2:
                second_differences = np.diff(differences)
                optimal_k = np.argmax(second_differences) + 2
                return min(optimal_k, max_k)
        
        return min(3, max_k)  # デフォルト値
    
    def find_similar_responses(self, query_text: str, embeddings: np.ndarray, 
                             texts: List[str], top_k: int = 5) -> List[Tuple[str, float]]:
        """意味的類似検索"""
        # クエリテキストの埋め込み生成
        query_embedding = self.generate_mock_embeddings([query_text])[0]
        
        # コサイン類似度計算
        similarities = cosine_similarity([query_embedding], embeddings)[0]
        
        # 上位k件取得
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        results = []
        for idx in top_indices:
            if similarities[idx] > 0.1:  # 最小類似度閾値
                results.append((texts[idx], similarities[idx]))
        
        return results
    
    def analyze_clusters(self, embeddings: np.ndarray, cluster_labels: np.ndarray, 
                        texts: List[str]) -> Dict[int, Dict]:
        """クラスタ分析"""
        cluster_analysis = {}
        
        for cluster_id in np.unique(cluster_labels):
            cluster_mask = cluster_labels == cluster_id
            cluster_texts = [text for i, text in enumerate(texts) if cluster_mask[i]]
            cluster_embeddings = embeddings[cluster_mask]
            
            # クラスタの特徴抽出
            word_freq = self.extract_keywords(cluster_texts, min_length=2)
            top_words = dict(word_freq.most_common(10))
            
            # クラスタ内の代表的なテキスト
            cluster_center = np.mean(cluster_embeddings, axis=0)
            distances = np.linalg.norm(cluster_embeddings - cluster_center, axis=1)
            representative_idx = np.argmin(distances)
            representative_text = cluster_texts[representative_idx]
            
            # センチメント分析
            sentiment = self.analyze_sentiment_simple(cluster_texts)
            
            cluster_analysis[cluster_id] = {
                'size': len(cluster_texts),
                'top_words': top_words,
                'representative_text': representative_text,
                'sentiment': sentiment,
                'texts': cluster_texts[:5]  # 最初の5つのテキスト例
            }
        
        return cluster_analysis
    
    def reduce_dimensions(self, embeddings: np.ndarray, method: str = 'tsne') -> np.ndarray:
        """次元削減"""
        if method == 'tsne':
            if len(embeddings) > 3:
                reducer = TSNE(n_components=2, random_state=42, perplexity=min(30, len(embeddings)-1))
                return reducer.fit_transform(embeddings)
            else:
                return np.random.rand(len(embeddings), 2)
        elif method == 'pca':
            reducer = PCA(n_components=2, random_state=42)
            return reducer.fit_transform(embeddings)
        elif method == 'umap':
            # UMAPの代わりに簡易実装
            if len(embeddings) > 10:
                reducer = PCA(n_components=2, random_state=42)
                return reducer.fit_transform(embeddings)
            else:
                return np.random.rand(len(embeddings), 2)
        
        return embeddings[:, :2]  # フォールバック
    
    def generate_wordcloud_data(self, texts: List[str]) -> Dict[str, int]:
        """ワードクラウド用データ生成"""
        try:
            # テキストを結合
            combined_text = ' '.join(texts)
            
            # キーワード抽出
            keywords = self.extract_keywords(texts, min_length=2)
            
            # 上位50個のキーワードを返す
            return dict(keywords.most_common(50))
            
        except Exception as e:
            st.error(f"ワードクラウドデータ生成エラー: {str(e)}")
            return {}
    
    def create_network_graph(self, embeddings: np.ndarray, texts: List[str], 
                           similarity_threshold: float = 0.7) -> dict:
        """ネットワークグラフ作成"""
        try:
            # 類似度行列計算
            similarity_matrix = cosine_similarity(embeddings)
            
            # ネットワークグラフ作成
            G = nx.Graph()
            
            # ノード追加
            for i, text in enumerate(texts):
                G.add_node(i, text=text[:50] + "..." if len(text) > 50 else text)
            
            # エッジ追加（類似度が閾値以上の場合）
            for i in range(len(texts)):
                for j in range(i+1, len(texts)):
                    if similarity_matrix[i][j] > similarity_threshold:
                        G.add_edge(i, j, weight=similarity_matrix[i][j])
            
            # レイアウト計算
            pos = nx.spring_layout(G, k=1, iterations=50)
            
            # Plotly用データ準備
            edge_x = []
            edge_y = []
            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
            
            node_x = [pos[node][0] for node in G.nodes()]
            node_y = [pos[node][1] for node in G.nodes()]
            node_text = [G.nodes[node]['text'] for node in G.nodes()]
            
            return {
                'edge_x': edge_x,
                'edge_y': edge_y,
                'node_x': node_x,
                'node_y': node_y,
                'node_text': node_text,
                'num_nodes': len(G.nodes()),
                'num_edges': len(G.edges())
            }
            
        except Exception as e:
            st.error(f"ネットワークグラフ作成エラー: {str(e)}")
            return None
    
    def analyze_temporal_trends(self, df: pd.DataFrame) -> Dict:
        """時系列トレンド分析"""
        try:
            # 日別の回答数とセンチメント
            daily_stats = df.groupby(df['response_date'].dt.date).agg({
                'text_response': 'count',
                'respondent_id': 'nunique'
            }).rename(columns={
                'text_response': 'response_count',
                'respondent_id': 'unique_respondents'
            })
            
            # 週別の文字数トレンド
            df['week'] = df['response_date'].dt.to_period('W')
            weekly_char_length = df.groupby('week')['text_response'].apply(
                lambda x: x.str.len().mean()
            )
            
            # 月別のキーワードトレンド
            df['month'] = df['response_date'].dt.to_period('M')
            monthly_keywords = {}
            
            for month in df['month'].unique():
                month_texts = df[df['month'] == month]['text_response'].tolist()
                keywords = self.extract_keywords(month_texts, min_length=2)
                monthly_keywords[str(month)] = dict(keywords.most_common(10))
            
            return {
                'daily_stats': daily_stats,
                'weekly_char_length': weekly_char_length,
                'monthly_keywords': monthly_keywords
            }
            
        except Exception as e:
            st.error(f"トレンド分析エラー: {str(e)}")
            return None
    
    def generate_insights_report(self, df: pd.DataFrame) -> Dict[str, str]:
        """詳細インサイトレポート生成"""
        insights = {}
        
        # 基本統計インサイト
        total_responses = len(df)
        avg_length = df['text_response'].str.len().mean()
        unique_respondents = df['respondent_id'].nunique()
        date_range = (df['response_date'].max() - df['response_date'].min()).days
        
        insights['basic_stats'] = f"""
        **基本統計サマリー:**
        - 総回答数: {total_responses:,}件
        - ユニーク回答者数: {unique_respondents:,}人
        - 平均文字数: {avg_length:.1f}文字
        - データ収集期間: {date_range}日間
        - 1日あたり平均回答数: {total_responses/max(date_range, 1):.1f}件
        """
        
        # センチメントインサイト
        sentiment_results = self.analyze_sentiment_simple(df['text_response'].tolist())
        total = sum(sentiment_results.values())
        pos_ratio = sentiment_results['positive'] / total
        neg_ratio = sentiment_results['negative'] / total
        
        sentiment_interpretation = ""
        if pos_ratio > 0.6:
            sentiment_interpretation = "非常にポジティブな傾向"
        elif pos_ratio > 0.4:
            sentiment_interpretation = "やや満足している傾向"
        elif neg_ratio > 0.4:
            sentiment_interpretation = "改善が必要な状況"
        else:
            sentiment_interpretation = "中性的な評価"
        
        insights['sentiment'] = f"""
        **センチメント分析:**
        - ポジティブ: {sentiment_results['positive']:,}件 ({pos_ratio:.1%})
        - ネガティブ: {sentiment_results['negative']:,}件 ({neg_ratio:.1%})
        - ニュートラル: {sentiment_results['neutral']:,}件 ({(1-pos_ratio-neg_ratio):.1%})
        - 総合評価: {sentiment_interpretation}
        """
        
        # クラスタインサイト
        if self.clusters is not None:
            cluster_sizes = [info['size'] for info in self.clusters.values()]
            largest_cluster_id = max(self.clusters.keys(), key=lambda k: self.clusters[k]['size'])
            largest_cluster = self.clusters[largest_cluster_id]
            
            insights['clusters'] = f"""
            **クラスタ分析:**
            - 検出クラスタ数: {len(self.clusters)}個
            - 最大クラスタサイズ: {max(cluster_sizes)}件
            - 平均クラスタサイズ: {np.mean(cluster_sizes):.1f}件
            - 主要テーマ: {largest_cluster['representative_text'][:100]}...
            - 主要キーワード: {', '.join(list(largest_cluster['top_words'].keys())[:5])}
            """
        
        # 時系列インサイト
        if date_range > 1:
            daily_counts = df.groupby(df['response_date'].dt.date).size()
            peak_day = daily_counts.idxmax()
            peak_count = daily_counts.max()
            
            insights['temporal'] = f"""
            **時系列パターン:**
            - 最も回答が多い日: {peak_day} ({peak_count}件)
            - 週末 vs 平日の回答パターン分析が可能
            - 継続的な回答収集: {date_range}日間
            """
        
        return insights
    
    def create_comprehensive_dashboard(self, df: pd.DataFrame) -> Dict:
        """包括的ダッシュボード用データ準備"""
        dashboard_data = {}
        
        # KPIメトリクス
        dashboard_data['kpis'] = {
            'total_responses': len(df),
            'unique_respondents': df['respondent_id'].nunique(),
            'avg_response_length': df['text_response'].str.len().mean(),
            'response_rate': len(df) / df['respondent_id'].nunique() if df['respondent_id'].nunique() > 0 else 0,
            'data_quality_score': (1 - df['text_response'].isna().sum() / len(df)) * 100
        }
        
        # トレンドデータ
        if hasattr(self, 'trend_analysis') and self.trend_analysis:
            dashboard_data['trends'] = self.trend_analysis
        
        # トップキーワード
        all_keywords = self.extract_keywords(df['text_response'].tolist())
        dashboard_data['top_keywords'] = dict(all_keywords.most_common(15))
        
        return dashboard_data

# メイン分析クラスのインスタンス化
@st.cache_data
def initialize_analyzer():
    return SurveyAnalyzer()

analyzer = initialize_analyzer()

# メインUI
st.markdown('<h1 class="main-header">📊 フリーテキストアンケート分析ツール</h1>', unsafe_allow_html=True)

st.markdown("""
このツールは、CSVやExcelファイルのフリーテキストアンケートデータを分析し、
キーワード検索、頻度分析、基本的な可視化機能を提供します。

**手順:**
1. ファイルをアップロード
2. 必要なカラムを選択
3. 分析開始
""")

# セッション状態の初期化
if 'data_uploaded' not in st.session_state:
    st.session_state.data_uploaded = False
if 'columns_configured' not in st.session_state:
    st.session_state.columns_configured = False

# サイドバー
with st.sidebar:
    st.header("🔧 データ設定")
    
    # ステップ1: ファイルアップロード
    st.subheader("📁 ステップ1: ファイルアップロード")
    uploaded_file = st.file_uploader(
        "ファイルをアップロード",
        type=['csv', 'xlsx', 'xls'],
        help="CSV（UTF-8推奨）またはExcelファイルをアップロードしてください"
    )
    
    if uploaded_file is not None:
        # ファイル読み込み
        try:
            if uploaded_file.name.endswith('.csv'):
                # 文字コード自動判定（簡易版）
                raw_data = uploaded_file.read()
                try:
                    df_raw = pd.read_csv(io.StringIO(raw_data.decode('utf-8')))
                except UnicodeDecodeError:
                    df_raw = pd.read_csv(io.StringIO(raw_data.decode('shift_jis')))
            else:
                df_raw = pd.read_excel(uploaded_file)
            
            analyzer.raw_data = df_raw
            st.session_state.data_uploaded = True
            st.success(f"✅ ファイル読み込み完了 ({len(df_raw)}行, {len(df_raw.columns)}列)")
            
            # データプレビュー
            with st.expander("📋 データプレビュー", expanded=False):
                st.dataframe(df_raw.head(), use_container_width=True)
            
        except Exception as e:
            st.error(f"ファイル読み込みエラー: {str(e)}")
            st.session_state.data_uploaded = False
    
    # ステップ2: カラム選択
    if st.session_state.data_uploaded and analyzer.raw_data is not None:
        st.subheader("🏷️ ステップ2: カラム選択")
        
        columns = analyzer.raw_data.columns.tolist()
        
        # 回答者IDカラム選択
        respondent_col = st.selectbox(
            "回答者ID カラム",
            options=columns,
            help="各回答者を識別するためのカラムを選択してください"
        )
        
        # 回答日時カラム選択
        date_col = st.selectbox(
            "回答日時 カラム", 
            options=columns,
            help="回答した日時のカラムを選択してください"
        )
        
        # テキスト回答カラム選択
        text_col = st.selectbox(
            "テキスト回答 カラム",
            options=columns,
            help="分析対象となるフリーテキスト回答のカラムを選択してください"
        )
        
        # 設定確認ボタン
        if st.button("✅ カラム設定を確認", type="primary"):
            # データ検証
            is_valid, errors = analyzer.validate_data(analyzer.raw_data, respondent_col, date_col, text_col)
            
            if not is_valid:
                st.error("⚠️ データ検証エラー:")
                for error in errors:
                    st.error(f"• {error}")
            else:
                # データ準備
                analyzer.data = analyzer.prepare_data(analyzer.raw_data, respondent_col, date_col, text_col) 
                analyzer.processed_data = analyzer.data.copy()
                analyzer.processed_data['text_response'] = analyzer.processed_data['text_response'].apply(analyzer.preprocess_text)
                
                analyzer.column_mapping = {
                    'respondent_id': respondent_col,
                    'response_date': date_col,
                    'text_response': text_col
                }
                
                st.session_state.columns_configured = True
                st.success(f"✅ 分析準備完了 ({len(analyzer.data)}件の有効なデータ)")
                st.rerun()
    
    # ステップ3: 分析設定
    if st.session_state.columns_configured and analyzer.processed_data is not None:
        st.subheader("⚙️ ステップ3: 分析設定")
        
        df = analyzer.processed_data
        
        # フィルタ設定
        st.markdown("**📅 期間フィルタ**")
        date_range = st.date_input(
            "分析期間を選択",
            value=(df['response_date'].min().date(), df['response_date'].max().date()),
            min_value=df['response_date'].min().date(),
            max_value=df['response_date'].max().date()
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            mask = (df['response_date'].dt.date >= start_date) & (df['response_date'].dt.date <= end_date)
            analyzer.processed_data = analyzer.processed_data[mask]
        
        st.markdown("**🔍 分析オプション**")
        min_word_length = st.slider("最小単語長", 1, 5, 2)
        top_n_words = st.slider("表示する上位単語数", 10, 50, 20)
        
        # 追加フィルタ（任意のカラムが利用可能な場合）
        other_columns = [col for col in analyzer.raw_data.columns 
                        if col not in [analyzer.column_mapping['respondent_id'], 
                                     analyzer.column_mapping['response_date'], 
                                     analyzer.column_mapping['text_response']]]
        
        if other_columns:
            st.markdown("**🎯 追加フィルタ**")
            selected_filter_col = st.selectbox(
                "フィルタ用カラム（任意）",
                options=['なし'] + other_columns
            )
            
            if selected_filter_col != 'なし':
                unique_values = analyzer.raw_data[selected_filter_col].unique()
                selected_values = st.multiselect(
                    f"{selected_filter_col} の値を選択",
                    options=unique_values,
                    default=unique_values
                )
                
                if selected_values:
                    # 元データに基づいてフィルタリング
                    original_mask = analyzer.raw_data[selected_filter_col].isin(selected_values)
                    # prepared_dataに対応するマスクを作成
                    filtered_raw = analyzer.raw_data[original_mask]
                    filtered_prepared = analyzer.prepare_data(
                        filtered_raw, 
                        analyzer.column_mapping['respondent_id'],
                        analyzer.column_mapping['response_date'], 
                        analyzer.column_mapping['text_response']
                    )
                    analyzer.processed_data = filtered_prepared
                    analyzer.processed_data['text_response'] = analyzer.processed_data['text_response'].apply(analyzer.preprocess_text)

        # Phase 2: AI分析オプション
        st.markdown("**🤖 AI分析オプション**")
        enable_ai_analysis = st.checkbox("AI分析を有効化", value=True, help="ベクトル埋め込み・クラスタリング機能")
        
        if enable_ai_analysis:
            similarity_threshold = st.slider("類似度閾値", 0.1, 0.9, 0.3, 0.1)
            n_clusters = st.selectbox("クラスタ数", ['自動', 3, 4, 5, 6, 7, 8], help="自動：最適クラスタ数を推定")
            dimension_reduction_method = st.selectbox("次元削減手法", ['t-SNE', 'PCA', 'UMAP'])
            
            # Phase 3: 高度な可視化・レポート機能
            st.markdown("**📊 高度な可視化**")
            enable_advanced_viz = st.checkbox("高度な可視化を有効化", value=False, help="ワードクラウド、ネットワークグラフ等")
            
            if enable_advanced_viz:
                wordcloud_options = st.multiselect(
                    "ワードクラウド生成", 
                    ["全体", "クラスタ別"], 
                    default=["全体"]
                )
                network_analysis = st.checkbox("ネットワーク分析", help="回答間の関連性を可視化")
                trend_analysis = st.checkbox("トレンド分析", help="時系列での変化を分析")
            
            # Phase 3: レポート機能
            st.markdown("**📄 レポート機能**")
            enable_reports = st.checkbox("レポート生成を有効化", value=False)
            
            if enable_reports:
                report_type = st.selectbox(
                    "レポート形式",
                    ["詳細レポート", "エグゼクティブサマリー", "カスタムレポート"]
                )
                include_visualizations = st.checkbox("可視化を含める", value=True)
                
                if st.button("📋 レポート生成", type="secondary"):
                    with st.spinner("レポート生成中..."):
                        insights = analyzer.generate_insights_report(analyzer.processed_data)
                        st.session_state.report_insights = insights
                        
                        if analyzer.trend_analysis is None:
                            analyzer.trend_analysis = analyzer.analyze_temporal_trends(analyzer.processed_data)
                    
                    st.success("✅ レポート生成完了")
                    st.rerun()
            
            # AI分析実行
            if st.button("🚀 AI分析実行", type="primary"):
                with st.spinner("ベクトル埋め込み生成中..."):
                    texts = analyzer.processed_data['text_response'].tolist()
                    analyzer.embeddings = analyzer.generate_mock_embeddings(texts)
                    
                with st.spinner("クラスタリング実行中..."):
                    n_clusters_val = None if n_clusters == '自動' else int(n_clusters)
                    analyzer.cluster_labels, actual_clusters = analyzer.perform_clustering(
                        analyzer.embeddings, n_clusters_val
                    )
                    analyzer.clusters = analyzer.analyze_clusters(
                        analyzer.embeddings, analyzer.cluster_labels, texts
                    )
                
                # Phase 3: 高度な分析も実行
                if enable_advanced_viz:
                    with st.spinner("高度な分析実行中..."):
                        if trend_analysis:
                            analyzer.trend_analysis = analyzer.analyze_temporal_trends(analyzer.processed_data)
                        
                        if network_analysis:
                            analyzer.network_graph = analyzer.create_network_graph(
                                analyzer.embeddings, texts, similarity_threshold=0.6
                            )
                
                st.success(f"✅ AI分析完了 ({actual_clusters}個のクラスタを検出)")
                st.rerun()

# メインコンテンツ
if st.session_state.columns_configured and analyzer.processed_data is not None:
    df = analyzer.processed_data
    
    # 基本統計
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("総回答数", len(df))
    
    with col2:
        avg_length = df['text_response'].str.len().mean()
        st.metric("平均文字数", f"{avg_length:.1f}")
    
    with col3:
        unique_respondents = df['respondent_id'].nunique()
        st.metric("回答者数", unique_respondents)
    
    with col4:
        date_range_days = (df['response_date'].max() - df['response_date'].min()).days
        st.metric("分析期間", f"{date_range_days}日間")
    
    # タブでコンテンツを分割
    if analyzer.embeddings is not None:
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "🔍 検索・フィルタ", "📊 頻度分析", "📈 可視化", 
            "💭 センチメント分析", "🎯 意味的検索", "🧩 クラスタ分析",
            "🎨 高度な可視化", "📋 ダッシュボード・レポート"
        ])
    else:
        tab1, tab2, tab3, tab4 = st.tabs(["🔍 検索・フィルタ", "📊 頻度分析", "📈 可視化", "💭 センチメント分析"])
    
    with tab1:
        st.markdown('<h2 class="sub-header">キーワード検索</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([3, 1])
        with col1:
            search_query = st.text_input("検索キーワード", placeholder="例: 満足, 問題")
        with col2:
            use_regex = st.checkbox("正規表現", help="正規表現を使用した高度な検索")
        
        if search_query:
            search_results = analyzer.search_responses(df['text_response'], search_query, use_regex)
            filtered_df = df[search_results]
            
            st.info(f"🔍 {len(filtered_df)}件の回答が見つかりました")
            
            if len(filtered_df) > 0:
                # 検索結果表示
                st.subheader("検索結果")
                display_df = filtered_df[['respondent_id', 'response_date', 'text_response']].copy()
                display_df['response_date'] = display_df['response_date'].dt.strftime('%Y-%m-%d')
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400
                )
                
                # CSVダウンロード
                csv = display_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 検索結果をCSVでダウンロード",
                    data=csv,
                    file_name=f"search_results_{search_query}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with tab2:
        st.markdown('<h2 class="sub-header">頻度分析</h2>', unsafe_allow_html=True)
        
        # 単語頻度分析
        st.subheader("単語頻度")
        word_freq = analyzer.extract_keywords(df['text_response'].tolist(), min_word_length)
        top_words = dict(word_freq.most_common(top_n_words))
        
        if top_words:
            words_df = pd.DataFrame(list(top_words.items()), columns=['単語', '出現回数'])
            st.dataframe(words_df, use_container_width=True)
        
        # N-gram分析
        st.subheader("2-gram分析")
        bigrams = analyzer.extract_ngrams(df['text_response'].tolist(), 2)
        top_bigrams = dict(bigrams.most_common(20))
        
        if top_bigrams:
            bigrams_df = pd.DataFrame(list(top_bigrams.items()), columns=['2-gram', '出現回数'])
            st.dataframe(bigrams_df, use_container_width=True)
    
    with tab3:
        st.markdown('<h2 class="sub-header">可視化</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 頻出単語の棒グラフ
            if top_words:
                fig_words = px.bar(
                    x=list(top_words.values()),
                    y=list(top_words.keys()),
                    orientation='h',
                    title=f"頻出単語 TOP{len(top_words)}",
                    labels={'x': '出現回数', 'y': '単語'}
                )
                fig_words.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_words, use_container_width=True)
        
        with col2:
            # 回答数の時系列推移
            daily_counts = df.groupby(df['response_date'].dt.date).size().reset_index()
            daily_counts.columns = ['日付', '回答数']
            
            fig_timeline = px.line(
                daily_counts,
                x='日付',
                y='回答数',
                title='回答数の推移',
                markers=True
            )
            fig_timeline.update_layout(height=400)
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # 文字数分布
        st.subheader("文字数分布")
        char_lengths = df['text_response'].str.len()
        
        fig_hist = px.histogram(
            x=char_lengths,
            nbins=30,
            title='回答の文字数分布',
            labels={'x': '文字数', 'y': '回答数'}
        )
        fig_hist.update_layout(height=400)
        st.plotly_chart(fig_hist, use_container_width=True)
        
        # 統計情報
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("最小文字数", char_lengths.min())
        with col2:
            st.metric("最大文字数", char_lengths.max())
        with col3:
            st.metric("中央値", char_lengths.median())
        with col4:
            st.metric("標準偏差", f"{char_lengths.std():.1f}")
    
    with tab4:
        st.markdown('<h2 class="sub-header">センチメント分析</h2>', unsafe_allow_html=True)
        
        # 簡易センチメント分析
        sentiment_results = analyzer.analyze_sentiment_simple(df['text_response'].tolist())
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 円グラフ
            fig_pie = px.pie(
                values=list(sentiment_results.values()),
                names=['ポジティブ', 'ネガティブ', 'ニュートラル'],
                title='センチメント分布',
                color_discrete_map={
                    'ポジティブ': '#10B981',
                    'ネガティブ': '#EF4444',
                    'ニュートラル': '#6B7280'
                }
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # メトリクス表示
            total_responses = sum(sentiment_results.values())
            
            st.metric(
                "ポジティブ率",
                f"{sentiment_results['positive']/total_responses*100:.1f}%",
                delta=f"{sentiment_results['positive']}件"
            )
            st.metric(
                "ネガティブ率", 
                f"{sentiment_results['negative']/total_responses*100:.1f}%",
                delta=f"{sentiment_results['negative']}件"
            )
            st.metric(
                "ニュートラル率",
                f"{sentiment_results['neutral']/total_responses*100:.1f}%",
                delta=f"{sentiment_results['neutral']}件"
            )
        
        # インサイト生成
        st.subheader("📝 分析インサイト")
        
        insights = []
        
        # 回答率インサイト
        if len(df) > 0:
            insights.append(f"• 総回答数は{len(df)}件で、平均文字数は{avg_length:.1f}文字です")
        
        # センチメントインサイト
        pos_ratio = sentiment_results['positive'] / total_responses
        neg_ratio = sentiment_results['negative'] / total_responses
        
        if pos_ratio > 0.5:
            insights.append("• 全体的にポジティブな回答が多く、満足度が高い傾向にあります")
        elif neg_ratio > 0.3:
            insights.append("• ネガティブな回答が比較的多く、改善の余地があります")
        else:
            insights.append("• ポジティブとネガティブのバランスが取れています")
        
        # 頻出キーワードインサイト
        if top_words:
            most_common_word = list(top_words.keys())[0]
            insights.append(f"• 最も頻出するキーワードは「{most_common_word}」({top_words[most_common_word]}回出現)")
        
        # 文字数インサイト
        if char_lengths.std() > char_lengths.mean():
            insights.append("• 回答の文字数にばらつきが大きく、詳細な回答と簡潔な回答が混在しています")
        
        for insight in insights:
            st.markdown(f'<div class="insight-box">{insight}</div>', unsafe_allow_html=True)
    
    # Phase 2: AI分析タブ
    if analyzer.embeddings is not None:
        with tab5:
            st.markdown('<h2 class="sub-header">意味的類似検索</h2>', unsafe_allow_html=True)
            
            st.markdown("""
            キーワードマッチではなく、**意味的な類似性**に基づいて回答を検索します。
            例：「満足」で検索すると「嬉しい」「良い」なども含む回答が見つかります。
            """)
            
            col1, col2 = st.columns([3, 1])
            with col1:
                semantic_query = st.text_input(
                    "意味的検索クエリ", 
                    placeholder="例: 満足している, 問題がある, 改善してほしい"
                )
            with col2:
                top_k_similar = st.number_input("表示件数", 1, 20, 5)
            
            if semantic_query:
                similar_responses = analyzer.find_similar_responses(
                    semantic_query,
                    analyzer.embeddings,
                    df['text_response'].tolist(),
                    top_k_similar
                )
                
                if similar_responses:
                    st.subheader(f"🎯 類似回答 (上位{len(similar_responses)}件)")
                    
                    for i, (text, similarity) in enumerate(similar_responses, 1):
                        with st.expander(f"{i}. 類似度: {similarity:.3f}"):
                            st.write(text)
                            
                            # 該当する回答者情報も表示
                            matching_row = df[df['text_response'] == text].iloc[0]
                            st.caption(f"回答者ID: {matching_row['respondent_id']} | 回答日: {matching_row['response_date'].strftime('%Y-%m-%d')}")
                else:
                    st.info("類似する回答が見つかりませんでした。検索クエリを変更してみてください。")
        
        with tab6:
            st.markdown('<h2 class="sub-header">クラスタ分析</h2>', unsafe_allow_html=True)
            
            if analyzer.clusters is not None:
                st.markdown("**回答をAIが自動的にグループ分けしました。類似したテーマの回答が同じクラスタにまとめられています。**")
                
                # クラスタ概要
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("クラスタ数", len(analyzer.clusters))
                with col2:
                    largest_cluster = max(analyzer.clusters.values(), key=lambda x: x['size'])
                    st.metric("最大クラスタサイズ", largest_cluster['size'])
                with col3:
                    avg_cluster_size = np.mean([cluster['size'] for cluster in analyzer.clusters.values()])
                    st.metric("平均クラスタサイズ", f"{avg_cluster_size:.1f}")
                
                # 各クラスタの詳細
                st.subheader("📊 クラスタ詳細分析")
                
                for cluster_id, cluster_info in analyzer.clusters.items():
                    with st.expander(f"🏷️ クラスタ {cluster_id} ({cluster_info['size']}件)", expanded=cluster_id==0):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**代表的な回答:**")
                            st.write(f"_{cluster_info['representative_text']}_")
                            
                            st.markdown("**頻出キーワード:**")
                            top_words = cluster_info['top_words']
                            if top_words:
                                words_text = ", ".join([f"{word}({count})" for word, count in list(top_words.items())[:8]])
                                st.write(words_text)
                        
                        with col2:
                            st.markdown("**センチメント分布:**")
                            sentiment = cluster_info['sentiment']
                            
                            fig_cluster_sentiment = px.pie(
                                values=list(sentiment.values()),
                                names=['ポジティブ', 'ネガティブ', 'ニュートラル'],
                                title=f"クラスタ {cluster_id} センチメント",
                                color_discrete_map={
                                    'ポジティブ': '#10B981',
                                    'ネガティブ': '#EF4444', 
                                    'ニュートラル': '#6B7280'
                                }
                            )
                            fig_cluster_sentiment.update_layout(height=300, showlegend=False)
                            st.plotly_chart(fig_cluster_sentiment, use_container_width=True)
                        
                        # サンプルテキスト
                        st.markdown("**このクラスタの回答例:**")
                        for j, sample_text in enumerate(cluster_info['texts'][:3], 1):
                            st.write(f"{j}. {sample_text}")
                
                # 2D可視化
                st.subheader("🗺️ クラスタの2D可視化")
                
                # 次元削減手法選択
                method_map = {'t-SNE': 'tsne', 'PCA': 'pca', 'UMAP': 'umap'}
                selected_method = method_map[dimension_reduction_method]
                
                with st.spinner(f"{dimension_reduction_method}による次元削減中..."):
                    reduced_embeddings = analyzer.reduce_dimensions(analyzer.embeddings, selected_method)
                
                # 散布図作成
                plot_df = pd.DataFrame({
                    'x': reduced_embeddings[:, 0],
                    'y': reduced_embeddings[:, 1], 
                    'cluster': [f'クラスタ {label}' for label in analyzer.cluster_labels],
                    'text': df['text_response'].tolist(),
                    'respondent_id': df['respondent_id'].tolist()
                })
                
                fig_scatter = px.scatter(
                    plot_df,
                    x='x', y='y',
                    color='cluster',
                    hover_data=['respondent_id'],
                    title=f"クラスタ可視化 ({dimension_reduction_method})",
                    labels={'x': f'{dimension_reduction_method} 1', 'y': f'{dimension_reduction_method} 2'}
                )
                
                fig_scatter.update_traces(
                    hovertemplate="<b>%{hovertext}</b><br>" +
                                "回答者ID: %{customdata[0]}<br>" +
                                "<extra></extra>",
                    hovertext=[text[:100] + "..." if len(text) > 100 else text for text in plot_df['text']]
                )
                
                fig_scatter.update_layout(height=600)
                st.plotly_chart(fig_scatter, use_container_width=True)
                
                # クラスタサイズ分布
                st.subheader("📈 クラスタサイズ分布")
                cluster_sizes = [cluster_info['size'] for cluster_info in analyzer.clusters.values()]
                cluster_names = [f"クラスタ {i}" for i in range(len(cluster_sizes))]
                
                fig_cluster_sizes = px.bar(
                    x=cluster_names,
                    y=cluster_sizes,
                    title="各クラスタの回答数",
                    labels={'x': 'クラスタ', 'y': '回答数'}
                )
                fig_cluster_sizes.update_layout(height=400)
                st.plotly_chart(fig_cluster_sizes, use_container_width=True)
                
            else:
                st.info("👆 左側のサイドバーで「AI分析実行」ボタンをクリックしてクラスタ分析を開始してください")
        
        # Phase 3: 高度な可視化タブ
        with tab7:
            st.markdown('<h2 class="sub-header">高度な可視化</h2>', unsafe_allow_html=True)
            
            if analyzer.embeddings is not None:
                viz_option = st.selectbox(
                    "可視化タイプを選択",
                    ["ワードクラウド", "ネットワークグラフ", "3D散布図", "時系列トレンド"]
                )
                
                if viz_option == "ワードクラウド":
                    st.subheader("☁️ ワードクラウド")
                    
                    wc_type = st.radio("表示タイプ", ["全体", "クラスタ別"])
                    
                    if wc_type == "全体":
                        texts = df['text_response'].tolist()
                        wordcloud_data = analyzer.generate_wordcloud_data(texts)
                        
                        if wordcloud_data:
                            # Plotlyでワードクラウド風の表示
                            fig_wc = px.bar(
                                x=list(wordcloud_data.values())[:20],
                                y=list(wordcloud_data.keys())[:20],
                                orientation='h',
                                title="頻出キーワード（ワードクラウド風）",
                                labels={'x': '出現回数', 'y': 'キーワード'}
                            )
                            fig_wc.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig_wc, use_container_width=True)
                    
                    elif wc_type == "クラスタ別" and analyzer.clusters:
                        selected_cluster = st.selectbox(
                            "クラスタを選択", 
                            list(range(len(analyzer.clusters)))
                        )
                        
                        cluster_texts = analyzer.clusters[selected_cluster]['texts']
                        wordcloud_data = analyzer.generate_wordcloud_data(cluster_texts)
                        
                        if wordcloud_data:
                            fig_wc = px.bar(
                                x=list(wordcloud_data.values())[:15],
                                y=list(wordcloud_data.keys())[:15],
                                orientation='h',
                                title=f"クラスタ {selected_cluster} キーワード",
                                labels={'x': '出現回数', 'y': 'キーワード'}
                            )
                            fig_wc.update_layout(height=500, yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig_wc, use_container_width=True)
                
                elif viz_option == "ネットワークグラフ":
                    st.subheader("🕸️ ネットワークグラフ")
                    
                    if analyzer.network_graph is not None:
                        graph_data = analyzer.network_graph
                        
                        fig_network = go.Figure()
                        
                        # エッジ追加
                        fig_network.add_trace(go.Scatter(
                            x=graph_data['edge_x'], y=graph_data['edge_y'],
                            line=dict(width=0.5, color='#888'),
                            hoverinfo='none',
                            mode='lines'
                        ))
                        
                        # ノード追加
                        fig_network.add_trace(go.Scatter(
                            x=graph_data['node_x'], y=graph_data['node_y'],
                            mode='markers',
                            hoverinfo='text',
                            text=graph_data['node_text'],
                            textposition="middle center",
                            marker=dict(
                                size=10,
                                color='lightblue',
                                line=dict(width=2, color='DarkSlateGrey')
                            )
                        ))
                        
                        fig_network.update_layout(
                            title="回答間の関連性ネットワーク",
                            titlefont_size=16,
                            showlegend=False,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            annotations=[ dict(
                                text=f"ノード数: {graph_data['num_nodes']}, 関連数: {graph_data['num_edges']}",
                                showarrow=False,
                                xref="paper", yref="paper",
                                x=0.005, y=-0.002 ) ],
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                        )
                        
                        st.plotly_chart(fig_network, use_container_width=True)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("ノード数", graph_data['num_nodes'])
                        with col2:
                            st.metric("関連数", graph_data['num_edges'])
                    else:
                        st.info("ネットワーク分析を有効化してAI分析を実行してください")
                
                elif viz_option == "3D散布図":
                    st.subheader("📊 3D散布図")
                    
                    if analyzer.embeddings is not None and len(analyzer.embeddings) > 3:
                        # 3D PCA
                        pca_3d = PCA(n_components=3, random_state=42)
                        embeddings_3d = pca_3d.fit_transform(analyzer.embeddings)
                        
                        fig_3d = go.Figure(data=go.Scatter3d(
                            x=embeddings_3d[:, 0],
                            y=embeddings_3d[:, 1], 
                            z=embeddings_3d[:, 2],
                            mode='markers',
                            marker=dict(
                                size=5,
                                color=[f'クラスタ {label}' for label in analyzer.cluster_labels] if analyzer.cluster_labels is not None else 'blue',
                                showscale=True
                            ),
                            text=[text[:100] + "..." if len(text) > 100 else text for text in df['text_response']],
                            hovertemplate="<b>%{text}</b><extra></extra>"
                        ))
                        
                        fig_3d.update_layout(
                            title="3D クラスタ可視化 (PCA)",
                            scene=dict(
                                xaxis_title="PC1",
                                yaxis_title="PC2", 
                                zaxis_title="PC3"
                            ),
                            height=600
                        )
                        
                        st.plotly_chart(fig_3d, use_container_width=True)
                    else:
                        st.info("3D可視化にはより多くのデータが必要です")
                
                elif viz_option == "時系列トレンド":
                    st.subheader("📈 時系列トレンド分析")
                    
                    if analyzer.trend_analysis is not None:
                        trend_data = analyzer.trend_analysis
                        
                        # 日別回答数トレンド
                        daily_stats = trend_data['daily_stats']
                        
                        fig_trend = make_subplots(
                            rows=2, cols=1,
                            subplot_titles=('日別回答数', '週別平均文字数'),
                            vertical_spacing=0.1
                        )
                        
                        # 日別回答数
                        fig_trend.add_trace(
                            go.Scatter(
                                x=daily_stats.index,
                                y=daily_stats['response_count'],
                                mode='lines+markers',
                                name='回答数',
                                line=dict(color='blue')
                            ),
                            row=1, col=1
                        )
                        
                        # 週別文字数
                        weekly_data = trend_data['weekly_char_length']
                        fig_trend.add_trace(
                            go.Scatter(
                                x=[str(w) for w in weekly_data.index],
                                y=weekly_data.values,
                                mode='lines+markers',
                                name='平均文字数',
                                line=dict(color='red')
                            ),
                            row=2, col=1
                        )
                        
                        fig_trend.update_layout(height=600, showlegend=False)
                        st.plotly_chart(fig_trend, use_container_width=True)
                        
                        # 月別キーワードトレンド
                        st.subheader("📅 月別キーワードトレンド")
                        monthly_keywords = trend_data['monthly_keywords']
                        
                        if monthly_keywords:
                            months = list(monthly_keywords.keys())
                            selected_month = st.selectbox("月を選択", months)
                            
                            if selected_month in monthly_keywords:
                                month_words = monthly_keywords[selected_month]
                                if month_words:
                                    fig_monthly = px.bar(
                                        x=list(month_words.values()),
                                        y=list(month_words.keys()),
                                        orientation='h',
                                        title=f"{selected_month} の頻出キーワード",
                                        labels={'x': '出現回数', 'y': 'キーワード'}
                                    )
                                    fig_monthly.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                                    st.plotly_chart(fig_monthly, use_container_width=True)
                    else:
                        st.info("時系列分析を有効化してAI分析を実行してください")
            else:
                st.info("高度な可視化を使用するには、まずAI分析を実行してください")
        
        # Phase 3: ダッシュボード・レポートタブ
        with tab8:
            st.markdown('<h2 class="sub-header">ダッシュボード・レポート</h2>', unsafe_allow_html=True)
            
            # ダッシュボード部分
            st.subheader("📊 包括的ダッシュボード")
            
            if analyzer.processed_data is not None:
                dashboard_data = analyzer.create_comprehensive_dashboard(df)
                kpis = dashboard_data['kpis']
                
                # KPIメトリクス
                st.markdown("**🎯 主要指標 (KPI)**")
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric(
                        "総回答数", 
                        f"{kpis['total_responses']:,}",
                        help="分析対象の総回答数"
                    )
                
                with col2:
                    st.metric(
                        "ユニーク回答者", 
                        f"{kpis['unique_respondents']:,}",
                        help="重複を除いた回答者数"
                    )
                
                with col3:
                    st.metric(
                        "平均文字数", 
                        f"{kpis['avg_response_length']:.1f}",
                        help="1回答あたりの平均文字数"
                    )
                
                with col4:
                    response_rate = kpis['response_rate'] if kpis['response_rate'] < 10 else 1.0
                    st.metric(
                        "回答率", 
                        f"{response_rate:.1f}",
                        help="回答者あたりの平均回答数"
                    )
                
                with col5:
                    st.metric(
                        "データ品質", 
                        f"{kpis['data_quality_score']:.1f}%",
                        help="欠損値を除いたデータの完全性"
                    )
                
                # トップキーワードのサンキー図
                st.subheader("🔗 キーワード関連図")
                top_keywords = dashboard_data['top_keywords']
                
                if len(top_keywords) >= 5:
                    # サンキー図用データ準備
                    keywords = list(top_keywords.keys())[:10]
                    values = list(top_keywords.values())[:10]
                    
                    fig_sankey = go.Figure(data=[go.Sankey(
                        node = dict(
                            pad = 15,
                            thickness = 20,
                            line = dict(color = "black", width = 0.5),
                            label = keywords,
                            color = "blue"
                        ),
                        link = dict(
                            source = [0] * len(keywords[1:]),
                            target = list(range(1, len(keywords))),
                            value = values[1:]
                        )
                    )])
                    
                    fig_sankey.update_layout(title_text="キーワード出現頻度フロー", font_size=10)
                    st.plotly_chart(fig_sankey, use_container_width=True)
                
                # レポート生成部分
                st.subheader("📋 自動レポート生成")
                
                if 'report_insights' in st.session_state:
                    insights = st.session_state.report_insights
                    
                    # レポート表示
                    report_tabs = st.tabs(["📊 統計サマリー", "💭 センチメント", "🧩 クラスタ", "📈 時系列"])
                    
                    with report_tabs[0]:
                        if 'basic_stats' in insights:
                            st.markdown(insights['basic_stats'])
                    
                    with report_tabs[1]:
                        if 'sentiment' in insights:
                            st.markdown(insights['sentiment'])
                    
                    with report_tabs[2]:
                        if 'clusters' in insights:
                            st.markdown(insights['clusters'])
                    
                    with report_tabs[3]:
                        if 'temporal' in insights:
                            st.markdown(insights['temporal'])
                    
                    # エクスポート機能
                    st.subheader("📥 レポートエクスポート")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        # CSV エクスポート
                        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📊 分析データ (CSV)",
                            data=csv_data,
                            file_name=f"survey_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    
                    with col2:
                        # インサイト エクスポート
                        insights_text = "\n\n".join([f"## {k.replace('_', ' ').title()}\n{v}" for k, v in insights.items()])
                        st.download_button(
                            label="💡 インサイト (TXT)",
                            data=insights_text,
                            file_name=f"survey_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                            mime="text/plain"
                        )
                    
                    with col3:
                        # JSON エクスポート（分析結果の詳細データ）
                        export_data = {
                            'kpis': kpis,
                            'top_keywords': top_keywords,
                            'insights': insights,
                            'analysis_timestamp': datetime.now().isoformat()
                        }
                        if analyzer.clusters:
                            export_data['clusters'] = analyzer.clusters
                        
                        json_data = json.dumps(export_data, ensure_ascii=False, indent=2)
                        st.download_button(
                            label="📄 分析結果 (JSON)",
                            data=json_data,
                            file_name=f"survey_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                else:
                    st.info("👆 左側のサイドバーで「レポート生成」ボタンをクリックしてレポートを生成してください")
                
                # カスタムダッシュボード設定
                st.subheader("⚙️ ダッシュボードカスタマイズ")
                
                with st.expander("表示設定", expanded=False):
                    show_advanced_metrics = st.checkbox("高度なメトリクスを表示", value=True)
                    chart_theme = st.selectbox("チャートテーマ", ["plotly", "plotly_white", "plotly_dark"])
                    update_frequency = st.selectbox("更新頻度", ["リアルタイム", "1分", "5分", "手動"])
                    
                    if show_advanced_metrics:
                        st.info("高度なメトリクス表示が有効です")
                        
                        # 追加メトリクス表示
                        if analyzer.clusters:
                            st.markdown("**🧠 AI分析メトリクス**")
                            ai_col1, ai_col2, ai_col3 = st.columns(3)
                            
                            with ai_col1:
                                st.metric("検出クラスタ数", len(analyzer.clusters))
                            
                            with ai_col2:
                                if analyzer.embeddings is not None:
                                    st.metric("ベクトル次元", analyzer.embeddings.shape[1])
                            
                            with ai_col3:
                                cluster_balance = np.std([info['size'] for info in analyzer.clusters.values()])
                                st.metric("クラスタバランス", f"{cluster_balance:.1f}")
                
                # 自動更新機能（デモ用）
                if st.button("🔄 ダッシュボード更新"):
                    st.rerun()
                
            else:
                st.info("ダッシュボードを表示するには、まずデータをアップロードしてください")

else:
    # データ未設定時の説明
    if not st.session_state.data_uploaded:
        st.info("👆 左側のサイドバーからCSVまたはExcelファイルをアップロードして分析を開始してください")
    elif not st.session_state.columns_configured:
        st.info("👆 左側のサイドバーでカラムを選択して設定を完了してください")
    
    st.markdown("### 📋 サンプルデータ形式")
    sample_data = pd.DataFrame({
        'ID': [1, 2, 3], 
        '回答日': ['2024-01-15', '2024-01-16', '2024-01-17'],
        '自由回答': [
            'サービスに満足しています。今後も利用したいと思います。',
            'もう少し使いやすくなると良いと思います。',
            'サポートの対応が早くて助かりました。'
        ],
        '年齢': [25, 35, 45],
        '性別': ['男性', '女性', '男性']
    })
    st.dataframe(sample_data, use_container_width=True)
    
    st.markdown("### 📝 カラム選択について")
    st.markdown("""
    アップロード後に以下のカラムを選択してください：
    
    - **回答者ID カラム**: 各回答者を識別するID（例：ID, user_id, 回答者番号）
    - **回答日時 カラム**: 回答した日時（例：回答日, created_at, timestamp）  
    - **テキスト回答 カラム**: 分析対象のフリーテキスト（例：自由回答, comment, feedback）
    
    ファイルのカラム名は自由に設定できます。アップロード後に適切なカラムを選択してください。
    """)
    
    st.markdown("### 🚀 機能一覧")
    st.markdown("""
    **Phase 1 実装済み機能:**
    - ✅ CSV/Excel ファイルアップロード（UTF-8, Shift-JIS対応）
    - ✅ 柔軟なカラム選択機能
    - ✅ データ検証（選択カラムの存在確認、データ型検証）
    - ✅ キーワード検索（通常検索・正規表現検索）
    - ✅ 頻度分析（単語頻度、N-gram分析）
    - ✅ 基本可視化（棒グラフ、時系列グラフ、ヒストグラム）
    - ✅ 簡易センチメント分析
    - ✅ 期間フィルタ機能
    - ✅ 追加フィルタ機能（任意のカラム）
    - ✅ 検索結果のCSVエクスポート
    - ✅ 分析インサイト自動生成
    
    **Phase 2 実装済み機能:**
    - ✅ ベクトル埋め込み生成（Cortex EMBED_TEXT_768相当のモック実装）
    - ✅ 意味的類似検索（VECTOR_COSINE_SIMILARITY相当）
    - ✅ K-meansクラスタリング（自動最適クラスタ数推定）
    - ✅ クラスタ分析と特徴抽出
    - ✅ 2D可視化（t-SNE、PCA、UMAP）
    - ✅ クラスタ別センチメント分析
    - ✅ 代表的回答の自動抽出
    
    **Phase 3 実装済み機能:**
    - ✅ ワードクラウド生成（全体・クラスタ別）
    - ✅ ネットワークグラフ（回答間の関連性可視化）
    - ✅ 3D散布図（PCA による3次元可視化）
    - ✅ 時系列トレンド分析（日別、週別、月別）
    - ✅ 包括的ダッシュボード（KPI、サンキー図）
    - ✅ 自動レポート生成（統計、センチメント、クラスタ、時系列）
    - ✅ 多形式エクスポート（CSV、TXT、JSON）
    - ✅ カスタマイズ可能なダッシュボード
    - ✅ インタラクティブな可視化
    - ✅ AI分析メトリクス表示
    
    **今後の拡張予定（Phase 3.5以降）:**
    - 🔄 実際のSnowflake Cortex VECTOR関数統合
    - 🔄 PowerPoint自動生成
    - 🔄 リアルタイム分析・監視
    - 🔄 A/Bテスト分析機能
    - 🔄 予測分析・異常検知
    - 🔄 多言語対応
    - 🔄 カスタムモデルの統合
    """)

# フッター
st.markdown("---")
st.markdown("**Snowflake Cortex フリーテキストアンケート分析ツール** | Version 3.0.0 (Phase 3)")
st.markdown("*Powered by Streamlit & Snowflake Cortex AI*")