"""
衆議院議員総選挙データ取得 & GCS アップロード
-----------------------------------------------
【処理の流れ】
    1. 矢内勇生先生の公開リソースからCSVデータをダウンロード
    2. 簡易クレンジング
    3. Google Cloud Storage (GCS) へアップロード

【データソース】
    矢内勇生先生（高知工科大学）の公開データ
    https://yukiyanai.github.io/resources/

【取得データ】
    - 小選挙区データ（候補者別得票数）
    - 比例区データ（候補者別）
    - 比例区データ（政党別得票数）

【対象選挙】
    - 第50回（2024）
    - 第49回（2021）
    - その他の過去データも取得可能

【ダッシュボード化のポイント】
    - 政党別得票数の推移
    - 選挙区別投票率
    - 当選者の年齢分布
    - 小選挙区 vs 比例区の得票数
"""

# ==================================================
# ライブラリ
# ==================================================

# Standard
from math import log
import os
import logging
from pandas.core.generic import gc
import requests
from datetime import datetime
from typing import List, Dict

# 3rd party
import pandas as pd
from google.cloud import storage
from scipy.sparse import data
from sqlalchemy.sql.ddl import exc


# ==================================================
# ログ設定
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ==================================================
# 設定値
# ==================================================
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "election-data-for-portforio")
GCS_PROJECT_ID = os.environ.get("GCS_PROJECT_ID", "election-data-pipeline-48780")

# 矢内先生のデータURL
YANAI_BASE_URL = "https://yukiyanai.github.io/jp/resources/data"

# 取得対象の選挙データ
ELECTION_DATA_SOURCES = {
    "2024": {
        "year": 2024,
        "election_no": 50,
        "files": {
            "districts": f"{YANAI_BASE_URL}/hr2024_districts.csv",
            "pr_candidates": f"{YANAI_BASE_URL}/hr2024_pr_candidates.csv",
            "pr_parties": f"{YANAI_BASE_URL}/hr2024_pr_parties.csv",
        }
    },
    "2021": {
        "year": 2021,
        "election_no": 49,
        "files": {
            "districts": f"{YANAI_BASE_URL}/hr2021_districts.csv",
            "pr_candidates": f"{YANAI_BASE_URL}/hr2021_pr_candidates.csv",
            "pr_parties": f"{YANAI_BASE_URL}/hr2021_pr_parties.csv",
        }
    },
}


# ==================================================
# CSV ダウンロード
# ==================================================
def download_csv(url: str, description: str) -> pd.DataFrame:
    """
    指定URLからCSVダウンロードしてDataFrameで返す。

    Args:
        url: CSVファイルのURL
        description: データの説明

    Returns:
        取得したデータのDataFrame
    """
    logger.info(f"データ取得開始: {description}")
    logger.info(f"URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # UTF-8でエンコード
        response.encoding = "utf-8"

        # CSVをDataFrameに変換
        df = pd.read_csv(
            url,
            encoding="utf-8",
        )

        logger.info(f"取得完了: {len(df)} 行, {len(df.columns)} 列")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"ダウンロード失敗: {e}")
        raise

    except Exception as e:
        logger.error(f"CSV読み込みエラー: {e}")
        raise


# ==================================================
# データのクレンジング
# ==================================================

def clean_election_data(df: pd.DataFrame, data_type: str, year: int) -> pd.DataFrame:
    """
    選挙データの基本的なクレンジング。
    ※ 詳細な変換は Dataflow パイプラインで実施。

    Args:
        df: クレンジング対象のDataFrame
        data_type: データ種別(districts, pr_candidates, pr_parties)
        year: 選挙年

    Returns:
        クレンジング済みDataFrame
    """
    logger.info(f"データクレンジング開始: {data_type} ({year}年)")

    df.columns = df.columns.str.strip()
    
    # 文字列カラムの前後の空白除去
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.strip() if col.dtype == "object" else col
    )

    # 完全に空の行を削除
    df = df.dropna(how="all")

    # メタデータカラムを追加
    df["data_type"] = data_type
    df["election_year"] = year

    logger.info(f"クレンジング完了: {len(df)} 行")
    return df


# ==================================================
# GCS へのアップロード
# ==================================================
def upload_to_gcs(df: pd.DataFrame, data_type: str, year: int) -> str:
    """
    DataFrame をCSVに変換して GCS へアップロード

    Args:
        df: アップロードするデータ
        data_type: データ種別
        year: 選挙年

    Returns:
        アップロード先の GCS URI (gs://...)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"raw/election_{year}_{data_type}_{timestamp}.csv"
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_name}"

    logger.info(f"GCS へのアップロード開始: {gcs_uri}")

    # GCS クライアントの初期値
    storage_client = storage.Client(project=GCS_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)

    # DataFrame -> CSV -> GCS
    csv_content = df.to_csv(index=False, encoding="utf-8")
    blob.upload_from_string(csv_content, content_type="text/csv; charset=utf-8")

    logger.info(f"アップロード完了: {gcs_uri}")
    return gcs_uri


# ==================================================
# メイン処理
# ==================================================

def main():
    logger.info("=== 衆議院選挙データ取得 & GCS アップロード開始 ===")
    
    uploaded_uris: List[str] = []

    for year_key, election_data in ELECTION_DATA_SOURCES.items():
        year = election_data["year"]
        election_no = election_data["election_no"]

        logger.info(f"--- 第{election_no}回 ({year}年) 選挙データ処理開始 ---")

        for data_type, url in election_data["files"].items():
            try:
                logger.info(f"データ種別: {data_type}")

                # 1. CSV ダウンロード
                df = download_csv(url, f"{year}年 {data_type}")

                # 2. クレンジング
                df = clean_election_data(df, data_type, year)

                # 3. GCS へアップロード
                gcs_uri = upload_to_gcs(df, data_type, year)
                uploaded_uris.append(gcs_uri)

            except Exception as e:
                logger.error(f"処理エラー: {data_type} ({year}年) - {e}")
                continue
    
    logger.info("=== 全データのアップロード完了 ===")
    logger.info("アップロード先一覧:")
    for uri in uploaded_uris:
        logger.info(f" {uri}")

        return uploaded_uris


if __name__ == "__main__":
    main()

