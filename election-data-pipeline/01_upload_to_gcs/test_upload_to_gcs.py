"""
upload_to_gcs.py unit test
--------------------------
実際のダウンロードやGCSには接続せず、モックを使用してテスト。
"""
# ==================================================
# ライブラリ
# ==================================================

# Default
from unittest import mock
from unittest.mock import patch, MagicMock

# 3rd party
import pytest
import pandas as pd

# Custom
from upload_to_gcs import clean_election_data


# ==================================================
# clean_election_data のテスト
# ==================================================

class TestCleanElectionData:
    def _make_sample_df(self):
        """テスト用のサンプルDataFrame（選挙データ風）"""
        return pd.DataFrame({
            "prefecture": [" 東京都 ", "大阪府　", None],
            "district": ["東京１区", "大阪3区", None],
            "name": ["山田 太郎", "佐藤 花子", None],
            "party": ["自由民主党", "立憲民主党", None],
            "votes": [50000, 48000, None],
        })
    
    def test_strips_whitespace(self):
        """前後の空白が除去されるか"""
        df = self._make_sample_df()
        result = clean_election_data(df, "test", 2024)

        assert result["prefecture"].iloc[0] == "東京都"
        assert result["prefecture"].iloc[1] == "大阪府"

    def test_adds_metadata_columns(self):
        """メタデータカラムが追加されるか"""
        df = self._make_sample_df()
        result = clean_election_data(df, "district", 2024)

        assert "data_type" in result.columns
        assert "election_year" in result.columns
        assert result["data_type"].iloc[0] == "district"
        assert result["election_year"].iloc[0] == 2024

    def test_drop_fully_null_rows(self):
        """全カラムNullの行が削除されるか"""
        df = self._make_sample_df()
        result = clean_election_data(df, "test", 2024)

        # 元は3行、最後の行が全部Nullなので2行になる
        assert len(result) == 2


# ==================================================
# upload_to_gcs のテスト
# ==================================================
class TestUploadToGcs:
    @patch("upload_to_gcs.storage.Client")
    def test_upload_to_correct_path(self, mock_storage_client):
        """
        GCS URI のフォーマットが正しいか、
        アップロードメソッドが呼ばれるかを確認。
        """
        from upload_to_gcs import upload_to_gcs

        # GCS クライアントのモック設定
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        # テストデータ
        df = pd.DataFrame({"col": ["val"]})
        result_uri = upload_to_gcs(df, "district", 2024)

        # gs:// から始まるURIが返る
        assert result_uri.startswith("gs://")
        # 年とデータ種別がパスに含まれる
        assert "2024" in result_uri
        assert "district" in result_uri
        # アップロードメソッドが実行された
        mock_blob.upload_from_string.assert_called_once()


# ==================================================
# download_csv のテスト
# ==================================================
class TestDownloadCsv:
    @patch("upload_to_gcs.requests.get")
    @patch("upload_to_gcs.pd.read_csv")
    def test_downloads_and_returns_dataframe(self, mock_read_csv, mock_get):
        """
        CSV取得とDataFrame変換が正しく動作するか。
        """
        from upload_to_gcs import download_csv

        # モックレスポンス
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.encoding = "utf-8"
        mock_get.return_value = mock_response

        # モックDataFrame
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_read_csv.return_value = mock_df

        # 実行
        result = download_csv("http://test.url", "test description")

        # requests.get が呼ばれた
        mock_get.assert_called_once()
        # DataFrameが返される
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
