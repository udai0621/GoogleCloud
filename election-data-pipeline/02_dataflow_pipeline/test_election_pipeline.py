"""
election_pipeline.py のユニットテスト
------------------------------------- 
Apache Beam の DoFn をテスト
"""

#======================================================
# ライブラリ
#======================================================

# Standard
import unittest

# 3rd party
import apache_beam as beam 
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Custom
from election_pipeline import ( CleanElectionData,
    NormalizePartyName,
    AddDistrictCode,
    FormatForBigQuery,
    PARTY_NORMALIZATION
)


#======================================================
# テストコード
#======================================================

class TestCleanElectionData(unittest.TestCase):
    """CleanElectionData DoFn のテスト"""

    def test_clean_numeric_fields(self):
        """数値フィールドの変換"""
        with TestPipeline() as p:
            input_data = [
                {'votes': '50,000', 'age': '45', 'vshare': '35.5'},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(CleanElectionData())
            )

            expected = [
                {'votes': 50000, 'age': 45, 'vshare': 35.5}
            ]

            # 検証関数
            def check_result(actual):
                result = list(actual)[0]
                assert result["votes"] == 50000
                assert result["age"] == 45
                assert result["vshare"] == 35.5

            assert_that(output, check_result)

    def test_handle_empty_values(self):
        """空の値の処理"""
        with TestPipeline() as p:
            input_data = [
                {'votes': '', 'age': None, 'name': ' 山田太郎 '},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(CleanElectionData())
            )

            # 空の値はNoneに、空白は除去される
            def check_result(actual):
                result = list(actual)[0]
                assert result['votes'] is None
                assert result['age'] is None
                assert result['name'] == '山田太郎'

            assert_that(output, check_result)


class TestNormalizePartyName(unittest.TestCase):
    """NormalizePartyName DoFn のテスト"""

    def test_normalize_party_names(self):
        """政党名の正規化"""
        with TestPipeline() as p:
            input_data = [
                {"party": "自民"},
                {"party": "立民"},
                {"party": "維新"},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(NormalizePartyName())
            )

            expected = [
                {"party": "自民", "party_normalized": "自由民主党", "party_original": "自民"},
                {"party": "立民", "party_normalized": "立憲民主党", "party_original": "立民"},
                {"party": "維新", "party_normalized": "日本維新の会", "party_original": "維新"},
            ]

            def check_result(actual):
                results = list(actual)
                assert results[0]["party_normalized"] == "自由民主党"
                assert results[1]["party_normalized"] == "立憲民主党"
                assert results[2]["party_normalized"] == "日本維新の会"

            assert_that(output, check_result)

    def test_unknown_party_passthrough(self):
        """マッピングにない政党はそのまま"""
        with TestPipeline() as p:
            input_data = [
                {"party": "新党X"},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(NormalizePartyName())
            )

            expected = [
                {"party": "新党X", "party_normalized": "新党X", "party_original": "新党X"}
            ]

            def check_result(actual):
                result = list(actual)[0]
                assert result["party_normalized"] == "新党X"
                assert result["party_original"] == "新党X"

            assert_that(output, check_result)


class TestAddDistrictCode(unittest.TestCase):
    """AddDistrictCode DoFn のテスト"""

    def test_add_district_code(self):
        """選挙区コードの追加"""
        with TestPipeline() as p:
            input_data = [
                {"prefecture": "東京都", "district": "東京1区"},
                {"prefecture": "大阪府", "district": "大阪3区"},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(AddDistrictCode())
            )

            expected = [
                {
                    "prefecture": "東京都",
                    "district": "東京1区",
                    "prefecture_code": "13",
                    "district_code": "13-01"
                },
                {
                    "prefecture": "大阪府",
                    "district": "大阪3区",
                    "prefecture_code": "27",
                    "district_code": "27-03"
                },
            ]
            
            # 実際の出力を確認する検証関数
            def check_result(actual):
                actual_list = list(actual)

                # 東京のデータを確認
                tokyo = [x for x in actual_list if x["district"] == "東京1区"][0]
                assert tokyo["prefecture"] == "東京都"
                assert tokyo["prefecture_code"] == "13"
                assert tokyo["district_code"] == "13-01"

                # 大阪のデータを確認
                osaka = [x for x in actual_list if x["district"] == "大阪3区"][0]
                assert osaka["prefecture"] == "大阪府"
                assert osaka["prefecture_code"] == "27"
                assert osaka["district_code"] == "27-03"

            assert_that(output, check_result)


class TestFormatForBigQuery(unittest.TestCase):
    """FormatForBigQuery DoFn のテスト"""

    def test_convert_boolean_fields(self):
        """Boolean フィールドの変換"""
        with TestPipeline() as p:
            input_data = [
                {"win_smd": 1, "win_pr": 0, "duplicate": "1"},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(FormatForBigQuery())
            )

            expected = [
                {"win_smd": True, "win_pr": False, "duplicate": True},
            ]

            def check_result(actual):
                result = list(actual)[0]
                assert result['win_smd'] is True
                assert result['win_pr'] is False
                assert result['duplicate'] is True

            assert_that(output, check_result)

    def test_handle_na_values(self):
        """NA値の処理"""
        with TestPipeline() as p:
            input_data = [
                {"name": "NA", "age": "nan", "votes": ""},
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(FormatForBigQuery())
            )

            expected = [
                {"name": None, "age": None, "votes": None}
            ]

            def check_result(actual):
                result = list(actual)[0]
                assert result['name'] is None
                assert result['age'] is None
                assert result['votes'] is None

            assert_that(output, check_result)


if __name__ == "__main__":
    unittest.main()
