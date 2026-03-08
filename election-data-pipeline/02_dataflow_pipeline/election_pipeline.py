"""
選挙データ変換パイプライン - Apache Beam / Dataflow
--------------------------------------------------- 
【処理の流れ】
    1. GCS から複数年の選挙データを読み込み
    2. データクレンジング・正規化
    3. 政党名の表記揺れ統一
    4. 選挙区コードの追加
    5. BigQuery へロード

【実行方法】
    # ローカル実行（開発・テスト）
    python election_pipeline.py --runner=DirectRunner

    # Dataflow 実行（本番）
    python election_pipeline.py \
      --runner=DataflowRunner \
      --project=[your-gcp-project-id] \
      --region==asia-northeast1 \
      --temp_location=gs://[your-bucket]/temp \
      --staging_location=gs://[your-bucket]/staging
"""

#======================================================
# ライブラリ
#======================================================

# Standard
import argparse
import json
import logging
import re
from typing import Dict, List, Any
from unittest import result

# 3rd party
import apache_beam as beam 
from apache_beam.io.tfrecordio import codecs
from apache_beam.options.pipeline_options import JobServerOptions, PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.coders import coders

#======================================================
# 日本語対応のためのモンキーパッチ
#======================================================
import apache_beam.io.gcp.bigquery_file_loads as bq_file_loads
from apache_beam.typehints.typehints import normalize 

# オリジナルの関数を保存
_original_json_dumps = json.dumps

# ensure_ascii = False を強制する関数
def _patch_json_dumps(obj, **kwargs):
    kwargs["ensure_ascii"] = False
    return _original_json_dumps(obj, **kwargs)

# モンキーパッチ適応
json.dumps = _patch_json_dumps

# カスタムJSONコーダー
class UTF8JSONCoder(coders.Coder):
    """UTF-8でエンコードするJSONコーダー"""
    def encode(self, value: Any) -> bytes:
        # ensure_ascii=False で日本語をそのまま出力
        return json.dumps(value, ensure_ascii=False).encode("utf-8")
    
    def decode(self, encoded):
        return json.loads(encoded.decode("utf-8"))
    
    def is_deterministic(self) -> bool:
        return True


#======================================================
# 政党名正規化マッピング
#======================================================

PARTY_NORMALIZATION = {
    # 自民党の表記ゆれ
    "自民": "自由民主党",
    "自由民主": "自由民主党",
    "自民党": "自由民主党",

    # 立憲民主党
    "立民": "立憲民主党",
    "立憲": "立憲民主党",

    # 公明党
    "公明": "公明党",

    # 日本維新の会
    "維新": "日本維新の会",
    "維新の会": "日本維新の会",

    # 共産党
    "共産": "日本共産党",
    "共産党": "日本共産党",

    # 国民民主党
    "国民": "国民民主党",
    "国民民主": "国民民主党",

    # れいわ新選組
    "れいわ": "れいわ新選組",

    # 社民党
    "社民": "社会民主党",
    "社民党": "社会民主党",

    # NHK党
    "NHK党": "NHK党",
    "NHKから国民を守る党": "NHK党",
    
    # 無所属
    "無": "無所属",
    "無所": "無所属",
}


#======================================================
# DoFn: データクレンジング
#======================================================

class CleanElectionData(beam.DoFn):
    """選挙データの基本的なクレンジングを行う"""

    def process(self, element: Dict[str, Any]):
        """
        各レコードをクレンジング

        Args:
            element: CSV行（辞書形式）

        Yields:
            クレンジング済みレコード
        """
        try:
            # 空白・改行の除去
            cleaned = {
                key: str(value).strip() if value else None
                for key, value in element.items()
            }

            # 数値フィールドの型変換
            numeric_fields = ['votes', 'age', 'previous', 'vshare']
            for field in numeric_fields:
                if field in cleaned and cleaned[field]:
                    try:
                        # カンマ区切りを除去して数値化
                        cleaned[field] = float(
                            str(cleaned[field]).replace(',', '')
                        )
                    except ValueError:
                        cleaned[field] = None

            # 整数フィールドの変換
            int_field = ['age', 'previous', 'votes', "dist_no", "election_year"]
            for field in int_field:
                if field in cleaned and cleaned[field] is not None:
                    try:
                        cleaned[field] = int(cleaned[field])
                    except (ValueError, TypeError):
                        cleaned[field] = None

            yield cleaned

        except Exception as e:
            logging.error(f"クレンジングエラー: {e}, element={element}")


#======================================================
# DoFn: 政党名の正規化
#======================================================

class NormalizePartyName(beam.DoFn):
    """政党名の表記ゆれを統一"""

    def process(self, element: Dict[str, Any]):
        """
        政党名を正規化

        Args:
            element: クレンジング済みレコード

        Yields:
            政党名正規化済みレコード
        """

        # 元のelementを変更せず、新しい辞書を作成
        result = dict(element)

        if 'party' in result and result['party']:
            original_party = result['party']
            normalized_party = PARTY_NORMALIZATION.get(
                original_party,
                original_party  # マッピングになければそのまま
            )
            result['party_normalized'] = normalized_party
            result['party_original'] = original_party

        yield result


#======================================================
# DoFn: 選挙区コードの追加
#======================================================

class AddDistrictCode(beam.DoFn):
    """選挙区に一意なコードを付与"""

    # 都道府県コードマッピング（JIS X 0401）
    PREFECTURE_CODES = {
        "北海道": "01", "青森県": "02", "岩手県": "03", "宮城県": "04",
        "秋田県": "05", "山形県": "06", "福島県": "07", "茨城県": "08",
        "栃木県": "09", "群馬県": "10", "埼玉県": "11", "千葉県": "12",
        "東京都": "13", "神奈川県": "14", "新潟県": "15", "富山県": "16",
        "石川県": "17", "福井県": "18", "山梨県": "19", "長野県": "20",
        "岐阜県": "21", "静岡県": "22", "愛知県": "23", "三重県": "24",
        "滋賀県": "25", "京都府": "26", "大阪府": "27", "兵庫県": "28",
        "奈良県": "29", "和歌山県": "30", "鳥取県": "31", "島根県": "32",
        "岡山県": "33", "広島県": "34", "山口県": "35", "徳島県": "36",
        "香川県": "37", "愛媛県": "38", "高知県": "39", "福岡県": "40",
        "佐賀県": "41", "長崎県": "42", "熊本県": "43", "大分県": "44",
        "宮崎県": "45", "鹿児島県": "46", "沖縄県": "47",
    }

    def process(self, element: Dict[str, Any]):
        """
        選挙区コードを追加

        Args:
            element: 政党名正規化済みレコード

        Yields:
            選挙区コード付きレコード
        """
        result = dict(element)

        # 都道府県コード　
        if 'prefecture' in result and result['prefecture']:
            pref_name = result["prefecture"]

            # 都道府県名を正規化（県・府・道がついていない場合は付与）
            if pref_name == "北海道":
                normalized_pref = "北海道"
            elif pref_name == "東京":
                normalized_pref = "東京都"
            elif pref_name in ["大阪", "京都"]:
                normalized_pref = f"{pref_name}府"
            elif pref_name and not pref_name.endswith(("都", "道", "府", "県")):
                normalized_pref = f"{pref_name}県"
            else:
                normalized_pref = pref_name

            pref_code = self.PREFECTURE_CODES.get(normalized_pref)
            if pref_code:
                result['prefecture_code'] = str(pref_code)

        # 選挙区コード(例: "13-01" = 東京１区)
        if 'district' in result and result['district']:
            # '東京1区' から '1' を抽出
            match = re.search(r'(\d+)区', result['district'])
            if match and 'prefecture_code' in result:
                district_num = match.group(1).zfill(2)
                result['district_code'] = f"{result['prefecture_code']}-{district_num}"

        yield result


#======================================================
# DoFn: BigQuery スキーマへの適合
#======================================================

class FormatForBigQuery(beam.DoFn):
    """BigQueryに適した形式に変換"""

    def process(self, element: Dict[str, Any]):
        """
        BigQuery 用フォーマットに変換

        Args:
            element: 選挙区コード付きレコード

        Yields:
            BigQuery ロード用レコード
        """

        # NULL 値を None に統一
        formatted = {
            key: (None if value in ['NA', 'nan', '', 'None'] else value)
            for key, value in element.items()
        }

        # Boolean フィールドの変換　
        bool_fields = ['win_smd', 'win_pr', 'duplicate', 'dup']
        for field in bool_fields:
            if field in formatted and formatted[field] is not None:
                # 1/0 を True/False に
                if formatted[field] in [1, '1', 1.0]:
                    formatted[field] = True
                elif formatted[field] in [0, '0', 0.0]:
                    formatted[field] = False

        yield formatted


#======================================================
# メインパイプライン
#======================================================

def run(argv=None):
    """メインパイプライン実行"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://[your-bucket]/raw/election_*.csv',
        help='入力ファイルパターン (GCS)'
    )

    parser.add_argument(
        '--output_table',
        dest='output_table',
        default='[your-project]:election_data.districts',
        help='出力先 BigQuery テーブル'
    )
    parser.add_argument(
        "--write_method",
        dest="write_method",
        default="FILE_LOADS",
        choices=["FILE_LOADS", "STREAMING_INSERTS"],
        help="BigQuery 書き込み方法（FILE_LOADS: 大規模データ向け、STREAMING_INSERTS: 小規模データ向け）"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pipeline オプション
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # BigQuery スキーマ定義（文字列形式）
    bq_schema = {
        'fields': [
            {'name': 'prefecture', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'prefecture_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'district', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'district_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'dist_no', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'yomi', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lastname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'firstname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_kana', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_kana', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'party', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'party_normalized', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'party_original', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'recommended', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'previous', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'duplicate', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'win_smd', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'win_pr', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'votes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'vshare', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'data_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'election_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:

        # データ読み込み & 変換　
        transformed_data = (
            p
            | 'ReadFromGCS' >> ReadFromText(
                known_args.input,
                skip_header_lines=1 # CSVヘッダーをスキップ
            )
            | 'ParseCSV' >> beam.Map(
                lambda line: dict(zip(
                        # ヘッダー
                        ['prefecture', 'dist_no', 'district', 'name', 'yomi',
                         'lastname', 'firstname', 'last_kana', 'first_kana',
                         'age', 'party', 'recommended', 'status', 'previous',
                         'duplicate', 'win_smd', 'win_pr', 'votes', 'vshare',
                         'data_type', 'election_year'],
                        line.split(',')
                ))
            )
            | 'CleanData' >> beam.ParDo(CleanElectionData())
            | 'NormalizeParty' >> beam.ParDo(NormalizePartyName())
            | 'AddDistrictCode' >> beam.ParDo(AddDistrictCode()) 
            | 'FormatForBQ' >> beam.ParDo(FormatForBigQuery())
        )

        # BigQuery へ書き込み
        if known_args.write_method == "STREAMING_INSERTS":
            # STREAMING_INSERTS: 小規模データ向け
            transformed_data | "WriteToBigQuery" >> WriteToBigQuery(
                known_args.output_table,
                schema=bq_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method="STREAMING_INSERTS"
            )
        else:
            transformed_data | 'WriteToBigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=bq_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method="FILE_LOADS",
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
