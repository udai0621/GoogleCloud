#!/bin/bash
# DirectRunner でローカル実行するスクリプト

echo "=== Dataflow パイプライン (DirectRunner) ==="
echo ""
echo "【注意】"
echo "- BigQuery テーブルが作成されます"
echo "- GCS の raw/*.csv を読み込みます"
echo ""

# 環境変数の確認
if [ -z "$GCS_BUCKET_NAME" ]; then
  echo "Error: GCS_BUCKET_NAME が設定されていません"
  echo "Run: export GCS_BUCKET_NAME='your-bucket-name'"
  exit 1
fi

if [ -z "$GCS_PROJECT_ID" ]; then
  echo "Error: GCS_PROJECT_ID が設定されていません"
  echo "Run: export GCS_PROJECT_ID='your-project-id'"
  exit 1
fi

echo "設定確認:"
echo "  プロジェクトID: $GCS_PROJECT_ID"
echo "  バケット名: $GCS_BUCKET_NAME"
echo ""

# BigQuery データセット作成(存在しない場合)
echo "BigQuery データセットを作成中..."
bq mk --dataset \
  --location=asia-northeast1 \
  --description="選挙データパイプライン" \
  ${GCS_PROJECT_ID}:election_data 2>/dev/null || echo "  -> すでに存在します"

echo ""
echo "パイプライン実行中..."
echo ""

# DirectRunner で実行
python election_pipeline.py \
  --runner=DirectRunner \
  --input=gs://${GCS_BUCKET_NAME}/raw/election_*_districts_*.csv \
  --output_table=${GCS_PROJECT_ID}:election_data.districts \
  --project=${GCS_PROJECT_ID} \
  --temp_location=gs://${GCS_BUCKET_NAME}/temp \
  --write_method=FILE_LOADS

echo ""
echo "=== 完了 ==="
echo ""
echo "【確認方法】"
echo "BigQuery でデータを確認:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM \`${GCS_PROJECT_ID}.election_data.districts\` LIMIT 10'"
echo ""
echo "または BigQuery コンソールから確認"
echo "  https://console.cloud.google.com/bigquery?project=${GCS_PROJECT_ID}"
