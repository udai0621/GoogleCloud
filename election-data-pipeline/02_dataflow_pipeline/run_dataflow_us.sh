#!/bin/bash
# DataflowRunner で GCP 上で実行するスクリプト

echo "=== Dataflow パイプライン (DataflowRunner) ==="
echo ""
echo "【注意】"
echo "- Dataflow ジョブが起動します (課金対象)"
echo "- 完了まで数分〜十数分かかります"
echo ""

# 環境変数の確認
if [ -z "$GCS_BUCKET_NAME" ]; then
  echo "Error: GCS_BUCKET_NAME が設定されていません"
  echo "Run: export GCS_BUCKET_NAME='your-bucket-name'"
  exit 1
fi

if [ -z "$GCS_PROJECT_ID" ]; then
  echo "Error: GCS_PROJECT_ID が設定されていません"
  echo "Run: export GCS_BUCKET_NAME='your-project-id'"
  exit 1
fi

# Dataflow API が有効か確認
echo "Dataflow API を有効化中..."
gcloud services enable dataflow.googleapis.com --project=${GCS_PROJECT_ID}
echo ""
echo "設定確認:"
echo "  プロジェクトID: $GCS_PROJECT_ID"
echo "  パケット名: $GCS_BUCKET_NAME"
echo "  リージョン: us-central1"
echo ""

# BigQuery データセット作成
echo "BigQuery データセットを作成中..."
bq mk --dataset \
  --location=US \
  --description="選挙データパイプライン" \
  ${GCS_PROJECT_ID}:election_data 2>/dev/null || echo " -> すでに存在します"

echo ""
echo "Dataflow ジョブを起動中..."
echo ""

# サービスアカウント
SA_ID="dataflow-pipeline-runner@${GCS_PROJECT_ID}.iam.gserviceaccount.com"
echo ""
echo "Dataflow ジョブを起動中..."
echo "  サービスアカウント: ${SA_ID}"
echo ""

# DataflowRunner で実行
python election_pipeline.py \
  --runner=DataflowRunner \
  --project=${GCS_PROJECT_ID} \
  --region=us-central1 \
  --temp_location=gs://${GCS_BUCKET_NAME}/temp \
  --staging_location=gs://${GCS_BUCKET_NAME}/staging \
  --input=gs://${GCS_BUCKET_NAME}/raw/election_*_districts_*.csv \
  --output_table=${GCS_PROJECT_ID}:election_data.districts \
  --job_name=election-data-pipeline-$(date +%Y%m%d-%H%M%S) \
  --save_main_session \
  --write_method=FILE_LOADS \
  --service_account_email=${SA_ID}

echo ""
echo "=== ジョブ起動完了 ==="
echo ""
