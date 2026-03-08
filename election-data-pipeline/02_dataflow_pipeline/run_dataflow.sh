#!/bin/bash
# DataflowRunner で GCP 上で実行するスクリプト

echo "=== Dataflow パイプライン (DataflowRunner) ==="
echo ""
echo "【注意】"
echo "  パケット名: $GCS_BUCKET_NAME"
echo "  リージョン: asia-northeast1"
echo ""

# BigQuery データセット作成
echo "BigQuery データセットを作成中..."
bq mk --dataset \
  --location=asia-northeast1 \
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
  --region=asia-northeast1 \
  --worker_zone=asia-northeast1-a \
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
echo "【進捗確認】"
echo "Dataflow コンソールで確認"
echo "  https://console.cloud.google.com/dataflow/jobs?project=${GCS_PROJECT_ID}"
echo ""
echo "【完了後の確認】"
echo "BigQuery でデータを確認:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) as total_records FROM \`${GCS_PROJECT_ID}.election_data.districts\`'"
echo ""
echo "詳細確認:"
echo "  bq query --use_legacy_sql=false 'SELECT election_year, party_normalized, COUNT(*) as count FROM \`${GCS_PROJECT_ID}.election_data.districts\` GROUP BY election_year, party_normalized ORDER BY election_year DESC, count DESC LIMIT 20'"
