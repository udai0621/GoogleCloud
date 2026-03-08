#!/bin/bash
# Dataflow 用サービスアカウントのセットアップ

set -e

echo "=== Dataflow サービスアカウントのセットアップ ==="
echo ""

# 環境変数の確認
if [ -z "$GCS_PROJECT_ID" ]; then
  echo "エラー: GCS_PROJECT_ID が設定されていません"
  exit 1
fi

SA_NAME="dataflow-pipeline-runner"
SA_ID="${SA_NAME}@${GCS_PROJECT_ID}.iam.gserviceaccount.com"

echo "Project: ${GCS_PROJECT_ID}"
echo "サービスアカウント: ${SA_ID}"
echo ""

echo "0. 必要な API を有効化中..."
gcloud services enable iam.googleapis.com --project=${GCS_PROJECT_ID}
gcloud services enable dataflow.googleapis.com --project=${GCS_PROJECT_ID}
gcloud services enable compute.googleapis.com --project=${GCS_PROJECT_ID}
echo "  -> 完了"

# 1. サービスアカウント作成
echo "1. サービスアカウントを作成中..."
gcloud iam service-accounts create ${SA_NAME} \
  --display-name="Dataflow Pipeline Runner" \
  --description="Dataflow パイプライン実行用のサービスアカウント" \
  --project=${GCS_PROJECT_ID} 2>/dev/null || echo " -> すでに存在します"

# 2. 必要最小限の権限を付与
echo ""
echo "2. 権限を付与中..."

# Dataflow Worker (ジョブ実行に必要)
gcloud projects add-iam-policy-binding ${GCS_PROJECT_ID} \
  --member="serviceAccount:${SA_ID}" \
  --role="roles/dataflow.worker" \
  --quiet

# Storage Object Admin (GCS の読み書き)
gcloud projects add-iam-policy-binding ${GCS_PROJECT_ID} \
  --member="serviceAccount:${SA_ID}" \
  --role="roles/storage.objectAdmin" \
  --quiet

# BigQuery Data Editor (BigQuery へのデータロード)
gcloud projects add-iam-policy-binding ${GCS_PROJECT_ID} \
  --member="serviceAccount:${SA_ID}" \
  --role="roles/bigquery.dataEditor" \
  --quiet

# BigQuery Job User (BigQuery ジョブの実行)
gcloud projects add-iam-policy-binding ${GCS_PROJECT_ID} \
  --member="serviceAccount:${SA_ID}" \
  --role="roles/bigquery.jobUser" \
  --quiet

# 3. ユーザーに Service Account User 権限を付与
echo ""
echo "3. 自分のアカウントに Service Account User 権限を付与中..."
CURRENT_USER=$(gcloud config get-value account)
echo "  - ユーザー: ${CURRENT_USER}"

gcloud iam service-accounts add-iam-policy-binding ${SA_ID} \
  --member="user:${CURRENT_USER}" \
  --role="roles/iam.serviceAccountUser" \
  --project=${GCS_PROJECT_ID} \
  --quiet

# 4. Dataflow Admin 権限をユーザーに付与 (ジョブ作成のため)
echo ""
echo "4. Dataflow Admin 権限をユーザーに付与中..."
gcloud projects add-iam-policy-binding ${GCS_PROJECT_ID} \
  --member="user:${CURRENT_USER}" \
  --role="roles/dataflow.admin" \
  --quiet

echo ""
echo "=== セットアップ完了 ==="
echo ""
echo "付与した権限:"
echo "  サービスアカウント (${SA_ID}):"
echo "    - roles/dataflow.worker"
echo "    - roles/storage.objectAdmin"
echo "    - roles/bigquery.dataEditor"
echo "    - roles/bigquery.jobUser"
echo ""
echo "  あなたのアカウント (${CURRENT_USER}):"
echo "    - roles/dataflow.admin"
echo "    - roles/iam.serviceAccountUser (on ${SA_ID})"
echo ""
echo "次のコマンドで実行してください:"
echo "  ./run_dataflow.sh"
