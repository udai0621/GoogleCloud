#!/bin/bash
# BigQuery 分析クエリの実行スクリプト

set -e

echo "=== BigQuery 分析クエリ実行 ==="
echo ""

# 環境変数確認
if [ -z "$GCS_PROJECT_ID" ]; then
  echo "エラー: GCS_PROJECT_ID が設定されていません"
  exit 1
fi

echo "プロジェクトID: ${GCS_PROJECT_ID}"
echo ""

# プレースホルダーを置換する関数
replace_placeholders() {
  sed "s/\${GCS_PROJECT_ID}/${GCS_PROJECT_ID}/g" "$1"
}

# ビュー作成
echo "1. ビューを作成中..."
echo ""

echo "  - party_summary ビュー"
replace_placeholders views/create_party_summary_view.sql |
  bq query --use_legacy_sql=false

echo "  - prefecture_summary ビュー"
replace_placeholders views/create_prefecture_summary_view.sql |
  bq query --use_legacy_sql=false

echo ""
echo "✅ ビュー作成完了"
echo ""

# 分析クエリ実行
echo "2. 分析クエリを実行中..."
echo ""

echo "  - 政党別得票数（年次比較）"
replace_placeholders queries/01_party_votes_by_year.sql |
  bq query --use_legacy_sql=false --format=pretty >results/01_party_votes_by_year.txt
echo "    結果: results/01_party_votes_by_year.txt"

echo "  - 都道府県別分析"
replace_placeholders queries/02_prefecture_analysis.sql |
  bq query --use_legacy_sql=false --format=pretty >results/02_prefecture_analysis.txt
echo "    結果: results/02_prefecture_analysis.txt"

echo "  - 選挙年次比較（2021 vs 2024）"
replace_placeholders queries/03_election_comparison.sql |
  bq query --use_legacy_sql=false --format=pretty >results/03_election_comparison.txt
echo "    結果: results/03_election_comparison.txt"

echo "  - 激戦区分析"
replace_placeholders queries/04_swing_districts.sql |
  bq query --use_legacy_sql=false --format=pretty >results/04_swing_districts.txt
echo "    結果: results/04_swing_districts.txt"

echo ""
echo "✅ すべてのクエリ実行完了"
echo ""

# サマリー
echo "=== 実行サマリー ==="
echo ""
echo "作成されたビュー:"
echo "  - ${GCS_PROJECT_ID}.election_data.party_summary"
echo "  - ${GCS_PROJECT_ID}.election_data.prefecture_summary"
echo ""
echo "出力ファイル:"
echo "  - results/01_party_votes_by_year.txt"
echo "  - results/02_prefecture_analysis.txt"
echo "  - results/03_election_comparison.txt"
echo "  - results/04_swing_districts.txt"
echo ""
echo "次のステップ:"
echo "  - Looker Studio でダッシュボード作成"
echo "  - BigQuery Console でクエリを確認"
