# 03 - BigQuery での分析・集計

## 概要

BigQuery で選挙データを分析し、政党勢力の変化や都道府県別の投票傾向を明らかにします。

```mermaid
graph LR 
  A[districts<br>テーブル]
  B[分析クエリ]
  C[集計ビュー]
  D[Looker Studio]
  
  A --> B --> C --> D 
  
  style A fill:#e8f5e9
  style B fill:#fff4e1
  style C fill:#e3f2fd
  style D fill:#fce4ec
```

--- 

## ディレクトリ構成

```tree 
03_bigquery/
├── queries/                               # 分析クエリ
│   ├── 01_party_votes_by_year.sql         # 政党別得票数（年次）
│   ├── 02_prefecture_analysis.sql         # 都道府県別分析
│   ├── 03_election_comparison.sql         # 2021 vs 2024 比較
│   └── 04_swing_districts.sql             # 激戦区分析
├── views/                                 # ビュー定義
│   ├── create_party_summary_view.sql      # 政党サマリービュー
│   └── create_prefecture_summary_view.sql # 都道府県サマリービュー
├── results/                               # クエリ実行結果
│   ├── 01_party_votes_by_year.txt
│   ├── 02_prefecture_analysis.txt
│   ├── 03_election_comparison.txt
│   └── 04_swing_districts.txt
├── run_analysis.sh                        # 一括実行スクリプト
└── README.md
```

--- 

## 実行方法

### 前提条件

```bash
# 環境変数を設定
# これまでに設定していれば不要
export GCS_PROJECT_ID="your-gcp-project-id"
```

### 一括実行 

```bash
# 全てのクエリとビューを作成
./run_analysis.sh
```

### 実行内容

1. ビュー作成（`party_summary`, `prefecture_summary`）
2. 分析クエリ実行（4種類）
3. 結果を`results/`に保存

--- 

## 分析クエリ詳細 

### 1. 政党別得票数（年次比較）

ファイル: `queries/01_party_votes_by_year.sql`
目的: 2021年と2024年の政党別パフォーマンスを比較
主な指標:
- 候補者数
- 当選者数（小選挙区/比例復活）
- 総得票数
- 平均得票率
- 小選挙区当選率

#### 実行 

```bash
cat queries/01_party_votes_by_year.sql | \
  sed "s/\${GCS_PROJECT_ID}/${GCS_PROJECT_ID}/g" | \
  bq query --use_legacy_sql=false
```

#### 期待される結果例

```txt
+---------------+------------------+-------------------+-------------+
| election_year | party_normalized | total_candidates  | smd_winners |
+---------------+------------------+-------------------+-------------+
|          2024 | 自由民主党       |               XXX |         XXX |
|          2024 | 立憲民主党       |               XXX |         XXX |
|          2021 | 自由民主党       |               XXX |         XXX |
+---------------+------------------+-------------------+-------------+
```

--- 

### 2. 都道府県別分析

ファイル: `queries/02_prefecture_analysis.sql`
目的: 都道府県ごとの政党勢力を可視化
主な指標:
- 都道府県内の議席占有率
- 都道府県内の得票占有率
- 当選者数

#### 実行 
```bash
cat queries/02_prefecture_analysis.sql | \
  sed "s/\${GCS_PROJECT_ID}/${GCS_PROJECT_ID}/g" | \
  bq query --use_legacy_sql=false
```

#### 期待される結果例
```bash
+---------------+------------+------------------+---------+----------------+
| election_year | prefecture | party_normalized | winners | seat_share_pct |
+---------------+------------+------------------+---------+----------------+
|          2024 | 東京都     | 自由民主党       |      XX |          XX.XX |
|          2024 | 東京都     | 立憲民主党       |      XX |          XX.XX |
+---------------+------------+------------------+---------+----------------+
```

--- 

### 3. 選挙年次比較（2021 vs 2024）

ファイル: `queries/03_election_comparison.sql`
目的: 政党ごとの勢力変化を定量化
主な指標:
- 議席変化（絶対数/変化率）
- 得票変化（絶対数/変化率）
- 候補者数の変化

#### 実行 
```bash
cat queries/03_election_comparison.sql | \
  sed "s/\${GCS_PROJECT_ID}/${GCS_PROJECT_ID}/g" | \
  bq query --use_legacy_sql=false
```

#### 期待される結果例
```bash
+------------------+--------------+--------------+-------------+----------------+
| party_normalized | winners_2021 | winners_2024 | seat_change | seat_change_pct|
+------------------+--------------+--------------+-------------+----------------+
| 自由民主党       |          XXX |          XXX |         +XX |          +X.XX |
| 立憲民主党       |          XXX |          XXX |         -XX |          -X.XX |
+------------------+--------------+--------------+-------------+----------------+
```

--- 

### 4. 激戦区分析

ファイル: `queries/04_swing_districts.sql` 
目的: 得票差が小さい選挙区を特定
主な指標:
- 1位と2位の得票差
- 得票差率
- 当選者と次点候補の情報

#### 実行 
```bash
cat queries/04_swing_districts.sql | \
  sed "s/\${GCS_PROJECT_ID}/${GCS_PROJECT_ID}/g" | \
  bq query --use_legacy_sql=false
```

#### 期待される結果例 
```txt
+---------------+------------+-----------+------------------+------------------+-------------+
| election_year | prefecture | district  | first_place_party|second_place_party| vote_margin|
+---------------+------------+-----------+------------------+------------------+-------------+
|          2024 | XX県       | XX区      | 自由民主党       | 立憲民主党       |         XXX |
+---------------+------------+-----------+------------------+------------------+-------------+
```

--- 

## ビュー

### party_summary ビュー

目的: Looker Studio で簡単に使える政党別集計
カラム:
- `election_year`: 選挙年
- `party_normalized`: 政党名
- `total_candidates`: 総候補者数
- `smd_winners`: 小選挙区当選者数
- `pr_winners`: 比例復活当選者数
- `total_winners`: 総当選者数
- `total_votes`: 総得票数
- `avg_vote_share`: 平均得票率
- `avg_candidate_age`: 候補者平均年齢
- `duplicate_rate`: 重複立候補率
- `smd_win_rate`: 小選挙区当選率

#### 使用例

```sql
SELECT * 
FROM `${GCS_PROJECT_ID}.election_data.party_summary`
WHERE election_year = 2024
ORDER BY total_winners DESC
```

--- 

### prefecture_summary ビュー 
目的: 都道府県ごとの政党勢力を可視化
カラム:
- `election_year`: 選挙年
- `prefecture`: 都道府県名
- `prefecture_code`: 都道府県コード
- `party_normalized`: 政党名
- `candidates`: 候補者数
- `winners`: 当選者数
- `total_votes`: 総得票数
- `avg_vote_share`: 平均得票率
- `districts_contested`: 立候補選挙区数

#### 使用例
```sql
SELECT * 
FROM `${GCS_PROJECT_ID}.election_data.prefecture_summary`
WHERE election_year = 2024
  AND prefecture = '東京都'
ORDER BY winners DESC
```

--- 

## 結果の確認

### BigQuery Console で確認

```
https://console.cloud.google.com/bigquery?project=${GCS_PROJECT_ID}
```

1. `election_data` データセットを選択
2. ビューをクリック（`party_summary`, `prefecture_summary`）
3. 「プレビュー」タブでデータ確認

### ローカルファイルで確認

```bash
# 結果ファイルを確認
cat results/01_party_votes_by_year.txt 
cat results/03_election_comparison.txt
```

--- 

## Looker Studio ダッシュボード

### ダッシュボード作成手順

1. Looker Studio を開く 

```
https://lookerstudio.google.com/
```

2. データソースを追加
  - 「作成」->「データソース」
  - BigQuery 選択 
  - プロジェクト: `${GCS_PROJECT_ID}` 
  - データセット: `election_data` 
  - テーブル/ビュー: `party_summary` or `districts`

3. レポート作成
  - 「作成」->「レポート」
  - データソースを選択

### 推奨グラフ

1. 政党別議席数（棒グラフ）
- ディメンション: `party_normalized`
- 指標: `total_winners`
- フィルタ: `election_year = 2024`

2. 得票率の推移（折れ線グラフ）
- ディメンション: `election_year`
- 指標: `total_votes`
- 内訳ディメンション: `party_normalized`

3. 都道府県別勢力図（地図）
- 地域: `prefecture`
- 指標: `winners`（サイズ）
- 色: `party_normalized`（優勢政党）

4. 激戦区一覧（表）
- ディメンション: `prefecture`, `district`
- 指標: `vote_margin`, `margin_pct`
- 並び替え: `vote_margin` 昇順

--- 

## 参考リンク

- [BigQuery SQL リファレンス](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [Looker Studio 公式ドキュメント](https://support.google.com/looker-studio)
- [BigQuery ビューのベストプラクティス](https://cloud.google.com/bigquery/docs/views)
