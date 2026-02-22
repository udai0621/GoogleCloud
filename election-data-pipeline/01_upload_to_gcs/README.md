# 01 - 衆議院選挙データ取得 & GCS アップロード

## このステップでやること

```
矢内先生の公開データ（CSV） → ダウンロード → GCS
```

矢内勇生先生（高知工科大学）が公開している衆議院議員総選挙データをダウンロードし、
Google Cloud Storage（GCS）に保管します。

---

## データソース

**矢内勇生先生の公開リソース**  
https://yukiyanai.github.io/resources/

- **ライセンス**: CC BY 4.0（出典明示で自由に利用可能）
- **文字コード**: UTF-8
- **形式**: CSV
- **品質**: すでに整形済み・カラム名も英語

---

## 取得するデータ

### 第50回（2024年）

| ファイル | 内容 | 主要カラム |
|---------|------|-----------|
| `hr2024_districts.csv` | 小選挙区データ | prefecture, district, name, party, votes, votshare |
| `hr2024_pr_candidates.csv` | 比例区（候補者別） | block, party, name, rank, win_pr |
| `hr2024_pr_parties.csv` | 比例区（政党別） | block, party, votes, vshare, pr_wins |

### 第49回（2021年）

同様の構造で2021年版のデータも取得可能。

---

## ダッシュボードでの可視化イメージ

このデータから以下のような分析が可能です：

1. **政党別得票数の推移**
   - 2021年 vs 2024年の得票数比較
   - 比例区での政党支持率の変化

2. **選挙区分析**
   - 都道府県別の投票傾向
   - 激戦区の特定（得票率の差が小さい選挙区）

3. **候補者分析**
   - 当選者の平均年齢
   - 新人 vs 現職の当選率
   - 小選挙区当選 vs 比例復活の割合

4. **地域トレンド**
   - 都市部 vs 地方の投票傾向
   - ブロック別の政党支持率

---

## 事前準備

### 1. GCP の準備

```bash
# GCS バケット作成
gcloud storage buckets create gs://your-election-data-bucket \
  --location=asia-northeast1 \
  --project=your-gcp-project-id

# サービスアカウント作成 & 権限付与
gcloud iam service-accounts create election-pipeline-sa \
  --display-name="Election Pipeline Service Account"

gcloud projects add-iam-policy-binding your-gcp-project-id \
  --member="serviceAccount:election-pipeline-sa@your-gcp-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding your-gcp-project-id \
  --member="serviceAccount:election-pipeline-sa@your-gcp-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"

# キーファイルをダウンロード
gcloud iam service-accounts keys create ~/election-sa-key.json \
  --iam-account=election-pipeline-sa@your-gcp-project-id.iam.gserviceaccount.com
```

### 2. 環境変数のセット

```bash
export GCS_BUCKET_NAME="your-election-data-bucket"
export GCS_PROJECT_ID="your-gcp-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/election-sa-key.json"
```

---

## 実行方法

```bash
# 依存パッケージのインストール
pip install -r requirements.txt

# スクリプト実行
python upload_to_gcs.py
```

### 実行後のGCS構造

```
gs://your-election-data-bucket/
└── raw/
    ├── election_2024_districts_20250220_120000.csv
    ├── election_2024_pr_candidates_20250220_120001.csv
    ├── election_2024_pr_parties_20250220_120002.csv
    ├── election_2021_districts_20250220_120003.csv
    ├── election_2021_pr_candidates_20250220_120004.csv
    └── election_2021_pr_parties_20250220_120005.csv
```

---

## データの特徴

### すでに整形済み

- カラム名が英語・スネークケース
- 文字コードはUTF-8
- 欠損値は `NA`
- データ型も適切に設定されている

→ **クレンジングがほぼ不要** = Dataflow の学習に集中できる！

### 追加できる選挙年

スクリプトの `ELECTION_DATA_SOURCES` に追加するだけで過去データも取得可能：

```python
"2017": {
    "year": 2017,
    "election_no": 48,
    "files": {
        "districts": f"{YANAI_BASE_URL}/hr2017_districts.csv",
        # ...
    }
},
```

---

## データの出典表記

このデータを使用する場合、以下のように出典を明記してください：

```
データ出典: 矢内勇生「第50回衆議院議員総選挙データ」
https://yukiyanai.github.io/jp/resources/data/hr2024election.html
```

---

## GCP 試験との対応

| 本スクリプトの処理 | 試験で問われる概念 |
|---|---|
| 公開CSVの直接ダウンロード | 外部データソースとの連携 |
| GCS へのアップロード | Data Lake 構築 |
| タイムスタンプ付きファイル名 | データバージョニング |
| 年度別・種別ファイル分割 | データパーティショニングの基礎 |

---

## 次のステップ

➡ [02_dataflow_pipeline](../02_dataflow_pipeline/) - Dataflow でデータ変換

**Dataflow で行う処理**:
- 複数年のデータ統合
- 政党名の正規化（表記ゆれ対応）
- 選挙区コードの追加
- BigQuery 用スキーマへの適合
- 政党別集計テーブルの作成
