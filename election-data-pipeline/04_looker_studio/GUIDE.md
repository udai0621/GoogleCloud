# Looker Studio ダッシュボード作成ガイド

## 概要

BigQuery のデータを Looker Studio で可視化し、インタラクティブなダッシュボードを作成します。

```mermaid
graph LR
    A[BigQuery<br/>テーブル・ビュー] --> B[Looker Studio<br/>データソース]
    B --> C[レポート作成]
    C --> D[ダッシュボード<br/>公開]
    
    style A fill:#e8f5e9
    style B fill:#fff4e1
    style C fill:#e3f2fd
    style D fill:#fce4ec
```

---

## Step 1: Looker Studio にアクセス

### 1-1. Looker Studio を開く

```
https://lookerstudio.google.com/
```

### 1-2. 新しいレポートを作成

1. 「作成」ボタンをクリック
2. 「レポート」を選択

---

## Step 2: データソースを追加

### 2-1. BigQuery コネクタを選択

1. データソース選択画面で「BigQuery」を選択
2. 「承認」をクリック（初回のみ）

### 2-2. プロジェクトとテーブルを選択

```
プロジェクト: election-data-pipeline-487804
データセット: election_data
テーブル: party_summary
```

**「追加」をクリック**

### 2-3. データソース名を変更

- データソース名: `政党サマリー`
- 「レポートに追加」をクリック

---

## Step 3: 追加のデータソースを作成

同様の手順で以下のデータソースを追加：

### データソース2: 小選挙区データ
```
テーブル: districts
名前: 小選挙区データ
```

### データソース3: 都道府県サマリー
```
テーブル: prefecture_summary
名前: 都道府県サマリー
```

### データソース4: 激戦区データ（カスタムクエリ）

1. 「カスタムクエリ」を選択
2. 以下のSQLを貼り付け：

```sql
WITH deduped_data AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY election_year, district_code, name 
      ORDER BY votes DESC
    ) as row_num
  FROM 
    `election-data-pipeline-487804.election_data.districts`
  WHERE row_num = 1
),
ranked_candidates AS (
  SELECT 
    election_year,
    prefecture,
    district,
    district_code,
    name,
    party_normalized,
    votes,
    ROW_NUMBER() OVER (
      PARTITION BY election_year, district_code 
      ORDER BY votes DESC, name
    ) as rank_in_district
  FROM 
    deduped_data
)
SELECT 
  election_year,
  prefecture,
  district,
  district_code,
  MAX(CASE WHEN rank_in_district = 1 THEN name END) as first_place_name,
  MAX(CASE WHEN rank_in_district = 1 THEN party_normalized END) as first_place_party,
  MAX(CASE WHEN rank_in_district = 1 THEN votes END) as first_place_votes,
  MAX(CASE WHEN rank_in_district = 2 THEN name END) as second_place_name,
  MAX(CASE WHEN rank_in_district = 2 THEN party_normalized END) as second_place_party,
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) as second_place_votes,
  MAX(CASE WHEN rank_in_district = 1 THEN votes END) - 
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) as vote_margin,
  ROUND(
    SAFE_DIVIDE(
      MAX(CASE WHEN rank_in_district = 1 THEN votes END) - 
      MAX(CASE WHEN rank_in_district = 2 THEN votes END),
      MAX(CASE WHEN rank_in_district = 1 THEN votes END) + 
      MAX(CASE WHEN rank_in_district = 2 THEN votes END)
    ) * 100,
    2
  ) as margin_pct
FROM 
  ranked_candidates
WHERE 
  rank_in_district <= 2
GROUP BY 
  election_year,
  prefecture,
  district,
  district_code
HAVING 
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) IS NOT NULL
ORDER BY 
  election_year DESC,
  vote_margin ASC
```

3. データソース名: `激戦区データ`
4. 「追加」をクリック

---

## Step 4: ダッシュボード1「政権交代の可視化」を作成

### ページ1: 概要

#### タイトルを追加
1. 「挿入」→「テキスト」
2. テキスト: `衆議院選挙 2021 vs 2024 - 政権交代の分析`
3. フォントサイズ: 24pt
4. 配置: ページ上部中央

---

#### グラフ1: 議席数の変化（棒グラフ）

1. **グラフを追加**
   - 「グラフを追加」→「縦棒グラフ」→「積み上げ縦棒グラフ」

2. **データソース**: `政党サマリー`

3. **設定**:
   - **ディメンション**: `party_normalized`
   - **内訳ディメンション**: `election_year`
   - **指標**: `total_winners`（合計）

4. **スタイル**:
   - グラフタイトル: `政党別議席数の変化`
   - 凡例位置: 右
   - 色:
     - 2021: #90CAF9（青）
     - 2024: #EF5350（赤）

5. **フィルタを追加**:
   - フィルタ条件: `party_normalized` が以下のいずれか
     - 自由民主党
     - 立憲民主党
     - 日本維新の会
     - 公明党
     - 国民民主党

---

#### グラフ2: 得票数の推移（折れ線グラフ）

1. **グラフを追加**
   - 「グラフを追加」→「折れ線グラフ」

2. **データソース**: `政党サマリー`

3. **設定**:
   - **ディメンション**: `election_year`
   - **内訳ディメンション**: `party_normalized`
   - **指標**: `total_votes`（合計）

4. **スタイル**:
   - グラフタイトル: `政党別得票数の推移`
   - 線の太さ: 3
   - マーカーサイズ: 8
   - 色:
     - 自由民主党: #E53935（赤）
     - 立憲民主党: #1E88E5（青）
     - 日本維新の会: #43A047（緑）
     - 公明党: #FB8C00（オレンジ）
     - 国民民主党: #8E24AA（紫）

---

#### グラフ3: スコアカード（主要指標）

**自民党の議席変化**

1. **グラフを追加**
   - 「グラフを追加」→「スコアカード」

2. **データソース**: `政党サマリー`

3. **設定**:
   - **指標**: `total_winners`（合計）
   - **比較期間**: `election_year`（2021 vs 2024）
   - **フィルタ**: `party_normalized` = 自由民主党

4. **スタイル**:
   - タイトル: `自民党 議席数`
   - コンパクト表示: ON
   - 前期間との比較を表示: ON

**同様に作成**:
- 立憲民主党の議席変化
- 維新の議席変化

---

## Step 5: ダッシュボード2「激戦区マップ」を作成

### ページ2を追加

1. 「ページを追加」をクリック
2. ページ名: `激戦区分析`

---

#### グラフ4: 激戦区の地図（ジオマップ）

1. **グラフを追加**
   - 「グラフを追加」→「Google マップ」→「塗り分けマップ」

2. **データソース**: `激戦区データ`

3. **設定**:
   - **地域**: `prefecture`
   - **指標**: `vote_margin`（平均）

4. **スタイル**:
   - グラフタイトル: `都道府県別 激戦度マップ`
   - 色のスケール:
     - 最小値（0-1000票）: #C62828（赤）
     - 中間値（1000-5000票）: #FFA726（オレンジ）
     - 最大値（5000票以上）: #66BB6A（緑）

---

#### グラフ5: 激戦区TOP20（表）

1. **グラフを追加**
   - 「グラフを追加」→「表」

2. **データソース**: `激戦区データ`

3. **設定**:
   - **ディメンション**:
     - `prefecture`（都道府県）
     - `district`（選挙区）
     - `first_place_name`（1位）
     - `first_place_party`（政党）
     - `second_place_name`（2位）
     - `second_place_party`（政党）
   - **指標**:
     - `vote_margin`（得票差）
     - `margin_pct`（差率%）

4. **スタイル**:
   - グラフタイトル: `激戦区TOP20`
   - 行数: 20
   - 並び替え: `vote_margin` 昇順
   - 条件付き書式:
     - `margin_pct` < 1% → 背景色を赤に

---

## Step 6: ダッシュボード3「政党別詳細分析」

### ページ3を追加

1. 「ページを追加」をクリック
2. ページ名: `政党別詳細`

---

#### グラフ6: 世代交代分析（散布図）

1. **グラフを追加**
   - 「グラフを追加」→「散布図」

2. **データソース**: `政党サマリー`

3. **設定**:
   - **ディメンション**: `party_normalized`
   - **X軸**: `avg_candidate_age`（平均年齢）
   - **Y軸**: `smd_win_rate`（小選挙区当選率）
   - **バブルサイズ**: `total_candidates`（候補者数）
   - **色**: `party_normalized`

4. **スタイル**:
   - グラフタイトル: `政党別 候補者年齢 vs 当選率`
   - X軸ラベル: `平均年齢（歳）`
   - Y軸ラベル: `当選率（%）`

---

#### グラフ7: 重複立候補率（棒グラフ）

1. **グラフを追加**
   - 「グラフを追加」→「横棒グラフ」

2. **データソース**: `政党サマリー`

3. **設定**:
   - **ディメンション**: `party_normalized`
   - **指標**: `duplicate_rate`（重複立候補率）
   - **フィルタ**: `election_year` = 2024

4. **スタイル**:
   - グラフタイトル: `政党別 重複立候補率（2024年）`
   - 並び替え: `duplicate_rate` 降順

---

## Step 7: フィルタコントロールを追加

### ページレベルフィルタ

各ページに以下のフィルタを追加：

1. **選挙年フィルタ**
   - 「フィルタを追加」→「ドロップダウンリスト」
   - フィールド: `election_year`
   - デフォルト値: すべて

2. **政党フィルタ**（ページ1とページ3）
   - 「フィルタを追加」→「ドロップダウンリスト」
   - フィールド: `party_normalized`
   - デフォルト値: すべて

---

## Step 8: スタイルとレイアウトを調整

### テーマを設定

1. 「テーマとレイアウト」をクリック
2. テーマ: `シンプル`
3. 背景色: `#F5F5F5`（グレー）

### レイアウト調整

- グラフ間のスペース: 均等に配置
- グリッドスナップ: ON
- 配置: 中央揃え

---

## Step 9: ダッシュボードを共有

### 9-1. レポート名を変更

1. レポート名: `衆議院選挙分析ダッシュボード 2021-2024`

### 9-2. 共有設定

1. 「共有」ボタンをクリック
2. 共有方法を選択:
   - **リンクを知っている全員**: 閲覧可能
   - **特定のユーザー**: メールアドレスで招待

### 9-3. PDFエクスポート（オプション）

1. 「ダウンロード」→「PDF」
2. ページ: すべて
3. 「ダウンロード」をクリック

---

## 完成イメージ

### ページ1: 政権交代の可視化
```
+--------------------------------------------------+
|  衆議院選挙 2021 vs 2024 - 政権交代の分析        |
+--------------------------------------------------+
| [自民党] -110議席 | [立憲] +94議席 | [維新] +14議席 |
+--------------------------------------------------+
|  [政党別議席数の変化]  |  [得票数の推移]         |
|  [積み上げ棒グラフ]    |  [折れ線グラフ]         |
+--------------------------------------------------+
```

### ページ2: 激戦区分析
```
+--------------------------------------------------+
|  激戦区分析                                      |
+--------------------------------------------------+
|  [都道府県別 激戦度マップ]                       |
|  [日本地図 - 色分け]                             |
+--------------------------------------------------+
|  [激戦区TOP20]                                   |
|  [表形式 - 得票差順]                             |
+--------------------------------------------------+
```

### ページ3: 政党別詳細
```
+--------------------------------------------------+
|  政党別詳細分析                                  |
+--------------------------------------------------+
|  [候補者年齢 vs 当選率]  | [重複立候補率]        |
|  [散布図]                | [横棒グラフ]          |
+--------------------------------------------------+
```

---

## トラブルシューティング

### データが表示されない

**原因**: データソースの接続エラー

**解決策**:
```
1. 「リソース」→「追加済みのデータソースの管理」
2. データソースを選択
3. 「再接続」をクリック
```

---

### グラフが空白

**原因**: フィルタが厳しすぎる

**解決策**:
```
1. フィルタを確認
2. 「すべて」を選択
3. データが表示されるか確認
```

---

### カスタムクエリがエラー

**原因**: SQL構文エラー

**解決策**:
```
1. BigQuery コンソールでクエリをテスト
2. エラーメッセージを確認
3. 修正してから Looker Studio に貼り付け
```

---

## 次のステップ

✅ ダッシュボード作成完了後:
1. スクリーンショットを撮影
2. `screenshots/` ディレクトリに保存
3. README.md に掲載

---

## 参考リンク

- [Looker Studio 公式ドキュメント](https://support.google.com/looker-studio)
- [BigQuery コネクタガイド](https://support.google.com/looker-studio/answer/6370296)
- [カスタムクエリの使い方](https://support.google.com/looker-studio/answer/6370331)
