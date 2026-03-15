-- 激戦区分析（シンプル版）
-- 目的: 得票差が小さい選挙区を特定し、政党間の競争を可視化

WITH ranked_candidates AS (
  SELECT 
    election_year,
    prefecture,
    district,
    district_code,
    name,
    party_normalized,
    votes,
    -- 各選挙区内での順位（得票順）
    ROW_NUMBER() OVER (
      PARTITION BY election_year, district_code 
      ORDER BY votes DESC, name  -- 同票の場合は名前順
    ) as rank_in_district
  FROM 
    `${GCS_PROJECT_ID}.election_data.districts`
)

SELECT 
  election_year,
  prefecture,
  district,
  district_code,
  -- 1位候補
  MAX(CASE WHEN rank_in_district = 1 THEN name END) as first_place_name,
  MAX(CASE WHEN rank_in_district = 1 THEN party_normalized END) as first_place_party,
  MAX(CASE WHEN rank_in_district = 1 THEN votes END) as first_place_votes,
  -- 2位候補
  MAX(CASE WHEN rank_in_district = 2 THEN name END) as second_place_name,
  MAX(CASE WHEN rank_in_district = 2 THEN party_normalized END) as second_place_party,
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) as second_place_votes,
  -- 得票差
  MAX(CASE WHEN rank_in_district = 1 THEN votes END) - 
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) as vote_margin,
  -- 得票差率
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
  -- 2位候補が存在する選挙区のみ（無投票当選を除外）
  MAX(CASE WHEN rank_in_district = 2 THEN votes END) IS NOT NULL
  -- 1位と2位が異なる候補者（データ重複を除外）
  AND MAX(CASE WHEN rank_in_district = 1 THEN name END) != 
      MAX(CASE WHEN rank_in_district = 2 THEN name END)
ORDER BY 
  election_year DESC,
  vote_margin ASC  -- 得票差が小さい順（激戦区）

