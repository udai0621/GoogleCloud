-- 都道府県サマリービュー
-- 目的: 都道府県ごとの政党勢力を可視化

CREATE OR REPLACE VIEW `${GCS_PROJECT_ID}.election_data.prefecture_summary` AS

SELECT 
  election_year,
  prefecture,
  prefecture_code,
  party_normalized,
  COUNT(*) as candidates,
  COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as winners,
  SUM(votes) as total_votes,
  ROUND(AVG(vshare), 2) as avg_vote_share,
  -- 都道府県内での勢力計算は後でJOINで計算
  COUNT(DISTINCT district_code) as districts_contested
FROM 
  `${GCS_PROJECT_ID}.election_data.districts`
GROUP BY 
  election_year,
  prefecture,
  prefecture_code,
  party_normalized
