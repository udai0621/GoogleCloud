-- 都道府県別分析
-- 目的: 都道府県ごとの投票傾向と政党勢力を分析

WITH prefecture_party_stats AS (
  SELECT 
    election_year,
    prefecture,
    prefecture_code,
    party_normalized,
    COUNT(*) as candidates,
    COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as winners,
    SUM(votes) as total_votes
  FROM 
    `${GCS_PROJECT_ID}.election_data.districts`
  GROUP BY 
    election_year,
    prefecture,
    prefecture_code,
    party_normalized
),

prefecture_totals AS (
  SELECT 
    election_year,
    prefecture,
    prefecture_code,
    COUNT(DISTINCT district_code) as total_districts,
    COUNT(*) as total_candidates,
    SUM(votes) as total_votes
  FROM 
    `${GCS_PROJECT_ID}.election_data.districts`
  GROUP BY 
    election_year,
    prefecture,
    prefecture_code
)

SELECT 
  p.election_year,
  p.prefecture,
  p.prefecture_code,
  p.party_normalized,
  p.candidates,
  p.winners,
  p.total_votes,
  t.total_districts,
  -- 都道府県内での議席占有率
  ROUND(SAFE_DIVIDE(p.winners, t.total_districts) * 100, 2) as seat_share_pct,
  -- 都道府県内での得票占有率
  ROUND(SAFE_DIVIDE(p.total_votes, t.total_votes) * 100, 2) as vote_share_pct
FROM 
  prefecture_party_stats p
JOIN 
  prefecture_totals t
  ON p.election_year = t.election_year
  AND p.prefecture_code = t.prefecture_code
WHERE 
  p.winners > 0  -- 当選者がいる政党のみ
ORDER BY 
  p.election_year DESC,
  p.prefecture_code,
  p.winners DESC
