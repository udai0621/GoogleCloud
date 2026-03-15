-- 選挙年次比較（2021 vs 2024）
-- 目的: 政党ごとの勢力変化を明確化

WITH year_2021 AS (
  SELECT 
    party_normalized,
    COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as winners_2021,
    SUM(votes) as votes_2021,
    COUNT(*) as candidates_2021
  FROM 
    `${GCS_PROJECT_ID}.election_data.districts`
  WHERE
    election_year = 2021 
  GROUP BY
    party_normalized
),

year_2024 AS (
  SELECT
    party_normalized,
    COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as winners_2024,
    SUM(votes) as votes_2024,
    COUNT(*) as candidates_2024
  FROM
    `${GCS_PROJECT_ID}.election_data.districts`
  WHERE
    election_year = 2024 
  GROUP BY
    party_normalized
)

SELECT
  COALESCE(a.party_normalized, b.party_normalized) as party_normalized,
  -- 2021年データ
  COALESCE(a.candidates_2021, 0) as candidates_2021,
  COALESCE(a.winners_2021, 0) as winners_2021,
  COALESCE(a.votes_2021, 0) as votes_2021,
  -- 2024年
  COALESCE(b.candidates_2024, 0) as candidates_2024,
  COALESCE(b.winners_2024, 0) as winners_2024,
  COALESCE(b.votes_2024, 0) as votes_2024,
  -- 変化量
  COALESCE(b.winners_2024, 0) - COALESCE(a.winners_2021, 0) as seat_change,
  COALESCE(b.votes_2024, 0) - COALESCE(a.votes_2021, 0) as vote_change,
  -- 変化率
  ROUND(
    SAFE_DIVIDE(
      COALESCE(b.winners_2024, 0) - COALESCE(a.winners_2021, 0),
      NULLIF(a.winners_2021, 0)
    ) * 100,
    2
  ) as seat_change_pct,
  ROUND(
    SAFE_DIVIDE(
      COALESCE(b.votes_2024, 0) - COALESCE(a.votes_2021, 0),
      NULLIF(a.votes_2021, 0)
    ) * 100,
    2 
  ) as vote_change_pct
FROM year_2021 a 
FULL OUTER JOIN 
  year_2024 b 
  ON a.party_normalized = b.party_normalized
ORDER BY
  COALESCE(b.winners_2024, 0) DESC 
