-- 政党別得票数の年次比較
-- 目的: 2021年と2024年の政党別得票数と当選者数を比較

WITH party_stats AS (
  SELECT
    election_year,
    party_normalized,
    COUNT(*) as total_candidates,
    COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as smd_winners,
    COUNT(CASE WHEN win_pr = TRUE THEN 1 END) as pr_winners,
    SUM(votes) as total_votes,
    ROUND(AVG(vshare), 2) as avg_vote_share,
    ROUND(AVG(age), 1) as avg_age
  FROM `${GCS_PROJECT_ID}.election_data.districts`
  GROUP BY
    election_year,
    party_normalized
)

SELECT
  election_year,
  party_normalized,
  total_candidates,
  smd_winners,
  pr_winners,
  (smd_winners + pr_winners) as total_winners,
  total_votes,
  avg_vote_share,
  avg_age,
  -- 当選率を計算
  ROUND(SAFE_DIVIDE(smd_winners, total_candidates) * 100, 2) as smd_win_rate,
  -- 1候補あたり平均得票数
  ROUND(SAFE_DIVIDE(total_votes, total_candidates), 0) as votes_per_candidate
FROM
  party_stats
ORDER BY
  election_year DESC,
  total_votes DESC

