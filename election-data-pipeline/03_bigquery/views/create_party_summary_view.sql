-- 政党サマリービュー
-- 目的: Looker Studio で簡単に使える集計済みビュー

CREATE OR REPLACE VIEW `${GCS_PROJECT_ID}.election_data.party_summary` AS

SELECT 
  election_year,
  party_normalized,
  COUNT(*) as total_candidates,
  COUNT(CASE WHEN win_smd = TRUE THEN 1 END) as smd_winners,
  COUNT(CASE WHEN win_pr = TRUE THEN 1 END) as pr_winners,
  COUNT(CASE WHEN win_smd = TRUE OR win_pr = TRUE THEN 1 END) as total_winners,
  SUM(votes) as total_votes,
  ROUND(AVG(vshare), 2) as avg_vote_share,
  ROUND(AVG(age), 1) as avg_candidate_age,
  ROUND(AVG(previous), 1) as avg_previous_wins,
  -- 重複立候補率
  ROUND(
    SAFE_DIVIDE(
      COUNT(CASE WHEN duplicate = TRUE THEN 1 END),
      COUNT(*)
    ) * 100,
    2
  ) as duplicate_rate,
  -- 小選挙区当選率
  ROUND(
    SAFE_DIVIDE(
      COUNT(CASE WHEN win_smd = TRUE THEN 1 END),
      COUNT(*)
    ) * 100,
    2
  ) as smd_win_rate
FROM 
  `${GCS_PROJECT_ID}.election_data.districts`
GROUP BY 
  election_year,
  party_normalized
