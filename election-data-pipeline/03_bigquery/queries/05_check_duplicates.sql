-- データ重複の確認
-- 同じ選挙区・候補者が複数回出現しているか確認

SELECT 
  election_year,
  district_code,
  name,
  party_normalized,
  votes,
  COUNT(*) as duplicate_count
FROM 
  `${GCS_PROJECT_ID}.election_data.districts`
GROUP BY 
  election_year,
  district_code,
  name,
  party_normalized,
  votes
HAVING 
  COUNT(*) > 1
ORDER BY 
  duplicate_count DESC,
  election_year DESC
LIMIT 20
