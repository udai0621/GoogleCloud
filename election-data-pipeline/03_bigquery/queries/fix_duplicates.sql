-- 重複データを削除
-- 各候補者の最初の1レコードのみ残す

CREATE OR REPLACE TABLE `${GCS_PROJECT_ID}.election_data.districts` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        election_year,
        district_code,
        name,
        party_normalized,
        votes
      ORDER BY prefecture  -- 任意の順序
    ) as row_num
  FROM 
    `${GCS_PROJECT_ID}.election_data.districts`
)
WHERE row_num = 1
