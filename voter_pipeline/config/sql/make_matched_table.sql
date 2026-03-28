DROP TABLE IF EXISTS NYS_Voter_Tagged.causeway_universe_with_statevoterid_matched;

CREATE TABLE NYS_Voter_Tagged.causeway_universe_with_statevoterid_matched AS
SELECT
  c.*,
  CAST(f.StateVoterId AS CHAR(50)) AS StateVoterId
FROM NYS_Voter_Tagged.causeway_universe c
JOIN NYS_Voter_Tagged.fullnyvoter_2025 f
  ON c.voter_key = f.voter_key
WHERE f.StateVoterId IS NOT NULL
  AND TRIM(f.StateVoterId) <> '';
