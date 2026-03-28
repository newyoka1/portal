/* 0) Drop old outputs */
DROP TABLE IF EXISTS fullvoter_origin_tokens;
DROP TABLE IF EXISTS fullvoter_sd_audience_counts;
DROP TABLE IF EXISTS fullvoter_ld_audience_counts;
DROP TABLE IF EXISTS fullvoter_cd_audience_counts;
DROP TABLE IF EXISTS fullvoter_state_audience_counts;

/* 1) Staging table: one row per voter per audience token */
CREATE TABLE fullvoter_origin_tokens (
  SDname VARCHAR(50) NOT NULL,
  LDname VARCHAR(50) NOT NULL,
  CDname VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  KEY idx_sd_aud (SDname, audience),
  KEY idx_ld_aud (LDname, audience),
  KEY idx_cd_aud (CDname, audience),
  KEY idx_aud (audience)
) ENGINE=InnoDB;

/* 2) Populate staging table (split origin into up to 5 tokens) */
INSERT INTO fullvoter_origin_tokens (SDname, LDname, CDname, audience)
SELECT
  COALESCE(NULLIF(TRIM(f.SDname), ''), 'UNKNOWN') AS SDname,
  COALESCE(NULLIF(TRIM(f.LDname), ''), 'UNKNOWN') AS LDname,
  COALESCE(NULLIF(TRIM(f.CDname), ''), 'UNKNOWN') AS CDname,
  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(f.origin, ',', n.n), ',', -1)) AS audience
FROM fullnyvoter_2025 f
JOIN util_nums_1_5 n
  ON f.origin IS NOT NULL
 AND TRIM(f.origin) <> ''
 AND n.n <= 1 + (LENGTH(f.origin) - LENGTH(REPLACE(f.origin, ',', '')))
WHERE TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(f.origin, ',', n.n), ',', -1)) <> '';

/* Optional: if you want to guarantee no accidental duplicates inside a single origin string,
   you can rebuild staging into a distinct form, but it costs extra time:
   CREATE TABLE fullvoter_origin_tokens_distinct AS
   SELECT DISTINCT SDname, LDname, CDname, audience, StateVoterId ... (requires adding StateVoterId to staging)
*/

/* 3) SD counts */
CREATE TABLE fullvoter_sd_audience_counts AS
SELECT
  SDname,
  audience,
  COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY SDname, audience;

ALTER TABLE fullvoter_sd_audience_counts
  ADD PRIMARY KEY (SDname, audience),
  ADD KEY idx_audience (audience);

/* 4) LD counts */
CREATE TABLE fullvoter_ld_audience_counts AS
SELECT
  LDname,
  audience,
  COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY LDname, audience;

ALTER TABLE fullvoter_ld_audience_counts
  ADD PRIMARY KEY (LDname, audience),
  ADD KEY idx_audience (audience);

/* 5) CD counts */
CREATE TABLE fullvoter_cd_audience_counts AS
SELECT
  CDname,
  audience,
  COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY CDname, audience;

ALTER TABLE fullvoter_cd_audience_counts
  ADD PRIMARY KEY (CDname, audience),
  ADD KEY idx_audience (audience);

/* 6) Statewide counts */
CREATE TABLE fullvoter_state_audience_counts AS
SELECT
  audience,
  COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY audience;

ALTER TABLE fullvoter_state_audience_counts
  ADD PRIMARY KEY (audience);

/* 7) Optional: keep or drop staging table
   Keeping it makes future reporting faster and more flexible.
   Drop it if you only need the 4 outputs. */
-- DROP TABLE fullvoter_origin_tokens;
