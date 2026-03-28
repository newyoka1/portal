SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;


/* RERUNNABLE (FAST) USING TRUNCATE + INSERT
   MySQL 8.4, max 5 audiences per origin.
*/

/* =========================
   0) Session tuning (safe)
   ========================= */
SET SESSION tmp_table_size = 1024*1024*1024;
SET SESSION max_heap_table_size = 1024*1024*1024;
SET SESSION sort_buffer_size = 64*1024*1024;
SET SESSION join_buffer_size = 64*1024*1024;

/* =========================
   1) Generated district columns + indexes on fullnyvoter_2025 (create once)
   ========================= */
ALTER TABLE fullnyvoter_2025
  ADD COLUMN IF NOT EXISTS SDname_v VARCHAR(50)
    GENERATED ALWAYS AS (NULLIF(TRIM(SDname), '')) STORED,
  ADD COLUMN IF NOT EXISTS LDname_v VARCHAR(50)
    GENERATED ALWAYS AS (NULLIF(TRIM(LDname), '')) STORED,
  ADD COLUMN IF NOT EXISTS CDname_v VARCHAR(50)
    GENERATED ALWAYS AS (NULLIF(TRIM(CDname), '')) STORED;

CREATE INDEX IF NOT EXISTS idx_sdname_v ON fullnyvoter_2025 (SDname_v);
CREATE INDEX IF NOT EXISTS idx_ldname_v ON fullnyvoter_2025 (LDname_v);
CREATE INDEX IF NOT EXISTS idx_cdname_v ON fullnyvoter_2025 (CDname_v);

/* =========================
   2) Numbers table 1..5 (create once)
   ========================= */
CREATE TABLE IF NOT EXISTS util_nums_1_5 (
  n TINYINT UNSIGNED NOT NULL PRIMARY KEY
) ENGINE=MEMORY;

INSERT IGNORE INTO util_nums_1_5 (n) VALUES (1),(2),(3),(4),(5);

/* =========================
   3) Staging table (create once)
   ========================= */
CREATE TABLE IF NOT EXISTS fullvoter_origin_tokens (
  SDname VARCHAR(50) NOT NULL,
  LDname VARCHAR(50) NOT NULL,
  CDname VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  KEY idx_sd_aud (SDname, audience),
  KEY idx_ld_aud (LDname, audience),
  KEY idx_cd_aud (CDname, audience),
  KEY idx_aud (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* =========================
   4) Summary tables (create once)
   ========================= */
CREATE TABLE IF NOT EXISTS fullvoter_sd_audience_counts (
  SDname VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters BIGINT NOT NULL,
  PRIMARY KEY (SDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_ld_audience_counts (
  LDname VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters BIGINT NOT NULL,
  PRIMARY KEY (LDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_cd_audience_counts (
  CDname VARCHAR(50) NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters BIGINT NOT NULL,
  PRIMARY KEY (CDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_state_audience_counts (
  audience VARCHAR(255) NOT NULL,
  voters BIGINT NOT NULL,
  PRIMARY KEY (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* =========================
   5) Rebuild staging (TRUNCATE + INSERT)
   ========================= */
TRUNCATE TABLE fullvoter_origin_tokens;

INSERT INTO fullvoter_origin_tokens (SDname, LDname, CDname, audience)
SELECT
  COALESCE(f.SDname_v, 'UNKNOWN') AS SDname,
  COALESCE(f.LDname_v, 'UNKNOWN') AS LDname,
  COALESCE(f.CDname_v, 'UNKNOWN') AS CDname,
  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(f.origin, ',', n.n), ',', -1)) AS audience
FROM fullnyvoter_2025 f
JOIN util_nums_1_5 n
  ON f.origin IS NOT NULL
 AND TRIM(f.origin) <> ''
 AND n.n <= 1 + (LENGTH(f.origin) - LENGTH(REPLACE(f.origin, ',', '')))
WHERE TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(f.origin, ',', n.n), ',', -1)) <> '';

/* =========================
   6) Rebuild summaries (TRUNCATE + INSERT)
   ========================= */
TRUNCATE TABLE fullvoter_sd_audience_counts;

INSERT INTO fullvoter_sd_audience_counts (SDname, audience, voters)
SELECT SDname, audience, COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY SDname, audience;

TRUNCATE TABLE fullvoter_ld_audience_counts;

INSERT INTO fullvoter_ld_audience_counts (LDname, audience, voters)
SELECT LDname, audience, COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY LDname, audience;

TRUNCATE TABLE fullvoter_cd_audience_counts;

INSERT INTO fullvoter_cd_audience_counts (CDname, audience, voters)
SELECT CDname, audience, COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY CDname, audience;

TRUNCATE TABLE fullvoter_state_audience_counts;

INSERT INTO fullvoter_state_audience_counts (audience, voters)
SELECT audience, COUNT(*) AS voters
FROM fullvoter_origin_tokens
GROUP BY audience;
