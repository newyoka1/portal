/* BUILD SD/LD/CD/STATE AUDIENCE COUNT TABLES
   Skip rebuild when inputs have not changed, based on pipeline_metadata hashes.

   Requirements:
   - pipeline_metadata contains keys:
       fullvoter_input_hash
       causeway_input_hash
     (your Python pipeline already writes these)
   - origin has <= 5 tokens

   This script:
   - Ensures pipeline_metadata exists
   - Ensures generated district columns (SDname_v, LDname_v, CDname_v) exist
   - Ensures util_nums_1_5 exists
   - Ensures staging + 4 summary tables exist
   - Runs a stored procedure that:
       - Computes signature = fullvoter_input_hash|causeway_input_hash
       - If unchanged since last build, skips
       - Else TRUNCATE + INSERT rebuilds all tables and stores new signature
*/

/* -------------------------
   0) Metadata table
   ------------------------- */
CREATE TABLE IF NOT EXISTS pipeline_metadata (
  name VARCHAR(64) PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* -------------------------
   1) Numbers table 1..5
   ------------------------- */
CREATE TABLE IF NOT EXISTS util_nums_1_5 (
  n TINYINT UNSIGNED NOT NULL PRIMARY KEY
) ENGINE=MEMORY;

INSERT IGNORE INTO util_nums_1_5 (n) VALUES (1),(2),(3),(4),(5);

/* -------------------------
   2) Staging + summary tables (create once)
   ------------------------- */
CREATE TABLE IF NOT EXISTS fullvoter_origin_tokens (
  SDname   VARCHAR(50)  NOT NULL,
  LDname   VARCHAR(50)  NOT NULL,
  CDname   VARCHAR(50)  NOT NULL,
  audience VARCHAR(255) NOT NULL,
  KEY idx_sd_aud (SDname, audience),
  KEY idx_ld_aud (LDname, audience),
  KEY idx_cd_aud (CDname, audience),
  KEY idx_aud (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_sd_audience_counts (
  SDname   VARCHAR(50)  NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters   BIGINT       NOT NULL,
  PRIMARY KEY (SDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_ld_audience_counts (
  LDname   VARCHAR(50)  NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters   BIGINT       NOT NULL,
  PRIMARY KEY (LDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_cd_audience_counts (
  CDname   VARCHAR(50)  NOT NULL,
  audience VARCHAR(255) NOT NULL,
  voters   BIGINT       NOT NULL,
  PRIMARY KEY (CDname, audience),
  KEY idx_audience (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fullvoter_state_audience_counts (
  audience VARCHAR(255) NOT NULL,
  voters   BIGINT       NOT NULL,
  PRIMARY KEY (audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* -------------------------
   3) Stored procedure (does the conditional rebuild)
   ------------------------- */
DROP PROCEDURE IF EXISTS rebuild_audience_counts;
DELIMITER $$

CREATE PROCEDURE rebuild_audience_counts()
BEGIN
  DECLARE v_full_hash TEXT DEFAULT NULL;
  DECLARE v_cw_hash   TEXT DEFAULT NULL;
  DECLARE v_sig       TEXT DEFAULT NULL;
  DECLARE v_last_sig  TEXT DEFAULT NULL;

  /* Session tuning (safe) */
  SET SESSION tmp_table_size = 1024*1024*1024;
  SET SESSION max_heap_table_size = 1024*1024*1024;
  SET SESSION sort_buffer_size = 64*1024*1024;
  SET SESSION join_buffer_size = 64*1024*1024;

  /* Ensure generated district columns exist (no IF NOT EXISTS support assumed) */
  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "ALTER TABLE fullnyvoter_2025
         ADD COLUMN SDname_v VARCHAR(50)
         GENERATED ALWAYS AS (NULLIF(TRIM(SDname), '')) STORED",
      "SELECT 1"
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND column_name  = 'SDname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "ALTER TABLE fullnyvoter_2025
         ADD COLUMN LDname_v VARCHAR(50)
         GENERATED ALWAYS AS (NULLIF(TRIM(LDname), '')) STORED",
      "SELECT 1"
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND column_name  = 'LDname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "ALTER TABLE fullnyvoter_2025
         ADD COLUMN CDname_v VARCHAR(50)
         GENERATED ALWAYS AS (NULLIF(TRIM(CDname), '')) STORED",
      "SELECT 1"
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND column_name  = 'CDname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  /* Ensure indexes exist on generated columns */
  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "CREATE INDEX idx_sdname_v ON fullnyvoter_2025 (SDname_v)",
      "SELECT 1"
    )
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND index_name   = 'idx_sdname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "CREATE INDEX idx_ldname_v ON fullnyvoter_2025 (LDname_v)",
      "SELECT 1"
    )
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND index_name   = 'idx_ldname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sql := (
    SELECT IF(
      COUNT(*) = 0,
      "CREATE INDEX idx_cdname_v ON fullnyvoter_2025 (CDname_v)",
      "SELECT 1"
    )
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name   = 'fullnyvoter_2025'
      AND index_name   = 'idx_cdname_v'
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  /* Read current pipeline hashes */
  SELECT value INTO v_full_hash
  FROM pipeline_metadata
  WHERE name = 'fullvoter_input_hash'
  LIMIT 1;

  SELECT value INTO v_cw_hash
  FROM pipeline_metadata
  WHERE name = 'causeway_input_hash'
  LIMIT 1;

  /* Build signature. If either hash is missing, force rebuild. */
  SET v_sig = CONCAT(COALESCE(v_full_hash, 'MISSING_FULL'), '|', COALESCE(v_cw_hash, 'MISSING_CW'));

  /* Compare to last build signature */
  SELECT value INTO v_last_sig
  FROM pipeline_metadata
  WHERE name = 'audience_counts_build_sig'
  LIMIT 1;

  IF v_last_sig IS NOT NULL AND v_last_sig = v_sig THEN
    SELECT 'SKIP: audience count tables unchanged' AS status, v_sig AS signature;
  ELSE
    /* Rebuild staging */
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

    /* Rebuild SD */
    TRUNCATE TABLE fullvoter_sd_audience_counts;

    INSERT INTO fullvoter_sd_audience_counts (SDname, audience, voters)
    SELECT SDname, audience, COUNT(*) AS voters
    FROM fullvoter_origin_tokens
    GROUP BY SDname, audience;

    /* Rebuild LD */
    TRUNCATE TABLE fullvoter_ld_audience_counts;

    INSERT INTO fullvoter_ld_audience_counts (LDname, audience, voters)
    SELECT LDname, audience, COUNT(*) AS voters
    FROM fullvoter_origin_tokens
    GROUP BY LDname, audience;

    /* Rebuild CD */
    TRUNCATE TABLE fullvoter_cd_audience_counts;

    INSERT INTO fullvoter_cd_audience_counts (CDname, audience, voters)
    SELECT CDname, audience, COUNT(*) AS voters
    FROM fullvoter_origin_tokens
    GROUP BY CDname, audience;

    /* Rebuild State */
    TRUNCATE TABLE fullvoter_state_audience_counts;

    INSERT INTO fullvoter_state_audience_counts (audience, voters)
    SELECT audience, COUNT(*) AS voters
    FROM fullvoter_origin_tokens
    GROUP BY audience;

    /* Persist signature */
    INSERT INTO pipeline_metadata (name, value)
    VALUES ('audience_counts_build_sig', v_sig)
    ON DUPLICATE KEY UPDATE value = VALUES(value);

    SELECT 'REBUILT: audience count tables refreshed' AS status, v_sig AS signature;
  END IF;

END$$
DELIMITER ;

/* -------------------------
   4) Run it
   ------------------------- */
CALL rebuild_audience_counts();
