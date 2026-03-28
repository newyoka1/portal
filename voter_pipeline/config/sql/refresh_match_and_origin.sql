/* ============================================================
   NYS_Voter_Tagged: MAX SPEED refresh of matched + origin update
   - Rerunnable
   - Builds lean matched table
   - Adds best indexes
   - Batch updates origin with commits per batch
   ============================================================ */

USE NYS_Voter_Tagged;

/* Optional session tuning (safe-ish, session only)
   If you are not using replication/binlog, this helps.
   Comment these out if you do not want any behavior changes.
*/
SET SESSION transaction_isolation = 'READ-COMMITTED';
SET SESSION innodb_lock_wait_timeout = 120;
SET SESSION sql_safe_updates = 0;

/* 1) Build lean matched table (only columns needed for update) */
DROP TABLE IF EXISTS causeway_universe_with_statevoterid_matched;

CREATE TABLE causeway_universe_with_statevoterid_matched
ENGINE=InnoDB
AS
SELECT
    f.StateVoterId,
    f.voter_key,
    c.origin
FROM causeway_universe c
JOIN fullnyvoter_2025 f
  ON f.voter_key = c.voter_key
WHERE f.StateVoterId IS NOT NULL
  AND f.StateVoterId <> ''
  AND TRIM(f.StateVoterId) <> '';

/* 2) Indexing: make joins and batching fast */
ALTER TABLE causeway_universe_with_statevoterid_matched
  MODIFY StateVoterId VARCHAR(50) NOT NULL,
  MODIFY voter_key VARCHAR(255) NOT NULL,
  ADD PRIMARY KEY (StateVoterId),
  ADD INDEX idx_voter_key (voter_key);

-- fullnyvoter_2025 already has PRIMARY KEY (StateVoterId) from your loader
-- ensure voter_key index exists (skip if it already exists, if it errors, just remove this block)
ALTER TABLE fullnyvoter_2025
  ADD INDEX idx_full_voter_key (voter_key);

/* 3) Batch update origin to avoid huge locks and log pressure */
DROP PROCEDURE IF EXISTS sp_update_origin_batched;
DELIMITER $$

CREATE PROCEDURE sp_update_origin_batched(IN p_batch INT)
BEGIN
    DECLARE rows_affected INT DEFAULT 1;

    WHILE rows_affected > 0 DO
        START TRANSACTION;

        UPDATE fullnyvoter_2025 f
        JOIN causeway_universe_with_statevoterid_matched m
          ON m.StateVoterId = f.StateVoterId
        SET f.origin = m.origin
        WHERE (f.origin IS NULL OR f.origin = '')
        LIMIT p_batch;

        SET rows_affected = ROW_COUNT();

        COMMIT;
    END WHILE;
END$$

DELIMITER ;

/* Run batches.
   50k is a good starting point on SSD. If you have plenty of IOPS, try 100k.
*/
CALL sp_update_origin_batched(50000);

/* Cleanup */
DROP PROCEDURE IF EXISTS sp_update_origin_batched;

/* 4) Fast validation summary */
SELECT
  (SELECT COUNT(*) FROM fullnyvoter_2025) AS full_total,
  (SELECT COUNT(*) FROM fullnyvoter_2025 WHERE origin IS NOT NULL AND origin <> '') AS full_origin_filled,
  (SELECT COUNT(*) FROM causeway_universe_with_statevoterid_matched) AS matched_rows,
  (SELECT COUNT(*)
   FROM fullnyvoter_2025 f2
   JOIN causeway_universe_with_statevoterid_matched m2
     ON m2.StateVoterId = f2.StateVoterId
   WHERE (f2.origin IS NULL OR f2.origin = '')
  ) AS matched_missing_origin;
