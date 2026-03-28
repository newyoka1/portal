UPDATE nys_audaince_causway.fullnyvoter_2025 f
JOIN nys_audaince_causway.causeway_universe_with_statevoterid_matched c
  ON f.StateVoterId = c.StateVoterId
SET f.origin = c.origin
WHERE (f.origin IS NULL OR TRIM(f.origin) = '');
