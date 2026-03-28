ALTER TABLE nys_audaince_causway.causeway_universe_with_statevoterid_matched
  ADD INDEX idx_voter_key (voter_key),
  ADD INDEX idx_statevoterid (StateVoterId);
