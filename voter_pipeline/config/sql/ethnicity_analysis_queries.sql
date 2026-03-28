-- =====================================================================
-- ETHNICITY ANALYSIS QUERIES
-- NYS Voter Tagging Pipeline - Enhanced with Ethnicity Standardization
-- =====================================================================

-- Set database
USE NYS_VOTER_TAGGING;

-- =====================================================================
-- 1. OVERALL ETHNICITY DISTRIBUTION
-- =====================================================================

-- Total voters by ethnicity
SELECT 
    StandardizedEthnicity,
    EthnicitySource,
    EthnicityConfidence,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fullnyvoter_2025), 2) as percentage
FROM fullnyvoter_2025
GROUP BY StandardizedEthnicity, EthnicitySource, EthnicityConfidence
ORDER BY voters DESC;

-- Ethnicity breakdown by source
SELECT 
    EthnicitySource,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fullnyvoter_2025), 2) as percentage
FROM fullnyvoter_2025
GROUP BY EthnicitySource
ORDER BY voters DESC;

-- =====================================================================
-- 2. CONGRESSIONAL DISTRICT ETHNICITY ANALYSIS
-- =====================================================================

-- Ethnicity distribution by Congressional District
SELECT 
    CDName,
    StandardizedEthnicity,
    voters,
    ROUND(voters * 100.0 / SUM(voters) OVER (PARTITION BY CDName), 2) as pct_of_district
FROM fullvoter_cd_ethnicity_counts
ORDER BY CDName, voters DESC;

-- Top 5 most diverse Congressional Districts
SELECT 
    CDName,
    COUNT(DISTINCT StandardizedEthnicity) as ethnicity_count,
    SUM(voters) as total_voters,
    GROUP_CONCAT(CONCAT(StandardizedEthnicity, ': ', voters) ORDER BY voters DESC SEPARATOR ' | ') as breakdown
FROM fullvoter_cd_ethnicity_counts
GROUP BY CDName
ORDER BY ethnicity_count DESC, total_voters DESC
LIMIT 5;

-- Districts with highest percentage of specific ethnicity
-- Example: Hispanic voters
SELECT 
    CDName,
    voters as hispanic_voters,
    ROUND(voters * 100.0 / (SELECT SUM(voters) FROM fullvoter_cd_ethnicity_counts c2 WHERE c2.CDName = c1.CDName), 2) as pct_hispanic
FROM fullvoter_cd_ethnicity_counts c1
WHERE StandardizedEthnicity = 'Hispanic'
ORDER BY pct_hispanic DESC
LIMIT 10;

-- =====================================================================
-- 3. AUDIENCE TARGETING BY ETHNICITY
-- =====================================================================

-- Find audiences with strong ethnic targeting
SELECT 
    audience,
    StandardizedEthnicity,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY audience), 2) as pct_of_audience
FROM fullnyvoter_2025
WHERE origin IS NOT NULL AND TRIM(origin) <> ''
GROUP BY audience, StandardizedEthnicity
HAVING COUNT(*) > 100  -- Filter to significant numbers
ORDER BY audience, voters DESC;

-- Most ethnically diverse audiences (by voter count)
SELECT 
    audience,
    COUNT(DISTINCT StandardizedEthnicity) as ethnicity_diversity,
    SUM(voters) as total_voters,
    GROUP_CONCAT(CONCAT(StandardizedEthnicity, ': ', voters) ORDER BY voters DESC SEPARATOR ' | ') as breakdown
FROM fullvoter_cd_audience_ethnicity_counts
GROUP BY audience
HAVING SUM(voters) > 500  -- Minimum audience size
ORDER BY ethnicity_diversity DESC, total_voters DESC
LIMIT 20;

-- =====================================================================
-- 4. TARGETED VS GENERAL POPULATION COMPARISON
-- =====================================================================

-- Compare ethnicity in a specific audience to general CD population
-- Example: Compare audience 'voters.csv' in CD '19'
SELECT 
    'Audience' as source,
    StandardizedEthnicity,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*) 
        FROM fullnyvoter_2025 f
        INNER JOIN fullvoter_audience_bridge b ON b.StateVoterId = f.StateVoterId
        WHERE b.audience = 'voters.csv' AND b.CDName = '19'
    ), 2) as percentage
FROM fullnyvoter_2025 f
INNER JOIN fullvoter_audience_bridge b ON b.StateVoterId = f.StateVoterId
WHERE b.audience = 'voters.csv' AND b.CDName = '19'
GROUP BY StandardizedEthnicity

UNION ALL

SELECT 
    'CD General' as source,
    StandardizedEthnicity,
    voters,
    ROUND(voters * 100.0 / (
        SELECT SUM(voters) 
        FROM fullvoter_cd_ethnicity_counts 
        WHERE CDName = '19'
    ), 2) as percentage
FROM fullvoter_cd_ethnicity_counts
WHERE CDName = '19'
ORDER BY source DESC, percentage DESC;

-- =====================================================================
-- 5. QUALITY CONTROL - ETHNICITY DATA SOURCES
-- =====================================================================

-- Count by ethnicity source quality
SELECT 
    EthnicitySource,
    EthnicityConfidence,
    StandardizedEthnicity,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fullnyvoter_2025), 2) as pct_total
FROM fullnyvoter_2025
GROUP BY EthnicitySource, EthnicityConfidence, StandardizedEthnicity
ORDER BY EthnicitySource, voters DESC;

-- Voters with predicted ethnicity (surname-based)
SELECT 
    StandardizedEthnicity,
    COUNT(*) as predicted_voters,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*) 
        FROM fullnyvoter_2025 
        WHERE EthnicitySource = 'predicted'
    ), 2) as pct_of_predicted
FROM fullnyvoter_2025
WHERE EthnicitySource = 'predicted'
GROUP BY StandardizedEthnicity
ORDER BY predicted_voters DESC;

-- Sample of predicted ethnicity voters for validation
SELECT 
    StateVoterId,
    FirstName,
    LastName,
    StandardizedEthnicity,
    EthnicitySource,
    StateEthnicity,
    ModeledEthnicity
FROM fullnyvoter_2025
WHERE EthnicitySource = 'predicted'
LIMIT 50;

-- =====================================================================
-- 6. MULTI-DIMENSIONAL ANALYSIS
-- =====================================================================

-- Audience performance by CD and ethnicity
SELECT 
    CDName,
    audience,
    StandardizedEthnicity,
    voters,
    ROUND(voters * 100.0 / SUM(voters) OVER (PARTITION BY CDName, audience), 2) as pct_of_cd_audience
FROM fullvoter_cd_audience_ethnicity_counts
WHERE voters > 50  -- Filter noise
ORDER BY CDName, audience, voters DESC;

-- Party affiliation by ethnicity (if OfficialParty exists)
SELECT 
    StandardizedEthnicity,
    OfficialParty,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY StandardizedEthnicity), 2) as pct_of_ethnicity
FROM fullnyvoter_2025
WHERE OfficialParty IS NOT NULL 
  AND TRIM(OfficialParty) <> ''
  AND StandardizedEthnicity <> 'Unknown'
GROUP BY StandardizedEthnicity, OfficialParty
ORDER BY StandardizedEthnicity, voters DESC;

-- Age range by ethnicity
SELECT 
    StandardizedEthnicity,
    AgeRange,
    COUNT(*) as voters,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY StandardizedEthnicity), 2) as pct_of_ethnicity
FROM fullnyvoter_2025
WHERE AgeRange IS NOT NULL 
  AND TRIM(AgeRange) <> ''
  AND StandardizedEthnicity <> 'Unknown'
GROUP BY StandardizedEthnicity, AgeRange
ORDER BY StandardizedEthnicity, AgeRange;

-- =====================================================================
-- 7. EXPORT QUERIES FOR ANALYSIS
-- =====================================================================

-- Export CD-level ethnicity summary for external analysis
SELECT 
    CDName,
    StandardizedEthnicity,
    voters,
    ROUND(voters * 100.0 / SUM(voters) OVER (PARTITION BY CDName), 2) as pct_of_district,
    (SELECT SUM(voters) FROM fullvoter_cd_ethnicity_counts WHERE CDName = c.CDName) as total_district_voters
FROM fullvoter_cd_ethnicity_counts c
ORDER BY CDName, voters DESC;

-- Export audience targeting effectiveness by ethnicity
SELECT 
    b.audience,
    f.StandardizedEthnicity,
    COUNT(*) as targeted_voters,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY b.audience), 2) as pct_of_audience,
    AVG(CAST(f.GeneralFrequency AS DECIMAL(10,2))) as avg_general_frequency,
    AVG(CAST(f.Age AS DECIMAL(10,2))) as avg_age
FROM fullvoter_audience_bridge b
INNER JOIN fullnyvoter_2025 f ON f.StateVoterId = b.StateVoterId
WHERE f.StandardizedEthnicity <> 'Unknown'
GROUP BY b.audience, f.StandardizedEthnicity
ORDER BY b.audience, targeted_voters DESC;
