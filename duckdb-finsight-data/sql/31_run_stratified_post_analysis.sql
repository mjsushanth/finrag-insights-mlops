

-- ============================================================================
-- SECTION E: FINAL VALIDATION & SUMMARY
-- ============================================================================

SELECT 
    '═════════════════════════════════════════' as separator,
    'SECTION E: FINAL VALIDATION' as status;

SELECT * FROM sample_1m_finrag;

-- ──────────────────────────────────────────────────────────────────────────
-- V1: Overall statistics
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '1. OVERALL STATISTICS' as validation_check,
    COUNT(*) as total_sentences,
    COUNT(DISTINCT cik_int) as unique_companies,
    COUNT(DISTINCT docID) as unique_filings,
    COUNT(DISTINCT section) as unique_sections,
    COUNT(DISTINCT report_year) as unique_years,
    ROUND(AVG(char_count), 1) as avg_char_length,
    ROUND(AVG(word_count_approx), 1) as avg_word_count
FROM sample_1m_finrag;


-- ──────────────────────────────────────────────────────────────────────────
-- V2: Temporal distribution vs targets
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '2. TEMPORAL DISTRIBUTION' as validation_check,
    sf.temporal_bin,
    COUNT(*) as actual_sampled,
    ba.target_n as target,
    COUNT(*) - ba.target_n as difference,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_sample,
    ba.sampling_rate_pct as planned_rate,
    CASE 
        WHEN ABS(COUNT(*) - ba.target_n) <= 5000 THEN '✓ Within tolerance'
        ELSE '⚠️ Exceeds tolerance'
    END as target_check
FROM sample_1m_finrag sf
LEFT JOIN bin_allocations ba ON sf.temporal_bin = ba.temporal_bin
GROUP BY sf.temporal_bin, ba.target_n, ba.sampling_rate_pct
ORDER BY sf.temporal_bin;


-- ──────────────────────────────────────────────────────────────────────────
-- V3: Modern era completeness (CRITICAL)
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '3. MODERN ERA COVERAGE (CRITICAL)' as validation_check,
    COUNT(*) as sentences_sampled,
    654096 as population_available,
    ROUND(COUNT(*) * 100.0 / 654096, 2) as coverage_pct,
    CASE 
        WHEN COUNT(*) >= 654096 * 0.99 THEN '✓✓ COMPLETE (≥99%)'
        WHEN COUNT(*) >= 654096 * 0.95 THEN '✓ NEAR-COMPLETE (95-99%)'
        WHEN COUNT(*) >= 654096 * 0.90 THEN '○ HIGH (90-95%)'
        ELSE '△ PARTIAL (<90%)'
    END as completeness_level
FROM sample_1m_finrag
WHERE temporal_bin = 'bin_2016_2020';


-- ──────────────────────────────────────────────────────────────────────────
-- V4: Section coverage (all 20 sections should be present in all bins)
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '4. SECTION COVERAGE PER BIN' as validation_check,
    temporal_bin,
    COUNT(DISTINCT section) as n_sections_present,
    CASE 
        WHEN COUNT(DISTINCT section) = 20 THEN '✓ Complete (20/20)'
        ELSE '⚠️ Missing ' || (20 - COUNT(DISTINCT section)) || ' sections'
    END as section_completeness
FROM sample_1m_finrag
GROUP BY temporal_bin
ORDER BY temporal_bin;


-- ──────────────────────────────────────────────────────────────────────────
-- V5: Company coverage
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '5. COMPANY COVERAGE' as validation_check,
    COUNT(DISTINCT cik_int) as companies_in_sample,
    75 as companies_expected,
    75 - COUNT(DISTINCT cik_int) as missing_companies,
    CASE 
        WHEN COUNT(DISTINCT cik_int) = 75 THEN '✓ All companies present'
        ELSE '⚠️ Missing ' || (75 - COUNT(DISTINCT cik_int)) || ' companies'
    END as coverage_status
FROM sample_1m_finrag;


-- ──────────────────────────────────────────────────────────────────────────
-- V6: Feature flag distribution
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '6. FEATURE FLAG STATISTICS' as validation_check,
    COUNT(CASE WHEN likely_kpi = true THEN 1 END) as n_kpi_sentences,
    COUNT(CASE WHEN has_numbers = true THEN 1 END) as n_numeric_sentences,
    COUNT(CASE WHEN is_table_like = true THEN 1 END) as n_table_sentences,
    COUNT(CASE WHEN has_forward_looking = true THEN 1 END) as n_forward_looking,
    COUNT(CASE WHEN has_comparison = true THEN 1 END) as n_comparative,
    COUNT(CASE WHEN is_material = true THEN 1 END) as n_material,
    COUNT(CASE WHEN mentions_years = true THEN 1 END) as n_year_mentions,
    COUNT(CASE WHEN has_risk_language = true THEN 1 END) as n_risk_related,
    COUNT(CASE WHEN is_recent = true THEN 1 END) as n_recent
FROM sample_1m_finrag;


-- ──────────────────────────────────────────────────────────────────────────
-- V7: Retrieval signal score distribution
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '7. SIGNAL SCORE DISTRIBUTION' as validation_check,
    retrieval_signal_score as score,
    COUNT(*) as n_sentences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_sample
FROM sample_1m_finrag
GROUP BY retrieval_signal_score
ORDER BY retrieval_signal_score DESC;


-- ──────────────────────────────────────────────────────────────────────────
-- V8: High-value content identification (score >= 5)
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '8. HIGH-VALUE CONTENT (signal score ≥5)' as validation_check,
    COUNT(*) as n_high_value_sentences,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sample_1m_finrag), 2) as pct_high_value,
    section_priority,
    COUNT(*) as count_by_priority
FROM sample_1m_finrag
WHERE retrieval_signal_score >= 5
GROUP BY section_priority
ORDER BY count_by_priority DESC;


-- ──────────────────────────────────────────────────────────────────────────
-- V9: Data quality checks (nulls, invalid values)
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '9. DATA QUALITY CHECKS' as validation_check,
    COUNT(CASE WHEN cik IS NULL THEN 1 END) as null_ciks,
    COUNT(CASE WHEN sentence IS NULL THEN 1 END) as null_sentences,
    COUNT(CASE WHEN section IS NULL THEN 1 END) as null_sections,
    COUNT(CASE WHEN temporal_bin IS NULL THEN 1 END) as null_bins,
    COUNT(CASE WHEN sample_id IS NULL THEN 1 END) as null_ids,
    COUNT(CASE WHEN row_hash IS NULL THEN 1 END) as null_hashes,
    CASE 
        WHEN COUNT(CASE WHEN cik IS NULL THEN 1 END) = 0 
         AND COUNT(CASE WHEN sentence IS NULL THEN 1 END) = 0
        THEN '✓ No nulls in critical fields'
        ELSE '⚠️ Nulls detected'
    END as quality_status
FROM sample_1m_finrag;


-- ──────────────────────────────────────────────────────────────────────────
-- V10: Per-company balance check
-- ──────────────────────────────────────────────────────────────────────────

WITH company_stats AS (
    SELECT 
        cik_int,
        name as company_name,
        COUNT(*) as n_sentences
    FROM sample_1m_finrag
    GROUP BY cik_int, name
)
SELECT 
    '10. COMPANY DISTRIBUTION' as validation_check,
    MIN(n_sentences) as min_per_company,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY n_sentences) as median_per_company,
    MAX(n_sentences) as max_per_company,
    ROUND(AVG(n_sentences), 0) as avg_per_company,
    ROUND(MAX(n_sentences) * 1.0 / NULLIF(MIN(n_sentences), 0), 2) as imbalance_ratio,
    CASE 
        WHEN MAX(n_sentences) * 1.0 / NULLIF(MIN(n_sentences), 0) < 10 
        THEN '✓ Balanced distribution'
        ELSE '○ Some imbalance (expected)'
    END as balance_status
FROM company_stats;




-- ═════════════════════════════════════════════════════════════════════════
-- FINAL SUCCESS SUMMARY
-- ═════════════════════════════════════════════════════════════════════════

WITH summary_stats AS (
    SELECT 
        COUNT(*) as final_sample_size,
        (SELECT sample_size_n FROM params) as target_size,
        COUNT(*) - (SELECT sample_size_n FROM params) as overage,
        COUNT(DISTINCT cik_int) as n_companies,
        COUNT(DISTINCT docID) as n_filings,
        MAX(sample_id) as max_sample_id,
        (SELECT sample_version FROM params) as version,
        MIN(sample_created_at) as created_at
    FROM sample_1m_finrag
)
SELECT 
    '✓✓✓ SAMPLING PROCEDURE COMPLETE' as status,
    final_sample_size || ' sentences sampled' as result,
    'Target: ' || target_size || ' | Overage: +' || overage || ' (+' || 
        ROUND(overage * 100.0 / target_size, 2) || '%)' as accuracy,
    n_companies || ' companies, ' || n_filings || ' filings' as coverage,
    'Version: ' || version as version_info,
    'Created: ' || created_at as timestamp_info
FROM summary_stats;




-- ============================================================================
-- ADD SECTION_NAME COLUMN TO EXISTING TABLE
-- Simple enrichment via LEFT JOIN to dimension table
-- ============================================================================

-- Step 1: Add the new column
ALTER TABLE sample_1m_finrag 
ADD COLUMN section_name VARCHAR;

SELECT 'Column added successfully' as status;


-- Step 2: Populate it with a LEFT JOIN to dimension table
UPDATE sample_1m_finrag
SET section_name = (
    SELECT ds.section_name
    FROM sampler.main.dim_sec_sections ds
    WHERE CAST(sample_1m_finrag.section AS DOUBLE) = ds.section_id
);

SELECT 'Section names populated' as status;


-- ============================================================================
-- Step 3: Validation - Check the results
-- ============================================================================

SELECT 
    section,
    section_name,
    COUNT(*) as n_sentences,
    COUNT(DISTINCT cik_int) as n_companies
FROM sample_1m_finrag
GROUP BY section, section_name
ORDER BY section;


-- Step 4: Check for any nulls (unmapped sections)
SELECT 
    'NULL CHECK' as validation,
    COUNT(*) as total_rows,
    COUNT(section_name) as populated_rows,
    COUNT(*) - COUNT(section_name) as null_rows,
    CASE 
        WHEN COUNT(*) = COUNT(section_name) THEN '✓ All sections mapped'
        ELSE '⚠️ Some sections unmapped'
    END as status
FROM sample_1m_finrag;


-- Step 5: Preview enriched data
SELECT 
    cik_int,
    name,
    section,
    section_name,
    report_year,
    sentence[1:120] as sentence_preview
FROM sample_1m_finrag
WHERE likely_kpi = true
LIMIT 20;


