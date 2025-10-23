-- ═══════════════════════════════════════════════════════════════════
-- IDENTIFIERS (9 columns)
-- ═══════════════════════════════════════════════════════════════════
cik, cik_int, name, ticker,          -- Company identification
docID, sentenceID,                   -- Document + sentence traceability
section, form, sic                   -- Context fields

-- ═══════════════════════════════════════════════════════════════════
-- CONTENT (1 column)
-- ═══════════════════════════════════════════════════════════════════
sentence                             -- The actual text

-- ═══════════════════════════════════════════════════════════════════
-- TEMPORAL (3 columns)
-- ═══════════════════════════════════════════════════════════════════
report_year, reportDate, temporal_bin

-- ═══════════════════════════════════════════════════════════════════
-- RAG FEATURES (3 columns - YOUR CUSTOM LOGIC)
-- ═══════════════════════════════════════════════════════════════════
likely_kpi, has_numbers, has_comparison

-- ═══════════════════════════════════════════════════════════════════
-- AUDIT (6 columns)
-- ═══════════════════════════════════════════════════════════════════
sample_created_at, sample_version, row_hash,
last_modified_date, source_file_path, load_method

-- ═══════════════════════════════════════════════════════════════════
-- OPTIONAL (1/2 columns - for scoring/filtering)
-- ═══════════════════════════════════════════════════════════════════
section_priority, section_name 

-- ═══════════════════════════════════════════════════════════════════
-- ═══════════════════════════════════════════════════════════════════




-- ============================================================================
-- CREATE SUBSET TABLES: 21 Companies with Lean Column Selection
-- Author: Joel Markapudi
-- Purpose: Production-ready tables for RAG system development
-- ============================================================================

-- ──────────────────────────────────────────────────────────────────────────
-- PARAMETERS
-- ──────────────────────────────────────────────────────────────────────────

SET VARIABLE export_base_path = 'D:/JoelDesktop folds_24/NEU FALL2025/MLops IE7374 Project/finrag-insights-mlops/data/exports';
SET VARIABLE export_version = 'v2.0';
SET VARIABLE export_timestamp = CURRENT_TIMESTAMP;

SELECT 'Parameters set' as status;


-- ============================================================================
-- TABLE 1: ALL TEMPORAL BINS (2006-2020)
-- Naming: finrag_21companies_allbins_v2.0.parquet
-- Use Case: Full historical analysis, temporal drift studies
-- ============================================================================

DROP TABLE IF EXISTS finrag_21companies_allbins;

CREATE TABLE finrag_21companies_allbins AS
SELECT 
    -- ═══════════════════════════════════════════════════════════════════
    -- IDENTIFIERS (9 columns)
    -- ═══════════════════════════════════════════════════════════════════
    cik,
    cik_int,
    name,
    tickers,
    docID,
    sentenceID,
    section,
    section_name,           
    form,
    sic,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- CONTENT (1 column)
    -- ═══════════════════════════════════════════════════════════════════
    sentence,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- TEMPORAL (3 columns)
    -- ═══════════════════════════════════════════════════════════════════
    report_year,
    reportDate,
    temporal_bin,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- RAG FEATURES (3 columns - Core signals only)
    -- ═══════════════════════════════════════════════════════════════════
    likely_kpi,
    has_numbers,
    has_comparison,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- OPTIONAL (1/2 columns - Scoring/Priority)
    -- ═══════════════════════════════════════════════════════════════════
    section_priority,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- AUDIT (6 columns)
    -- ═══════════════════════════════════════════════════════════════════
    sample_created_at,
    last_modified_date,
    sample_version,
    source_file_path,
    load_method,
    row_hash

FROM sample_1m_finrag
WHERE cik_int IN (
    SELECT cik_int FROM finrag_tgt_comps_21
)
ORDER BY cik_int, report_year DESC, section, sentenceID;


SELECT 'TABLE 1 CREATED: finrag_21companies_allbins' as status;


-- ──────────────────────────────────────────────────────────────────────────
-- TABLE 1: Validation Summary
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '═══ TABLE 1: ALL BINS VALIDATION ═══' as report_section,
    COUNT(*) as total_sentences,
    COUNT(DISTINCT cik_int) as n_companies,
    COUNT(DISTINCT docID) as n_filings,
    COUNT(DISTINCT temporal_bin) as n_temporal_bins,
    COUNT(DISTINCT section) as n_sections,
    MIN(report_year) as earliest_year,
    MAX(report_year) as latest_year,
FROM finrag_21companies_allbins;

-- Breakdown by temporal bin
SELECT 
    'TABLE 1: Temporal Distribution' as check_type,
    temporal_bin,
    COUNT(*) as n_sentences,
    COUNT(DISTINCT cik_int) as n_companies,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM finrag_21companies_allbins
GROUP BY temporal_bin
ORDER BY temporal_bin;

--#,check_type,temporal_bin,n_sentences,n_companies,pct_of_total
--1,TABLE 1: Temporal Distribution,bin_2006_2009,"40,778",18,14.64
--2,TABLE 1: Temporal Distribution,bin_2010_2015,"62,159",20,22.31
--3,TABLE 1: Temporal Distribution,bin_2016_2020,"175,619",20,63.05

-- ============================================================================
-- TABLE 2: MODERN ERA ONLY (2016-2020)
-- Naming: finrag_21companies_modern_v2.0.parquet
-- Use Case: Production RAG, highest quality data, API integration
-- ============================================================================

DROP TABLE IF EXISTS finrag_21companies_modern;

CREATE TABLE finrag_21companies_modern AS
SELECT 
    -- ═══════════════════════════════════════════════════════════════════
    -- IDENTIFIERS (9 columns)
    -- ═══════════════════════════════════════════════════════════════════
    cik,
    cik_int,
    name,
    tickers,
    docID,
    sentenceID,
    section,
    section_name,           
    form,
    sic,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- CONTENT (1 column)
    -- ═══════════════════════════════════════════════════════════════════
    sentence,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- TEMPORAL (3 columns)
    -- ═══════════════════════════════════════════════════════════════════
    report_year,
    reportDate,
    temporal_bin,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- RAG FEATURES (3 columns - Core signals only)
    -- ═══════════════════════════════════════════════════════════════════
    likely_kpi,
    has_numbers,
    has_comparison,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- OPTIONAL (1/2 columns - Scoring/Priority)
    -- ═══════════════════════════════════════════════════════════════════
    section_priority,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- AUDIT (6 columns)
    -- ═══════════════════════════════════════════════════════════════════
    sample_created_at,
    last_modified_date,
    sample_version,
    source_file_path,
    load_method,
    row_hash

FROM sample_1m_finrag
WHERE cik_int IN (
    SELECT cik_int FROM finrag_tgt_comps_21
)
AND temporal_bin = 'bin_2016_2020'  -- MODERN ERA ONLY
ORDER BY cik_int, report_year DESC, section, sentenceID;

SELECT 'TABLE 2 CREATED: finrag_21companies_modern' as status;


-- ──────────────────────────────────────────────────────────────────────────
-- TABLE 2: Validation Summary
-- ──────────────────────────────────────────────────────────────────────────

SELECT 
    '═══ TABLE 2: MODERN ERA VALIDATION ═══' as report_section,
    COUNT(*) as total_sentences,
    COUNT(DISTINCT cik_int) as n_companies,
    COUNT(DISTINCT docID) as n_filings,
    COUNT(DISTINCT report_year) as n_years,
    COUNT(DISTINCT section) as n_sections,
    MIN(report_year) as earliest_year,
    MAX(report_year) as latest_year,
FROM finrag_21companies_modern;

-- Year-by-year distribution
SELECT 
    'TABLE 2: Year Distribution' as check_type,
    report_year,
    COUNT(*) as n_sentences,
    COUNT(DISTINCT cik_int) as n_companies,
    COUNT(DISTINCT docID) as n_filings
FROM finrag_21companies_modern
GROUP BY report_year
ORDER BY report_year DESC;


-- ============================================================================
-- COMPARATIVE ANALYSIS: Table 1 vs Table 2
-- ============================================================================

WITH table_comparison AS (
    SELECT 
        'TABLE 1: All Bins (2006-2020)' as dataset,
        COUNT(*) as n_sentences,
        COUNT(DISTINCT cik_int) as n_companies,
        COUNT(DISTINCT docID) as n_filings,
        COUNT(DISTINCT report_year) as n_years,
        MIN(report_year) as year_min,
        MAX(report_year) as year_max
    FROM finrag_21companies_allbins
    
    UNION ALL
    
    SELECT 
        'TABLE 2: Modern Only (2016-2020)',
        COUNT(*),
        COUNT(DISTINCT cik_int),
        COUNT(DISTINCT docID),
        COUNT(DISTINCT report_year),
        MIN(report_year),
        MAX(report_year)
    FROM finrag_21companies_modern
)
SELECT 
    dataset,
    n_sentences,
    n_companies,
    n_filings,
    n_years,
    year_min || '-' || year_max as year_range,
    ROUND(n_sentences * 1.0 / n_companies, 0) as avg_sentences_per_company
FROM table_comparison;


-- ============================================================================
-- EXPORT TO PARQUET: TABLE 1 (ALL BINS)
-- ============================================================================

SET VARIABLE export_name_1 = 'finrag_21companies_allbins_v2.0.parquet';
SET VARIABLE export_path_1 = (
    getvariable('export_base_path') || '/' || getvariable('export_name_1')
);

SELECT 'Exporting TABLE 1...' as status, getvariable('export_path_1') as destination;

PREPARE export_table1 AS
    COPY finrag_21companies_allbins 
    TO ?
    (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);

EXECUTE export_table1(getvariable('export_path_1'));

SELECT 
    '✓ TABLE 1 EXPORTED' as status,
    getvariable('export_name_1') as filename,
    COUNT(*) as rows_exported
FROM finrag_21companies_allbins;


-- ============================================================================
-- EXPORT TO PARQUET: TABLE 2 (MODERN ERA)
-- ============================================================================

SET VARIABLE export_name_2 = 'finrag_21companies_modern_v2.0.parquet';
SET VARIABLE export_path_2 = (
    getvariable('export_base_path') || '/' || getvariable('export_name_2')
);

SELECT 'Exporting TABLE 2...' as status, getvariable('export_path_2') as destination;

PREPARE export_table2 AS
    COPY finrag_21companies_modern 
    TO ?
    (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);

EXECUTE export_table2(getvariable('export_path_2'));

SELECT 
    '✓ TABLE 2 EXPORTED' as status,
    getvariable('export_name_2') as filename,
    COUNT(*) as rows_exported
FROM finrag_21companies_modern;


-- ============================================================================
-- FINAL SUCCESS SUMMARY
-- ============================================================================

SELECT 
    '═════════════════════════════════════════' as separator,
    '✓✓✓ PRODUCTION TABLES CREATED & EXPORTED' as status;

SELECT 
    'Table Name' as attribute,
    'finrag_21companies_allbins' as table_1,
    'finrag_21companies_modern' as table_2
UNION ALL
SELECT 
    'Filename',
    getvariable('export_name_1'),
    getvariable('export_name_2')
UNION ALL
SELECT 
    'Companies',
    CAST((SELECT COUNT(DISTINCT cik_int) FROM finrag_21companies_allbins) AS VARCHAR),
    CAST((SELECT COUNT(DISTINCT cik_int) FROM finrag_21companies_modern) AS VARCHAR)
UNION ALL
SELECT 
    'Sentences',
    CAST((SELECT COUNT(*) FROM finrag_21companies_allbins) AS VARCHAR),
    CAST((SELECT COUNT(*) FROM finrag_21companies_modern) AS VARCHAR)
UNION ALL
SELECT 
    'Year Range',
    '2006-2020 (15 years)',
    '2016-2020 (5 years)'
UNION ALL
SELECT 
    'Temporal Bins',
    '3 bins (all periods)',
    '1 bin (modern only)'
UNION ALL
SELECT 
    'Use Case',
    'Historical analysis, drift studies',
    'Production RAG, API integration';


-- ============================================================================
-- SCHEMA DOCUMENTATION
-- ============================================================================

SELECT 
    '═══ FINAL SCHEMA SUMMARY ═══' as documentation,
    '24 lean columns selected' as column_count,
    'Dropped: labels, returns, sentenceCount, exchanges, etc.' as columns_dropped,
    'Added: section_name (human-readable)' as columns_added;

SELECT 
    column_name,
    column_type,
    CASE 
        WHEN column_name IN ('cik', 'cik_int', 'name', 'tickers', 'docID', 'sentenceID', 'section', 'section_name', 'form', 'sic') 
        THEN 'Identifiers'
        WHEN column_name = 'sentence' THEN 'Content'
        WHEN column_name IN ('report_year', 'reportDate', 'temporal_bin') THEN 'Temporal'
        WHEN column_name IN ('likely_kpi', 'has_numbers', 'has_comparison') THEN 'RAG Features'
        WHEN column_name IN ('section_priority', 'retrieval_signal_score') THEN 'Scoring'
        WHEN column_name IN ('sample_created_at', 'last_modified_date', 'sample_version', 'source_file_path', 'load_method', 'row_hash') 
        THEN 'Audit'
    END as category
FROM information_schema.columns
WHERE table_name = 'finrag_21companies_modern'
ORDER BY 
    CASE category
        WHEN 'Identifiers' THEN 1
        WHEN 'Content' THEN 2
        WHEN 'Temporal' THEN 3
        WHEN 'RAG Features' THEN 4
        WHEN 'Scoring' THEN 5
        WHEN 'Audit' THEN 6
    END,
    ordinal_position;
























