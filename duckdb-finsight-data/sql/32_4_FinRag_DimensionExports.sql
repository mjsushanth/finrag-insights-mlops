
-- ============================================================================
-- EXPORT DIMENSION TABLES TO PARQUET
-- Naming Convention: finrag_dim_{table_purpose}_v{version}.parquet 
-- if needed, add version. else, auto replace files.
-- ============================================================================

SET VARIABLE export_base_path = 'D:/JoelDesktop folds_24/NEU FALL2025/MLops IE7374 Project/finrag-insights-mlops/data/exports';


-- ══════════════════════════════════════════════════════════════════════════
-- EXPORT 1: Section Dimension (SEC 10-K Section Mapping)
-- ══════════════════════════════════════════════════════════════════════════

SET VARIABLE dim_sections_export = (
    getvariable('export_base_path') || '/finrag_dim_sec_sections.parquet'
);

SELECT 'Exporting section dimension...' as status, getvariable('dim_sections_export') as destination;

PREPARE export_dim_sections AS
    COPY sampler.main.dim_sec_sections 
    TO ?
    (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- DuckDB automatically overwrites if file exists (no manual delete needed)
EXECUTE export_dim_sections(getvariable('dim_sections_export'));

SELECT 
    '✓ DIMENSION 1 EXPORTED' as status,
    'finrag_dim_sec_sections.parquet' as filename,
    COUNT(*) as rows_exported
FROM sampler.main.dim_sec_sections;


-- ══════════════════════════════════════════════════════════════════════════
-- EXPORT 2: Company Dimension (21 Curated Companies)
-- ══════════════════════════════════════════════════════════════════════════

SET VARIABLE dim_companies_export = (
    getvariable('export_base_path') || '/finrag_dim_companies_21.parquet'
);

SELECT 'Exporting company dimension...' as status, getvariable('dim_companies_export') as destination;

PREPARE export_dim_companies AS
    COPY sampler.main.finrag_tgt_comps_21 
    TO ?
    (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- DuckDB automatically overwrites if file exists (no manual delete needed)
EXECUTE export_dim_companies(getvariable('dim_companies_export'));

SELECT 
    '✓ DIMENSION 2 EXPORTED' as status,
    'finrag_dim_companies_21.parquet' as filename,
    COUNT(*) as rows_exported
FROM sampler.main.finrag_tgt_comps_21;


-- ══════════════════════════════════════════════════════════════════════════
-- EXPORT SUMMARY
-- ══════════════════════════════════════════════════════════════════════════

SELECT 
    '═════════════════════════════════════════' as separator,
    '✓✓ DIMENSION TABLES EXPORTED' as status;

SELECT 
    'Dimension Table' as table_type,
    'Filename' as export_name,
    'Purpose' as description
UNION ALL
SELECT 
    'Section Mapping',
    'finrag_dim_sec_sections.parquet',
    'Maps section IDs (0-20) to human-readable names'
UNION ALL
SELECT 
    'Company Lookup',
    'finrag_dim_companies_21.parquet',
    'Contains 21 curated companies with CIK, ticker, selection source';


-- ══════════════════════════════════════════════════════════════════════════
-- VALIDATION: Files Ready for Python Import
-- ══════════════════════════════════════════════════════════════════════════

SELECT 
    '═══ PYTHON IMPORT PATHS ═══' as info,
    'Use these paths in your standardization script:' as instruction;

SELECT 
    'dim_sections' as dimension,
    getvariable('export_base_path') || '/finrag_dim_sec_sections.parquet' as python_path
UNION ALL
SELECT 
    'dim_companies',
    getvariable('export_base_path') || '/finrag_dim_companies_21.parquet';



