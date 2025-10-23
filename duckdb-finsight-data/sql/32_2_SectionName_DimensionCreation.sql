-- ============================================================================
-- SECTION DIMENSION TABLE: SEC 10-K Filing Structure
-- Maps numeric section codes to human-readable names and metadata
-- ============================================================================

DROP TABLE IF EXISTS sampler.main.dim_sec_sections;

CREATE TABLE sampler.main.dim_sec_sections AS
SELECT * FROM (VALUES
    -- ═══════════════════════════════════════════════════════════════════
    -- PART I: Business and Risk Information
    -- ═══════════════════════════════════════════════════════════════════
    (0,  'PART_0_HEADER',           'Document Header & Cover',           'P0_METADATA',    0, 'Document metadata, cover page, table of contents'),
    (1,  'PART_I_ITEM_1',            'Item 1: Business',                  'P1_BUSINESS',    1, 'Business description, products, strategy, competition'),
    (1.01, 'PART_I_ITEM_1A',         'Item 1A: Risk Factors',             'P1_RISK',        1, 'Risk factors and uncertainties'),
    (1.02, 'PART_I_ITEM_1B',         'Item 1B: Unresolved Staff Comments','P1_SEC_COMMENTS', 1, 'SEC staff comments (if any)'),
    (2,  'PART_I_ITEM_2',            'Item 2: Properties',                'P1_PROPERTIES',  1, 'Physical properties, facilities, real estate'),
    (3,  'PART_I_ITEM_3',            'Item 3: Legal Proceedings',         'P1_LEGAL',       1, 'Litigation, legal matters'),
    (4,  'PART_I_ITEM_4',            'Item 4: Mine Safety Disclosures',   'P1_MINE_SAFETY', 1, 'Mine safety (if applicable)'),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- PART II: Financial Information and Management Analysis
    -- ═══════════════════════════════════════════════════════════════════
    (5,  'PART_II_ITEM_5',           'Item 5: Market for Stock',          'P2_MARKET',      2, 'Stock market data, dividends, repurchases'),
    (6,  'PART_II_ITEM_6',           'Item 6: Selected Financial Data',   'P2_SELECTED_FIN',2, 'Multi-year financial summary'),
    (7,  'PART_II_ITEM_7',           'Item 7: MD&A',                      'P2_MDNA',        2, 'Management Discussion & Analysis - CRITICAL for financial QA'),
    (7.01, 'PART_II_ITEM_7A',        'Item 7A: Market Risk',              'P2_MARKET_RISK', 2, 'Quantitative/qualitative market risk disclosures'),
    (8,  'PART_II_ITEM_8',           'Item 8: Financial Statements',      'P2_FINANCIALS',  2, 'Audited financial statements - CRITICAL for numbers'),
    (9,  'PART_II_ITEM_9',           'Item 9: Accounting Disagreements',  'P2_ACCT_DISAGREE',2, 'Disagreements with auditors (rare)'),
    (9.01, 'PART_II_ITEM_9A',        'Item 9A: Controls and Procedures',  'P2_CONTROLS',    2, 'Internal controls, SOX 404 disclosures'),
    (9.02, 'PART_II_ITEM_9B',        'Item 9B: Other Information',        'P2_OTHER_INFO',  2, 'Other material information'),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- PART III: Directors, Officers, Corporate Governance
    -- ═══════════════════════════════════════════════════════════════════
    (10, 'PART_III_ITEM_10',         'Item 10: Directors, Officers & Gov','P3_DIRECTORS',   3, 'Board of directors, executive officers'),
    (11, 'PART_III_ITEM_11',         'Item 11: Executive Compensation',   'P3_COMPENSATION',3, 'Executive pay, equity grants'),
    (12, 'PART_III_ITEM_12',         'Item 12: Security Ownership',       'P3_OWNERSHIP',   3, 'Share ownership, equity plans'),
    (13, 'PART_III_ITEM_13',         'Item 13: Related Transactions',     'P3_RELATED_TX',  3, 'Related party transactions'),
    (14, 'PART_III_ITEM_14',         'Item 14: Fees and Services',        'P3_AUDITOR_FEES',3, 'Auditor fees and services'),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- PART IV: Exhibits and Signatures
    -- ═══════════════════════════════════════════════════════════════════
    (15, 'PART_IV_ITEM_15',          'Item 15: Exhibits List',            'P4_EXHIBITS_LIST',4, 'List of exhibits and financial schedules'),
    (16, 'PART_IV_ITEM_16',          'Item 16: Form 10-K Summary',        'P4_SUMMARY',     4, 'Optional summary section'),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- SUPPLEMENTARY SECTIONS (Your dataset shows these exist)
    -- ═══════════════════════════════════════════════════════════════════
    (17, 'SUPPLEMENTARY_17',         'Supplementary Data 17',             'P5_SUPPLEMENTARY',5, 'Additional supplementary information'),
    (18, 'SUPPLEMENTARY_18',         'Supplementary Data 18',             'P5_SUPPLEMENTARY',5, 'Additional supplementary information'),
    (19, 'EXHIBITS_SECTION',         'Exhibits Content',                  'P5_EXHIBITS',    5, 'Actual exhibit documents'),
    (20, 'SIGNATURES_SECTION',       'Signatures',                        'P5_SIGNATURES',  5, 'Officer and director signatures'),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- UNMAPPED (Fallback)
    -- ═══════════════════════════════════════════════════════════════════
    (99, 'UNMAPPED_SECTION',         'Unknown Section',                   'P9_UNKNOWN',     9, 'Section not mapped in dimension table')
    
) AS t(
    section_id,           -- INT: Maps to your numeric 'section' column
    section_code,         -- VARCHAR: Machine-readable code
    section_name,         -- VARCHAR: Human-readable name
    section_category,     -- VARCHAR: Grouped category for filtering
    part_number,          -- INT: Which "Part" of 10-K (1-4)
    section_description   -- VARCHAR: What this section contains
);





-- ============================================================================
-- ADD METADATA COLUMNS
-- ============================================================================

ALTER TABLE sampler.main.dim_sec_sections ADD COLUMN rag_priority VARCHAR;
ALTER TABLE sampler.main.dim_sec_sections ADD COLUMN typical_query_types VARCHAR;
ALTER TABLE sampler.main.dim_sec_sections ADD COLUMN is_financial_core BOOLEAN;

UPDATE sampler.main.dim_sec_sections
SET 
    rag_priority = CASE section_id
        WHEN 1 THEN 'HIGH'        -- Business description
        WHEN 1.01 THEN 'MEDIUM'   -- Risk factors
        WHEN 7 THEN 'CRITICAL'    -- MD&A
        WHEN 8 THEN 'CRITICAL'    -- Financial statements
        WHEN 7.01 THEN 'HIGH'     -- Market risk
        WHEN 10 THEN 'MEDIUM'     -- Directors (governance queries)
        WHEN 11 THEN 'HIGH'       -- Executive comp (common query)
        ELSE 'LOW'
    END,
    
    typical_query_types = CASE section_id
        WHEN 1 THEN 'What does the company do? Products? Strategy?'
        WHEN 1.01 THEN 'What are the risks? What could go wrong?'
        WHEN 7 THEN 'Revenue trends? Why did margins change? Outlook?'
        WHEN 8 THEN 'What was revenue? Net income? Cash flow? Balance sheet?'
        WHEN 7.01 THEN 'Interest rate exposure? FX risk? Commodity risk?'
        WHEN 10 THEN 'Who is the CEO? Board members?'
        WHEN 11 THEN 'How much does the CEO make? Stock options?'
        ELSE 'Specialized queries'
    END,
    
    is_financial_core = CASE 
        WHEN section_id IN (7, 8, 7.01, 6) THEN true
        ELSE false
    END;


    
SELECT * FROM sampler.main.dim_sec_sections;
    
-- ============================================================================
-- VALIDATION & PREVIEW
-- ============================================================================

-- V1: Show all sections with metadata
SELECT 
    section_id,
    section_code,
    section_name,
    section_category,
    rag_priority,
    is_financial_core,
    typical_query_types
FROM sampler.main.dim_sec_sections
WHERE section_id <= 20  -- Your dataset has sections 0-20
ORDER BY section_id;


-- V2: Check which sections are in your sample data
SELECT 
    sf.section,
    ds.section_name,
    ds.section_category,
    ds.rag_priority,
    COUNT(*) as n_sentences,
    COUNT(DISTINCT sf.cik_int) as n_companies,
    ROUND(AVG(CASE WHEN sf.likely_kpi THEN 1.0 ELSE 0.0 END) * 100, 1) as pct_likely_kpi
FROM sample_1m_finrag sf
LEFT JOIN sampler.main.dim_sec_sections ds ON CAST(sf.section AS DOUBLE) = ds.section_id
GROUP BY sf.section, ds.section_name, ds.section_category, ds.rag_priority
ORDER BY sf.section;


-- V3: Identify any unmapped sections in your data
SELECT DISTINCT 
    section,
    COUNT(*) as n_sentences
FROM sample_1m_finrag
WHERE CAST(section AS DOUBLE) NOT IN (SELECT section_id FROM sampler.main.dim_sec_sections)
GROUP BY section
ORDER BY section;


