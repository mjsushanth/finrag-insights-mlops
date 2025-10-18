# DuckDB Usage for finrag-insights-mlops

#### Course Project (MLOps IE7374)
#### Author: Joel Markapudi.

## Introduction
We plan to make use of DuckDB for data sampling and extended data exploration (Most of the data exploration is with Polars.)
Goal: DBeaver + DuckDB (file-backed DB).


### Features: Production-grade stratified sampling pipeline with DuckDB SQL.
- Integrated three heterogeneous sources: S&P 500 ETF holdings (Excel), SEC CIK mappings (JSON), and 71.8M-sentence corpus (Parquet).
- Implemented two-tier fuzzy matching, numerous ad-hoc data cleaning scripts, join analysis scripts, on-spot data validation queries.
- Achieved corpus coverage (539 matched from 700 targets) with documented missing companies justifications.

- Designed product-focused temporal weighting (15/20/65 split across 2006-2009, 2010-2015, 2016-2020) based on user query patterns.
- Eliminated randomization risks: Guarantees complete section coverage (all 20 sections) for 2016-2020 period.

- Two-stage execution pattern: sampling (sentenceIDs only, 1.7s) followed by complete schema retrieval (36.8s join-back).
- Engineered 12 derived flags with word-boundary-protected regex patterns to minimize false positives. 

- Created reproducible, strong data preparation workflow using DuckDB SQL scripts.
- Completely parameterized execution, Session Variable Architecture, Environment-agnostic design present for `run_stratisfied.sql.`
- Clean separation: configuration (variables) → intermediate state (params table) → execution logic (sampling/join/export).
- Organized 15+ SQL scripts using numbered taxonomy (00_setup, 20_eda, 21_curation, 31_sampling, 90_qc) documenting iterative problem-solving. 

- Live system support: ETL-pattern audit columns for incremental load. (`last_modified_date, sample_version, source_file_path, load_method, etc.`)
- **Execution logging framework** tracks pipeline steps with row counts, messages, and sub-second timing precision for troubleshooting and performance analysis.
- Export to compressed Parquet (ZSTD) produces ~75MB artifact.

- Documented dataset coverage limitations (89 missing S&P 500 companies including Alphabet/Google) with root cause analysis. (Huggingface dataset has 71M rows but yet misses numerous companies.)


### Production-Ready Proof:

Step                  | Duration    | % of Total | Assessment
----------------------|-------------|------------|------------------
Initialization        | 0.01s       | <1%        | ✓ Instant
Parameters            | 0.01s       | <1%        | ✓ Instant
Corpus Load           | 3.67s       | 7.8%       | ✓ Fast (1.95M rows)
Bin Populations       | 0.27s       | 0.6%       | ✓ Fast
Pivot                 | 0.004s      | <1%        | ✓ Instant
Allocation Strategy   | 0.003s      | <1%        | ✓ Instant
Bin Allocations       | 0.005s      | <1%        | ✓ Instant
Sampling Execution    | 1.70s       | 3.6%       | ✓✓ EXCELLENT
Schema Retrieval      | 36.83s      | 78.3%      | ○ Expected (big join)
Schema Validation     | 0.10s       | 0.2%       | ✓ Fast
Feature Engineering   | 1.18s       | 2.5%       | ✓ Fast
Export to Parquet     | 3.07s       | 6.5%       | ✓ Fast
----------------------|-------------|------------|------------------
TOTAL                 | ~47s        | 100%       | ✓✓ Production-Ready

- 47s is excellent for a large 71M -> 1M workload with complex sampling logic.


### Limitations & Scope Boundaries

1. Connection to MotherDuck (serverless DuckDB), Dagster pipeline, Dbt - Airflow for database orchestration.
    - 1M dataset sample acts as a one-time data prep step for ML model training.
    - MotherDuck pricing, hosting raw dataset (per GB-hour for compute or storage cost) cant be justified for a 1-time sampling task.
    - DuckDB's file-based architecture enables full local development with production-grade performance.
2. Sample `finrag_sampling.py` is written to show Proof of Concept: AWS SageMaker Notebook, dag, SQL-wrapper for load and execute SQL concept ( `conn.execute(sql_script)` ). This is just a dummified workflow script.




### Project layout - Preliminary
    duckdb/
    00_pragmas.sql           -- threads, memory, extensions
    01_macros_sampling.sql   -- macros = "stored procedures"
    10_run_uniform.sql       -- calls macros with parameters
    ..
    ..
    11_run_stratified.sql
    12_run_hash_deterministic.sql
    30_eda ..
    31_run_stratified ..
    ..
    ..
    qc_checks.sql            -- quick validations
    sampler.duckdb             -- the DB file DBeaver connects to

1. 01_macros_sampling.sql (define reusable “procs”) - Concepts such as Uniform (block) sampling to Parquet, Stratified sampling by (company, year). 
2. Other ideas for sampling: Deterministic hash-based sampling.

### Grouping Idea For DuckDB Scripts:
1. A_bootstrap/, B_params/, C_macros/, D_jobs/, E_qc/.
2 Or, Numbering Scheme: 00_, 01_, 10_, 11_, 12_, qc_.sql, etc.
    00–09 · Session bootstrap
    10–19 · Reference / DDL 
    20–29 · EDA / inspection
    30–39 · Actions / jobs
    ...
    90–99 · QA / QC / audits


### Disclaimer on Unmatched Tickers:
- After extensive EDA query and excel-based double-checking, manual reviews, 89 companies out of the master list (for the historical fact table) have no name, ticker or any match in the 4675 distinct CIK-Company dimension, which comes from large data - 71 Million rows. 
- We attribute this lack of match to the huggingface dataset's limitations.
- Some of those companies include: Broadcom Inc., Alphabet Inc., BERKSHIRE HATHAWAY INC, JPMORGAN CHASE & CO, BANK OF AMERICA CORP /DE/, WELLS FARGO & COMPANY/MN, GOLDMAN SACHS GROUP INC, MORGAN STANLEY, AT&T INC.
- Details are in excel `..\finrag-insights-mlops\duckdb-finsight-data\ManualMatches - 700SP vs 4700Full.xlsx` 




### Budget Hours consideration for automated SQL-testing suite and learning:
- Decided to prioritize core sampling tasks and manual QC checks over building a full automated SQL-testing suite.
- Almost few thousand lines+ of SQL code and multiple-chained CTEs, complex weighted scoring, etc. - is for purely historical data sampling, so it made write only validation queries on the fly.
- SQL scripts run ONCE to create static dataset - so decision was to not dockerize one-time, non-operational scripts.
- Found it extremely challenging to code DuckDB SQL and polars, manage multiple scripting, research tasks at once (as an early solo developer).
