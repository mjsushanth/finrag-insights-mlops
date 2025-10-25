## Project Activity log: finrag-insights-mlops 

#### Course Project (MLOps IE7374).
#### Author and Activity Owner: Joel Markapudi.

### Story 1 Summary:
#### Research multiple financial text corpora (SEC 10-K/10-Q, FinQA, FinanceBench), Document data hierarchy (cik → docID → section → sentence) and defined core business problem: automated extraction + explanation of KPIs. Produced finalized project scoping document & main excel **design_docs\Finance RAG - HLD Draft v1.1.xlsx**.

### Story 1.1 – Dataset and Problem Scoping 
- Researched multiple financial text corpora (SEC 10-K/10-Q, FinQA, FinanceBench, IBM FinQA etc.).
- Selected **Financial-Reports-SEC (Janos Audran)** as the base dataset.
- Understood dataset schema: 200k sentences × 19 features, 0 % nulls, multi-section (0–19) layout.
- Documented data hierarchy → `cik → docID → section → sentence`.
- Defined business problem: *automated extraction + explanation of KPIs from 10-K filings*.
- Presented my findings to the team and got finalized, selected as the project! (yay!)
- Drafted **project scope & flowcharts** (Section 6 of Scoping Doc).
- Finalized dataset card and inclusion in `Project Scoping_IE 7374_FinSights.pdf`.
- Prepared initial architecture outline (Structured RAG + Narrative RAG pipeline).
- Fine Research done on all sections of scope document and created a finalized, revised version of it.
- Handled data, datacard overview, initial EDA, data - availability, original source/api, schema, infrastructure idea for the project, tech stack and algorithms to be used, etc. 

### Story 2.1 – Parquet Compression & Data Preparation
- Loaded HuggingFace dataset locally (`small_full`, `large_full` configs).
- Converted Arrow/IPC streaming format → compressed **Parquet** (`.parquet`) files.
- Achieved ~30× compression; verified schema integrity via `pyarrow.dataset` validation.
- Created reproducible data paths under  
  `data/exports/sec_filings_small_full.parquet` and `...large_full.parquet`.


### Story 3 Summary:
#### Advanced Analytical Tasks: EDA across 200k sentences + EDA across 71.8M sentences. Analysis using Polars/DuckDB, Quantifying temporal coverage, Implemented TF-IDF n-gram analysis for section-specific vocabularies, numeric density (currency, %, YoY patterns), duplication estimates (SimHash) on sentences, produced **Master EDA Notes.pdf**.


### Story 3.1 – Core Data Profiling
- Verified all 19 columns, data types, uniqueness, null % = 0.
- Mapped dataset hierarchy (company → filing → section → sentence).
- Quantified token & character lengths; identified table-like outliers.

### Story 3.2 – Advanced Analytical Tasks
- Implemented **top n-grams by section** (TF-IDF) → produced signature vocabularies.
- Ran **KPI signal scan** (currency, %, EPS, units, YoY, growth patterns) → found MD&A and Notes richest.
- Estimated **approx. duplication rates** via SimHash → highlighted need for dedupe.
- Auto-generated **section label suggestions** using keyword matching + n-gram support.
- Validated manual labels for top sections (0 Business, 1 Risks, 7 Selected Data, 8 MD&A, 10 Notes).
- Produced multiple CSV artifacts:  
  `top_ngrams_by_section.csv`, `kpi_signal_scan_by_section.csv`,  
  `duplication_by_section.csv`, `section_label_suggestions.csv`.

### Story 3.3 – Interpretation & Documentation
- Created **Master EDA Notes.pdf** combining numerical tables, interpretive paragraphs, and actionable insights.
- Defined **section-wise KPI priorities**, **chunk size rules**, and **retrieval routing logic**. (Potential to change.)
- Cross-verified findings with auxiliary analysis (Deep Analysis Addendum).
- Finalized **EDA Synthesis → Algorithmic Design Shift** memo for model dev planning.
- EDA driven interpretation:  token distribution statistics (mean: 26 tokens, p95: 55 tokens), section-wise KPI priorities, and chunk size recommendations (3-5 sentences ≈ 75-130 tokens) 


### Story 4 Summary:
#### 28-35+ SQL scripts (increasing), 40+ validation queries, macro-enscapulated one-shot procs or cleanly parameterized, documented SQL, subsecond-logging tables, analysis queries, multiple production datasets and dimensions according to evolving need. Basically all the DB work. 

### Story 4.1 – Advanced Data Engineering: Multi-Source Integration & Stratified Sampling
- **Integrated 3 heterogeneous sources** via fuzzy matching: S&P 500 ETF holdings (Excel), SEC CIK mappings (JSON), 71.8M-sentence corpus (Parquet)
- Implemented **two-tier company curation pipeline** with weighted multi-objective scoring (`quality_score`) balancing temporal consistency, section coverage, content volume, and recency relevance
- Achieved **700 → 540 → 75 → 21 company refinement** through iterative analysis, prioritizing disclosure quality over raw market cap. Its now dynamic with a **macro-encapsulated N-company selection** strategy code.
- Production SQL scripts organized by taxonomy (00_setup, 20_eda, 21_curation, 31_sampling, 90_qc)
- Python code for manual data fetch, parse, clean and insert into historical fact tables. (For Google.)

### Story 4.2 – Production Stratified Sampling Pipeline
- Designed **two-stage sampling architecture**: lightweight sentenceID sampling (1.7s) + schema join-back (36.8s) for **1M-sentence extraction** from 71.8M corpus
- Implemented **temporal stratification** with product-focused weighting (15/20/65 across 2006-2009/2010-2015/2016-2020 bins) capturing 100% of modern era (654k sentences)
- Built **section-preserving stratification** (`Company × Year × Section` grain) eliminating random sampling bias while maintaining all 20 SEC sections
- Created **execution logging framework** with sub-second timing precision tracking 11 pipeline steps for performance analysis
- Engineered **12 derived RAG features** (`likely_kpi`, `has_numbers`, `has_comparison`) using word-boundary-protected regex, achieving retrieval signal scoring
- Upon iterative needs and scale refinement, the 75-company 1M-sample is further downsampled to a **21-company subset**. 

### Story 4.3 – Schema Engineering & Dimension Modeling
- Designed **lean production schema** (45 → 24 columns) eliminating market data noise (`labels`, `returns`) while preserving semantic value
- Created **SEC section dimension table** (`dim_sec_sections`) mapping numeric codes to human-readable names with RAG priority metadata
- Built **dynamic company selection macro** enabling parameterized table creation (N companies from S&P + M from quality score)
- Implemented **ETL audit column pattern** (`sample_created_at`, `row_hash`, `load_method`) supporting incremental loads and deduplication
- Created **one-shot executable pipelines**: 47-second end-to-end sampling etc. for main-run files.
- Detailed `.md` files for documentation of database-based stratification, schema design work present in duckdb-finsight-data/ folder.

### Story 4.4 – Quality Control & Validation queries.
- Developed **schema comparison utilities** for cross-dataset validation (API vs historical data compatibility checks)
- Performed **temporal density analysis** across 28 years (1993-2020), demonstrating data quality evolution and modern era concentration


### Story 5.1 – LLM-Based KPI Extraction: Architecture Evaluation & Iteration
#### Summary: These attempts proved 'concept, code success' but incredible inefficient due to usage of CUDA-pytorch on 16GB RTX Nvidia GPU. (too many hours for generation.) Many other llama-cpp attempts took days and some were broken code layers. 

#### Ollama-Based Prototyping & Initial Transformer Experiments
- Deployed **Ollama inference runtime** for local LLM experimentation; tested Qwen2.5-3B on 7 SEC 10-K sample sentences
- Discovered **sequential processing bottleneck**: 14B model required 15+ minutes for 100 sentences, projecting 100+ days for 1M-sentence corpus
- Evaluated **RoBERTa and domain-specific financial transformers** for NER-based extraction; abandoned due to poor handling of compositional phrases ("adjusted operating income before special items"). 
- Identified fundamental limitation: **regex captures structure, not semantics**; LLMs required for context-dependent interpretation and negation handling

#### llama.cpp Server and Python bindings Architecture & Optimization Attempts
- Migrated to **llama.cpp server** (b6814 CUDA build) for batch processing; debugged Windows-specific flag syntax (`-c` vs `--n-ctx`, `-ngl` vs `--n-gpu-layers`). 
- Had intense difficulty with routes under /v1/* pattern versus native versions (b6814 build reported invalid argument: --api, etc.). Online examples discrepancies issues, Parameter Translation Issues - led to choosing python bindings.
- Implemented **parallel inference slots** (12 concurrent) with continuous batching on sent-transformers; verified GPU offload on RTX 3080 Ti (16GB VRAM) - didnt scale unfortunately. llama cpp python - proved more reliable.
- Tested context increases, (4096 → 8192 tokens) and quantization model switches (Q4_K_M vs Q5_K_M); Q5 was better work.
- Tried **multi-sentence packing strategy**: 12-sentence batches with delimiter-based prompt structure to amortize inference overhead
- Got many **inference degradations, context-rot, model-state based failure**: model outputted empty JSON arrays due to grammar constraint conflicts with complex packed prompts. Its a quantized LLM pollution problem.
- Conducted **cost-performance analysis** across local (7B Q5) vs cloud APIs (GPT-4o-mini, GPT-5-mini, Claude Haiku/Sonnet). GPT-4o-mini at $0.15/$0.60 per 1M tokens = $0.47 for 10k sentences, beating local inference.
- Implemented **OpenAI API integration** with Fernet symmetric encryption for key management.
- Explored designs on regex pre-filter → LLM extraction (but with code failure and lack of time).
- Explored **LLM-guided regex pattern mining**: 10-pass analysis using local Qwen2.5-7B. Did not complete.



















### Author and Activity Owner: Joel Markapudi.
