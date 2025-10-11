## Project Activity log: finrag-insights-mlops 

Course Project (MLOps IE7374).

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



### Author: Joel Markapudi.
