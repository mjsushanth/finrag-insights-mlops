# finrag-insights-mlops

#### Course Project (MLOps IE7374).
#### FINRAG Insights!  

Building an AI-powered financial analysis pipeline for structured KPI extraction and explainable reporting from 10-K filings.

## Project Overview:
1. For background, and Business HLD (High-Level Design) refer to the `design_docs/Finance RAG - HLD Draft v1.1.xlsx and Project Scoping_IE 7374_FinSights.pdf`. They explain the business problem, solution approach, and high-level architecture.
    a. Excel has cloud-cost estimates, cloud tools research, algo-research in neat cells. Perfect doc for intense study and plans; most useful for developers.

2. For initial data engineering, please refer to `duckdb-finsight-data/DuckDB_README.md`. Files like `DuckDB_EDA_LargeData.md and DuckDB_Sampling_Strat.md` explain the EDA analysis and sampling strategies.

3. For more extensive EDA and data study, `results - eda, research/Master_EDA_Notes.pdf` contains valuable information. Various notebook scripts are present here for exploration too.

4. `finrag-insights-mlops/duckdb-finsight-data` has 30+ SQL scripts which are reusable, and purposes explained in the `DuckDB_README.md`. Other duckdb md files are logic-explanations and very useful. 
 


## High level Conceptual Flow:
┌─────────────────────────────────────────────────────────────┐
│ DATA ENGINEERING LAYER (aws_src/)                           │
│ - Extract: S3 bucket management, file I/O                   │
│ - Transform: Merge, dedup, schema validation                │
│ - Load: Export to parquet (NO embeddings here!)             │
└─────────────────────────────────────────────────────────────┘
                          ↓ (Clean parquet files)
┌─────────────────────────────────────────────────────────────┐
│ ML FEATURE ENGINEERING LAYER (embeddings_src/)              │
│ - Chunking strategies: Semantic, fixed-size, sliding window │
│ - Embedding models: sentence-transformers, OpenAI, Cohere   │
│ - Evaluation: Retrieval@K, chunk overlap analysis           │
│ - Experimentation: MLflow tracking, A/B tests               │
└─────────────────────────────────────────────────────────────┘
                          ↓ (Vectors + metadata)
┌─────────────────────────────────────────────────────────────┐
│ SERVING LAYER (rag_api/)                                    │
│ - Vector store: FAISS, Pinecone, S3 Vectors                 │
│ - Retrieval: Hybrid search, reranking                       │
│ - Generation: LLM inference                                 │
└─────────────────────────────────────────────────────────────┘

FINRAG-INSIGHTS-MLOPS/
│

├── duckdb_finsight_data/           # data engineering. entire DuckDB work
├── notebooks/                      # intense research ( EDAs, Polars, Tests, llamacpp..)
└── model/                          # model experiments (potentially)

├── src_aws_etl/                    # data engineering
│   ├── config/
│   ├── etl/ .....
│   └── requirements.txt            # boto3, polars (no cuda ML libs!)
│
├── src_embeddings/                 # ML feature engineering
│   ├── chunking/
│   ├── models/
│   ├── experiments/
│   ├── gpu_config/
│   ├── tests/
│   └── requirements.txt           

├── src_rag_api/                    # Production RAG serving
│   ├── retrieval/
│   ├── generation/
│   └── requirements.txt


### Source Dataset Links:
1. Primary: https://huggingface.co/datasets/khaihernlow/financial-reports-sec
2. Potentially used: EdgarTools https://github.com/dgunning/edgartools
3. Other sources: sec10k company_tickers.json, State Street SPDR ETFs daily holdings for S&P 500 companies.
4. Primary datasets' source citation: https://zenodo.org/records/5589195


