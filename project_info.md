# Project: NeoGlobal P&L Integrity Engine
**Role:** Senior Technical FP&A Architect
**Objective:** Build a $0-cost, self-healing reconciliation pipeline between Stripe Billing and NetSuite ERP.

## Technical Constraints ($0 Stack)
- **Database:** DuckDB (OLAP engine)
- **Precision:** All currency must use DECIMAL(19, 4) to prevent rounding errors.
- **Mapping:** Stripe `ch_xxx` and `in_xxx` IDs map to NetSuite `externalId`.
- **Tools:** Python, dbt-core, Docker, GitHub Actions, Streamlit.

## Phase 3 Strategy: Synthetic Data Generation
- **Method:** CTGAN for tabular financial distributions; Local LLM (Ollama) for messy JSON logs.
- **Anomalies:** Injected "leakage" (missing rows), timing drifts (24h lag), and metadata mismatches.

## More about the project:
### **Phase 1: Research & Data Mapping (The "Audit" Mindset)**

Before writing a single line of code, you must define what "correct" looks like.

- **Step 1.1: Standardize the Chart of Accounts (COA):** Research standard SaaS/Logistics COA (e.g., Account 4000 for Revenue, 1200 for A/R) to ensure the ERP data reflects GAAP standards.
- **Step 1.2: Define Precision Requirements:** Identify the correct SQL datatypes for financial values (e.g., using `DECIMAL(18, 2)` instead of `FLOAT` to avoid rounding errors in reconciliation).
- **Step 1.3: Map Logic Bridges:** Confirm the "Join Key" (e.g., Stripe `metadata.external_id` mapping to NetSuite `external_ref_id`) to ensure System A can actually talk to System B.

### **Phase 2: Local Infrastructure Setup ($0 Cost)**

Build the "factory" where the data will be processed.

- **Step 2.1: Initialize Repository:** Implement the proposed folder structure in GitHub.
- **Step 2.2: Dockerize the Stack:** Write a `docker-compose.yml` that pulls images for **DuckDB** (the warehouse) and **dbt** (the engine).
- **Step 2.3: Environment Variables:** Setup a `.env` file to handle local paths and "mock" API keys, showing you understand data security.

### **Phase 3: Synthetic Data Generation (The "Messy" Reality)**

Generate data that looks like a real company’s nightmare.

- **Step 3.1: Seed Data Creation:** Use **Faker** to generate 100,000+ rows of "clean" logs.
- **Step 3.2: Inject Anomalies:** Programmatically delete 2% of ERP records and change the amounts on 1% of Billing logs to simulate "Human Error" or "API Failure".
- **Step 3.3: Schema Verification:** Run a Python script to verify that the generated CSV/Parquet files match the intended datatypes exactly before ingestion.

### **Phase 4: Transformation & Reconciliation Logic (The "Moat")**

This is where you prove you are a Senior Technical FP&A Analyst.

- **Step 4.1: Source Ingestion:** Use **dbt** to load raw files into "Staging" tables.
- **Step 4.2: Build the "Integrity Layer":** Write SQL to join the two systems and calculate `variance = billing_amount - ledger_amount`.
- **Step 4.3: Implement Data Tests:** Use dbt's built-in tests (`not_null`, `unique`, `relationships`) to ensure no transaction is lost in the pipeline.

### **Phase 5: Visualization & CI/CD**

Move from "Code" to "Business Insights".

- **Step 5.1: Streamlit Dashboard:** Build a simple Python UI that displays "Total Leakage ($)" and a list of "Unreconciled Transactions".
- **Step 5.2: GitHub Action Orchestration:** Create a `.yml` file that automatically runs your dbt tests whenever you push code, proving "Continuous Integration".
- **Step 5.3: Documentation:** Auto-generate a dbt documentation site to show a visual map of the data lineage.

### **Phase 6: The "Robert Half" Polish**

- **Step 6.1: Case Study Write-up:** Document the "Problem," "Technical Solution," and "Business Impact" (e.g., "Identified $50k in missing revenue").
- **Step 6.2: Final Audit:** Walk through the code one last time to ensure all variables are named professionally and no "junk" files are in the repo.

## Infrastructure Update: M4 Native Hybrid Stack
**Last Updated:** Jan 13, 2026
**Hardware:** M4 Mac Mini (Apple Silicon)
**Host Engine:** OrbStack (Native Swift / Light-resource Docker replacement)

### Storage Architecture (Performance-First)
- **Codebase/Index:** Internal Macintosh HD (Root) for low-latency VS Code indexing and Git operations.
- **Data Root (OrbStack):** Offloaded to `/Volumes/Samsung SSD/.docker_infra/data_root` (APFS) to preserve internal drive health.
- **Persistent Volumes:** Heavy datasets (1.5M+ rows) and DuckDB binary files are bind-mounted to `/Volumes/Samsung SSD/persistent_volumes/neoglobal_integrity/`.
### Mapping Logic
- All `data/` and `warehouse/` directories inside the `analytics_engine` container must bind-mount to the persistent_volumes path above to ensure $0 data persistence and internal drive longevity.

### Deployment Standards
- **Containerization:** All Python dependencies (CTGAN, dbt-duckdb, Ollama) are isolated in the `analytics_engine` container.
- **Network:** Zero-cost local-first networking; no cloud egress/ingress costs for the development phase.

### Implementation Update: Data Isolation & Synthetic Data Pipeline (Phase 3 Prep)
To address the critical requirement of data isolation and prepare for synthetic data generation, the following architectural and code changes were implemented:

- **Hybrid Storage Strategy Enabled:** Configured `docker-compose.yml` to leverage OrbStack's efficiency on M4 Mac Mini, mapping external Samsung SSD paths directly to neutral container directories (`/mnt/ssd_raw`, `/mnt/ssd_warehouse`). This ensures zero analytical data footprint on the internal Macintosh HD.
- **Dynamic Path Handling:** Introduced environment variables `DATA_PATH_RAW` and `DATA_PATH_WAREHOUSE` within `docker-compose.yml` to standardize data access paths for containerized applications.
- **dbt Integration Update:** Modified `dbt_project/profiles.yml` to point the DuckDB database path to the new `/mnt/ssd_warehouse/neoglobal.db` location, ensuring dbt operates exclusively on the external SSD volume.
- **`generate_seed_data.py` Developed:** Created a Python script (`data_gen/generate_seed_data.py`) to generate 100 'perfect' Stripe and NetSuite transaction matches, saving these CSVs to `/mnt/ssd_raw` on the external SSD, adhering to DECIMAL(19, 4) precision and specified lag logic.
- **`train_model.py` Developed:** Created a Python script (`data_gen/train_model.py`) utilizing SDV (Synthetic Data Vault) to train generative AI models (Stripe and NetSuite) from the seed data. These models are saved as `.pkl` files to `/mnt/ssd_raw/models` on the external SSD.
- **Verification:** Successfully verified that all generated seed data and trained models are stored exclusively on the Samsung SSD, confirming complete data isolation from the internal project repository.

### Implementation Update: Phase 5 & 6 Completion (High-Density Synthesis & dbt Transformation)
**Last Updated:** Jan 13, 2026

To validate the reconciliation engine against a large, realistic dataset, the project has completed the high-density data synthesis and the core dbt transformation logic.

- **Batched Data Synthesis:** A new script, `data_gen/synthesize_bulk_batched.py`, was developed to generate 1.5 million Stripe and 1.5 million NetSuite records. The process was batched (150,000 rows per batch) to manage memory and ensure resilience. The script now explicitly enforces that `netsuite.credit_usd` matches `stripe.amount` for all non-anomalous data, correcting an issue where the `sdv` models generated statistically similar but not identical values.
- **Anomaly Injection Verified:** 1,500 anomalies (500 missing NetSuite records, 1,000 `+/- $0.01` amount mismatches) were programmatically injected into the NetSuite dataset.
- **dbt Transformation Layer:** Staging models (`stg_stripe`, `stg_netsuite`) were created to ingest the 3 million total rows from the partitioned Parquet files on the external SSD.
- **Reconciliation Model:** A `fct_reconciliation` model was built to perform a full outer join between the two systems. It successfully flags records that are `is_missing_in_erp`, have an `is_amount_mismatch`, or are a `is_perfect_match`.
- **End-to-End Verification:** After a full run of the new data pipeline and dbt transformation, a verification query confirmed that the `fct_reconciliation` model correctly identified **all 1,500 anomalies**, proving the core logic of the P&L Integrity Engine.