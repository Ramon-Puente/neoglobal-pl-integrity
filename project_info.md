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