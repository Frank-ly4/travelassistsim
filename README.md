# TravelAssist
Travel Assistance Case Management Analytics Platform (Azure + SQL + Power BI + AI)

## Project Goal
Build an end-to-end analytics portfolio project for a simulated travel assistance / case management operation.

This project will demonstrate:
- SQL data modeling and KPI analysis
- Power BI reporting (PL-300 / DA-100 skillset)
- Azure Data Lake + ADF pipeline concepts (DP-900 / DP-203)
- AI enrichment with Azure AI Services (AI-900 / AI-102)

## Current Phase
**Phase 0 / Step 1**
- Project scope
- KPI definitions
- Architecture
- Local project scaffold

## Planned Phases
1. Scope + KPI definitions ✅
2. Synthetic data generation
3. SQL schema + KPI queries
4. Power BI MVP
5. Azure (ADLS + ADF + Azure SQL)
6. AI enrichment (Language / Document Intelligence)
7. Portfolio packaging

## Run (local)
```bash
python scripts/run_cli.py show-config
python scripts/run_cli.py show-kpis


---

## `config/settings.yaml`
This is the core project config.

```yaml
project:
  name: TravelAssist
  environment: local
  timezone: America/Chicago
  owner: Franklin
  version: 0.1.0

paths:
  raw_root: data/raw
  clean_root: data/clean
  curated_root: data/curated
  logs_root: output/logs
  reports_root: output/reports

data_generation:
  random_seed: 42
  row_targets:
    cases: 25000
    status_history: 120000
    contacts: 80000
    documents: 35000
    notes: 50000

business_rules:
  sla_minutes:
    critical_first_response: 15
    high_first_response: 60
    medium_first_response: 240
    low_first_response: 720
  reopen_rate_pct: 4.0
  escalation_base_rate_pct: 12.0

azure:
  enabled: false
  adls:
    storage_account: ""
    container: ""
  sql:
    server: ""
    database: ""
  data_factory:
    name: ""

ai_services:
  enabled: false
  language:
    endpoint: ""
  document_intelligence:
    endpoint: ""