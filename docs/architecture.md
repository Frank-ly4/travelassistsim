# TravelAssist Architecture (Step 1)

## 1. Target Cloud Architecture (end state)

Synthetic Data / Source Files
    -> ADLS Gen2 Bronze (raw)
    -> Azure Data Factory (ingest + transform)
    -> ADLS Gen2 Silver (cleaned)
    -> Azure SQL (star schema / reporting marts)
    -> Power BI (semantic model + dashboards)

Optional AI Enrichment:
Case Notes / Documents
    -> Azure AI Language / Document Intelligence
    -> Enriched outputs (SQL / ADLS Gold)
    -> Power BI AI analytics page

## 2. Local Development Architecture (current phase)

config/      -> YAML settings and KPI definitions
docs/        -> scope and architecture decisions
src/         -> Python package for generator and utilities
data/raw     -> synthetic raw exports (local bronze equivalent)
data/clean   -> cleaned/standardized data (local silver equivalent)
data/curated -> reporting-ready extracts (local gold equivalent)

## 3. Why this architecture supports Data Analytics learning
- Teaches data lineage (source -> transformed -> report)
- Supports SQL and Power BI modeling
- Mirrors real Azure analytics projects
- Enables gradual expansion to AI and cloud pipelines

## 4. Build Order
1. Scope + KPIs + scaffold (current)
2. Synthetic data generator
3. SQL schema + queries
4. Power BI MVP
5. ADLS + ADF + Azure SQL
6. AI enrichment
7. Portfolio packaging