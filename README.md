# Commodities Ontology

## Demo

[![Commodities Ontology Demo](https://img.youtube.com/vi/u-DbKSHB2wQ/0.jpg)](https://www.youtube.com/watch?v=u-DbKSHB2wQ)

> Watch the full walkthrough on YouTube: [https://www.youtube.com/watch?v=u-DbKSHB2wQ](https://www.youtube.com/watch?v=u-DbKSHB2wQ)

![Databricks Genie space for Commodities Ontology](Databricks%20Genie%20space%20for%20Commodities%20Ontology.png)
![Medallion architecture overview](Medallion%20architecture%20overview.png)


## Context
Faster signal extraction context, linking and decision framing, which helps with decision intelligence for Commodities, which provides better hedging decisions.

### The Problem
Traders don’t lack data. They already consume from a firehouse ranging from Bloomberg, Reuters, Vitol, etc. The real problem is signal extraction. Important market signals are buried in a firehose of information.

### What Traders Struggle With
Signals are fragmented across many sources. Traders must manually connect the dots. This happens under extreme time pressure. Result → slower decisions, missed context.

### What We Do
We provide Decision Intelligence for Commodities. Platform continuously scans market signals: infrastructure disruptions, policy changes, supply chain movements, geopolitical events.

### Our Core Advantage
We provide context linking. Identify relationships. Turn fragmented data into clear market narratives.

### What Traders Get
Better decision framing. Aggregated signals in one place. Faster trading and hedging decisions for spot markets and future markets. Bloomberg Terminal evolving into an AI copilot for energy markets.

“Let me show you how this works.”

The value is not in “reading news.” Traders already have that. The value is in faster signal extraction, context linking, and decision framing -> Decision intelligence for commodities -> better hedging decisions.

Traders already have a firehose of information: Bloomberg, Reuters, Vitol, OPEC, etc. The problem is not access to data. The problem is signal extraction.

“What actually matters for price in the next 1 hour, 1 day, or 1 month?”

#### What an AI agent could realistically do
- Detect market-moving events instantly
        - Event detected: refinery outage
        - Crude demand impact: -350k barrels/day
        - Expected impact:
                - WTI crude ↓
                - gasoline crack spread ↑
        - Confidence: medium
- Connect unrelated signals from the market
- Deliver decision framing
        - Market signal summary:
                - Bullish signals:
                        + OPEC production cut rumors
                        + US inventories falling
                - Bearish signals:
                        - China demand slowing
                        - refinery outages
                - Net sentiment: moderately bullish
                - Suggested strategies:
                        - hold long WTI
                        - buy short-term call options

#### Executive-level conclusion
A news-reading AI alone creates moderate value. A system that does: event detection, supply/demand modeling, signal aggregation, trade scenario simulation creates very high value.
Think of it as: Bloomberg Terminal → AI Co-Pilot for Energy Markets



This repository builds on top of the Petrinex data from [Calgary_Hackathon_Data_March_26](https://github.com/vragovvolo/Calgary_Hackathon_Data_March_26.git) by creating a lite version of an ontology framework to provide decision intelligence for commodities traders.

**Source Data:**
- Original Petrinex data and hackathon pipeline: [Calgary_Hackathon_Data_March_26](https://github.com/vragovvolo/Calgary_Hackathon_Data_March_26.git)
- This repo adds the Commodities Market Data Pipeline for enhanced signal extraction and context linking.


**Zero synthetic data.** Everything is sourced from [Petrinex Public Data](https://www.petrinex.ca/PD/Pages/default.aspx) -- Alberta's official Petroleum Registry maintained by the Alberta Energy Regulator (AER).

---

> **Tags:** `commodities` `ontology` `decision-intelligence` `databricks` `market-data` `scalable-architecture` `energy-markets` `trading` `AI` `signal-extraction`

---

> **Scalable Architecture:**
> This project is designed with a scalable, modular architecture. The Medallion architecture enables easy expansion, integration, and adaptation to new data sources, analytics, and AI-driven workflows. The pipeline supports large volumes of market data and can be extended for additional commodities, geographies, and trading strategies.

---



## Quick Start


1. Import this repo into your Databricks workspace
2. Open `notebooks/01_setup_data`
3. Set your catalog and schema name in the widgets (defaults: `hackathon` / `energy_data`)
4. Attach to **serverless** compute or any cluster
5. **Run All** -- takes ~5 minutes (reads from local files, no API downloads)

That's it. All 8 tables + PDFs will be created in your Unity Catalog.

---

## Commodities Market Data Pipeline

Once the Petrinex data is loaded via the notebook, you can use the Commodities Market Data Pipeline for advanced analytics and decision intelligence.

### Prerequisites

Before running the pipeline, ensure you have:

| Requirement | Details |
|---|---|
| Databricks workspace | With Unity Catalog enabled |
| Catalog & schema | `hackathon` catalog, `market_agents` schema (or update the table references in each script) |
| Databricks AI Functions | `ai_extract` and `ai_query` must be available in your region |
| Databricks Pipelines (DLT) | Required to execute the `@dp.materialized_view` decorated scripts |
| Serverless compute | Recommended for fastest setup; any DLT-compatible cluster works |

### Installation

1. **Import this repository** into your Databricks workspace via Git integration:
   - Go to **Workspace → Repos → Add Repo**
   - Paste the repo URL and click **Create**

2. **Create the Unity Catalog schema** (if it does not already exist):
   ```sql
   CREATE CATALOG IF NOT EXISTS hackathon;
   CREATE SCHEMA IF NOT EXISTS hackathon.market_agents;
   ```

3. **Create a Databricks Pipeline** for each transformation layer:
   - Go to **Workflows → Delta Live Tables → Create Pipeline**
   - Set the source to the script path (e.g., `Commodities Market Data Pipeline/transformations/bronze/bronze_market_data.py`)
   - Set the target schema to `hackathon.market_agents`
   - Use **Serverless** compute
   - Repeat for silver and gold scripts

4. **Run the pipelines in order:**
   ```
   bronze_market_data.py   →   silver_market_data.py   →   gold_market_summary.py
   ```

### How It Works

| Layer | Script | Output Table | Description |
|---|---|---|---|
| **Bronze** | `bronze/bronze_market_data.py` | `hackathon.market_agents.bronze_market_data` | Ingests raw market news articles (text, URL, published date) via MCP search queries spanning Jan 2025 – Mar 2026 |
| **Silver** | `silver/silver_market_data.py` | `hackathon.market_agents.silver_market_data` | Uses `ai_extract` to parse commodity, price, sentiment, and headline from raw text; filters to oil & gas articles; adds a per-article trading recommendation via `ai_query` |
| **Gold** | `gold/gold_market_summary.py` | `hackathon.market_agents.gold_market_summary` | Aggregates silver data by commodity and date; computes sentiment counts and overall trading recommendation using `databricks-meta-llama-3-3-70b-instruct` |

### Output CSVs

Pre-generated output files are included in `Commodities Market Data Pipeline/data/` for reference:
- `bronze_market_data.csv` — Raw ingested articles
- `silver_market_data.csv` — Structured, AI-enriched articles
- `gold_market_summary.csv` — Aggregated daily commodity sentiment and recommendations

### Example Workflow
1. Load Petrinex data using `notebooks/01_setup_data` (see Quick Start above).
2. Run the three pipeline scripts in order (bronze → silver → gold).
3. Analyze the resulting Delta tables or CSVs for signal extraction, context linking, and decision framing.
4. Integrate with Genie Space or a Knowledge Assistant for real-time decision intelligence.

---

## Screenshots

![Databricks Genie space for Commodities Ontology](notebooks/Databricks%20Genie%20space%20for%20Commodities%20Ontology.png)
![Medallion architecture overview](notebooks/Medallion%20architecture%20overview.png)


---

## What You Get

| Table | Rows | Source | Description |
|-------|------|--------|-------------|
| **volumetrics** | ~27M | Pre-downloaded (2024-2025) | Facility-level monthly production volumes. Every barrel of oil, MCF of gas, and m3 of water produced, flared, vented, injected, or consumed as fuel at every facility in Alberta. |
| **ngl_volumes** | ~2.5M | Pre-downloaded (2024-2025) | Well-level monthly production. Gas, oil, condensate, water, plus NGL components (ethane, propane, butane, pentane) per well. |
| **facilities** | ~30K | Derived | Unique facilities with lat/lon coordinates (converted from DLS), type, operator, region. Ready for mapping. |
| **operators** | ~600 | Derived | Real operator profiles aggregated from production data: facility count, total oil/gas/condensate/water, active months. |
| **wells** | ~100K | Derived | Per-well summary: production totals, geological formation (Montney, Mannville, Cardium, etc.), field name (PEMBINA, KAYBOB, etc.), operator, facility linkage. |
| **field_codes** | 80 | AER Reference | Maps numeric AER field codes to names (e.g., 0877 → PEMBINA). |
| **facility_emissions** | ~400K | Derived | Real Petrinex-reported flaring, venting, and fuel gas volumes per facility per month. |
| **market_prices** | 14 | Public benchmarks | Monthly WTI, WCS, AECO, USD/CAD (Jan 2025 -- Feb 2026). |

### PDFs (for Knowledge Assistants)

Downloaded to `/Volumes/{catalog}/{schema}/documentation/`:

| Document | Source | Size |
|----------|--------|------|
| AER Directive 007 | Alberta Energy Regulator | 315 KB |
| AER Manual 011 | Alberta Energy Regulator | 2.6 MB |
| Petrinex Volumetrics Guide | Petrinex | 863 KB |
| Petrinex NGL Guide | Petrinex | 321 KB |
| Petrinex Well Infrastructure Guide | Petrinex | 817 KB |

---

## Data Model

```
data/
├── volumetrics/     (24 ZIP files, 181 MB -- Vol 2024-01 to 2025-12)
└── ngl_volumes/     (24 ZIP files, 78 MB -- NGL 2024-01 to 2025-12)

        ↓ notebook loads into ↓

volumetrics (27M)          ngl_volumes (2.5M)
     |                          |
     +---> facilities (30K)     +---> wells (100K)
     |         |                       |
     +---> operators (600)             +---> field_codes (80)
     |
     +---> facility_emissions (400K)

market_prices (14) -- standalone reference
```

### Key Relationships

- `volumetrics.ReportingFacilityID` = `facilities.facility_id`
- `volumetrics.OperatorBAID` = `operators.operator_baid`
- `ngl_volumes.WellID` = `wells.well_id`
- `wells.facility_id` = `facilities.facility_id` (wells belong to facilities)
- `wells.field_name` = `field_codes.field_code` (field name lookup)
- `facility_emissions.facility_id` = `facilities.facility_id`

---

## Key Columns & Codes

### Product IDs (in volumetrics)
| Code Pattern | Product |
|---|---|
| `%OIL%`, `%CRD%` | Crude oil |
| `%GAS%` | Natural gas |
| `%CND%`, `%COND%` | Condensate |
| `%WTR%`, `%WATER%` | Produced water |

### Activity IDs (in volumetrics)
| Code | Activity | Records |
|---|---|---|
| `PROD` | Production | ~12M |
| `DISP` | Disposition (sent out) | ~2.4M |
| `REC` | Received | ~1.7M |
| `FUEL` | Fuel gas consumed | ~1.5M |
| `VENT` | Vented gas | ~1.4M |
| `INJ` | Injection | ~550K |
| `FLARE` | Flared gas | ~150K |

**Important:** When aggregating total production, filter to `ActivityID = 'PROD'` to avoid double-counting volumes that flow through multiple facilities.

### Facility Types
| Code | Type | Count |
|---|---|---|
| BT | Battery | ~25,000 |
| GS | Gas Gathering System | ~2,800 |
| IF | Injection Facility | ~2,000 |
| GP | Gas Plant | ~500 |

### Formations (in wells table)
Derived from AER pool code prefixes:

| Formation | Wells | Description |
|---|---|---|
| Montney | 48,000 | NW Alberta, major tight gas/condensate play |
| Mannville | 10,700 | Conventional oil & gas across Alberta |
| Spirit River | 5,900 | Deep basin gas |
| Cardium | 4,800 | Light tight oil, central Alberta |
| Duvernay | 3,800 | Shale gas/condensate, west-central |
| Viking | 1,300 | Shallow gas, southern/central Alberta |
| + 19 more | | Leduc, Edmonton, Nisku, Halfway, Wilrich, etc. |

### Unit Conversions (Petrinex reports in metric)
| Petrinex Unit | O&G Standard | Conversion |
|---|---|---|
| m3 (oil/condensate/water) | barrels (bbl) | × 6.2898 |
| e3m3 (gas) | MCF (thousand cubic feet) | × 35.3147 |
| m3/month → bbl/day | bbl/d | × 6.2898 / 30.44 |
| e3m3/month → MCF/day | MCF/d | × 35.3147 / 30.44 |
| GJ | GJ | (no conversion) |

---

## Sample Queries

### Top 10 oil producers by daily rate
```sql
SELECT OperatorName,
       ROUND(SUM(Volume) * 6.2898 / (COUNT(DISTINCT ProductionMonth) * 30.44)) as avg_bbl_per_day,
       COUNT(DISTINCT ReportingFacilityID) as facilities
FROM {catalog}.{schema}.volumetrics
WHERE (ProductID LIKE '%OIL%' OR ProductID LIKE '%CRD%')
AND ActivityID = 'PROD'
GROUP BY OperatorName
ORDER BY avg_bbl_per_day DESC
LIMIT 10
```

### Wells by formation
```sql
SELECT formation, COUNT(*) as wells,
       ROUND(SUM(total_oil_m3) * 6.2898) as total_oil_bbl,
       ROUND(SUM(total_gas_e3m3) * 35.3147) as total_gas_mcf
FROM {catalog}.{schema}.wells
GROUP BY formation
ORDER BY wells DESC
```

### Top flaring facilities
```sql
SELECT f.facility_name, f.operator_name, f.region,
       SUM(e.flare_volume) as total_flare_e3m3,
       SUM(e.vent_volume) as total_vent_e3m3
FROM {catalog}.{schema}.facility_emissions e
JOIN {catalog}.{schema}.facilities f ON e.facility_id = f.facility_id
GROUP BY f.facility_name, f.operator_name, f.region
ORDER BY total_flare_e3m3 DESC
LIMIT 20
```

### Peak producing oil wells with field names
```sql
SELECT w.well_id, w.field_display_name, w.formation, w.operator_name,
       ROUND(MAX(CAST(n.OilProduction AS DECIMAL(12,1))) * 6.2898 / 30.44, 1) as peak_bbl_d
FROM {catalog}.{schema}.ngl_volumes n
JOIN {catalog}.{schema}.wells w ON n.WellID = w.well_id
WHERE n.WellID LIKE 'ABWI%'
GROUP BY w.well_id, w.field_display_name, w.formation, w.operator_name
ORDER BY peak_bbl_d DESC
LIMIT 20
```

---

## Ideas for Hackathon Projects

- **Interactive Map** -- Plot 30K facilities and 126K wells using Leaflet.js, filter by formation/operator
- **Genie Space** -- Text-to-SQL over all 8 tables for natural language production analytics
- **Knowledge Assistant** -- RAG over the AER/Petrinex PDFs for regulatory Q&A
- **Multi-Agent Supervisor** -- Combine Genie + KA for a unified energy intelligence agent
- **Decline Curve Analysis** -- Use well-level monthly data to model production decline curves
- **Emissions Dashboard** -- Rank operators/facilities by flaring and venting intensity
- **Operator Benchmarking** -- Compare operators by production per facility, emissions intensity
- **Formation Analytics** -- Compare Montney vs Duvernay vs Cardium well performance
- **Price Correlation** -- Correlate WTI/WCS prices with Alberta production volumes

---

## Repo Structure

```
Calgary_Hackathon_Data_March_26/
├── README.md
├── notebooks/
│   └── 01_setup_data.py       # Run this -- creates all 8 tables + downloads PDFs
└── data/
    ├── volumetrics/            # 24 Petrinex Vol ZIPs (2024-01 to 2025-12, 181 MB)
    │   ├── Vol_2024-01.zip
    │   ├── Vol_2024-02.zip
    │   └── ... (24 files)
    └── ngl_volumes/            # 24 Petrinex NGL ZIPs (2024-01 to 2025-12, 78 MB)
        ├── NGL_2024-01.zip
        ├── NGL_2024-02.zip
        └── ... (24 files)
```

The ZIP files contain nested ZIPs with CSVs inside (Petrinex's standard format). The notebook handles the extraction automatically.

---

## Data Sources & Credits

- **Petrinex Public Data** -- [petrinex.ca](https://www.petrinex.ca/PD/Pages/default.aspx) -- Alberta's Petroleum Registry
- **petrinex Python package** -- [github.com/guanjieshen/petrinex-python-api](https://github.com/guanjieshen/petrinex-python-api) -- Thanks to Guanjie Shen
- **AER Directive 007** -- Volumetric and Infrastructure Requirements
- **AER Manual 011** -- How to Submit Volumetric Data
- **Market Prices** -- EIA (WTI), Alberta Government (WCS, AECO), Bank of Canada (USD/CAD)
- **Field Codes** -- Alberta Energy Regulator field code registry

---

## Requirements

- Databricks workspace with **Unity Catalog** enabled
- **Serverless** compute or any cluster (no special requirements)
- **Internet access** only needed for PDF downloads (production data is included in the repo)
- ~5 minutes runtime (reads from local files)
- Compatible with both serverless and classic compute (pure SQL transformations)

## License

Data is sourced from publicly available Petrinex records. For hackathon and educational use only.
