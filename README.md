# NHS Antibiotics Prescribing — ICB SQL Analysis (PostgreSQL)

This project analyses NHS antibiotic prescribing patterns using the NHSBSA English Prescribing Dataset (EPD) with SNOMED code.  
I built a small end-to-end pipeline to download large monthly extracts, filter them to antibiotics, load them into PostgreSQL, and run SQL analysis at Integrated Care Board (ICB) level.

The focus is on reproducible data preparation + practical SQL analytics rather than just queries alone.

Across the latest 3 monthly extracts, the filtered antibiotics dataset contained ~1.54 million rows and was aggregated to ICB-month level for comparison.

---

## What this project demonstrates

- Working with large real-world open datasets (multi-GB monthly CSV files)
- Scripted data download using Python
- Chunked CSV processing in Python (memory-safe filtering)
- Domain filtering using BNF classification + drug name logic
- Loading data into PostgreSQL using `psql` and `\copy`
- Building layered schemas: `raw → staging → mart`
- SQL analytics including:
  - aggregation
  - grouping by geography and time
  - window functions (month-to-month change)
  - outlier detection using z-scores
  - derived metrics (cost per item)
- Exporting query outputs for reporting

---

## Data source

NHS Business Services Authority (NHSBSA) Open Data Portal  
English Prescribing Dataset (EPD) with SNOMED code

Data source page:
https://opendata.nhsbsa.net/dataset/english-prescribing-dataset-epd-with-snomed-code

Citation:
NHS Business Services Authority. "English Prescribing Dataset (EPD) with SNOMED Code". Accessed 12/02/26.

---

## Project structure

```
src/
  download_epd_latest_3_months.py
  build_antibiotics_subset.py

sql/
  01_schema.sql
  02_load.sql
  03_transform.sql
  04_analysis.sql
  05_export_outputs.sql

data/ (ignored by git)
  raw/
  processed/

outputs/
  national_trend.csv
  top_icbs_latest_month.csv
  outlier_icbs_latest_month.csv
```

---

## How antibiotics were defined

The dataset provides BNF chapter information at top level (e.g. `05: Infections`).

Antibiotic rows were identified by:

1. Keeping only rows where `BNF_CHAPTER_PLUS_CODE` starts with **05 (Infections)**
2. Applying an antibacterial keyword filter on:
   - `BNF_PRESENTATION_NAME`
   - `BNF_CHEMICAL_SUBSTANCE`

Keyword patterns include terms such as:
`cillin`, `cycline`, `mycin`, `cef`, `ceph`, `floxacin`, `trimethoprim`, `metronidazole`.

This removes most antivirals, antifungals and non-antibacterial infection treatments while remaining fully reproducible from the code.

---

## Data volume

After filtering to antibiotics across the latest 3 months:

- Rows loaded into PostgreSQL: **1,536,554**
- Aggregated mart table (ICB × month): **129 rows**

This keeps the project laptop-friendly while still large enough for realistic SQL analysis.

---

## Main analysis performed

All analysis is done in SQL after loading into PostgreSQL.

### National trends
- Total antibiotic items per month
- Total antibiotic cost per month

### ICB comparisons
- Top ICBs by prescribing volume (latest month)
- Cost per item by ICB
- High-volume vs high-cost areas

### Change over time
- Month-to-month item changes by ICB
- Percentage change using window functions (`LAG`)

### Outlier detection
- Z-score calculation for total items by ICB (latest month)
- Identifies unusually high prescribing regions

---

## How to run (Windows)

### 1. Create and activate virtual environment

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Download latest 3 months

```powershell
py .\src\download_epd_latest_3_months.py
```

### 3. Build antibiotics subset (chunked)

```powershell
py .\src\build_antibiotics_subset.py
```

### 4. Create database and schemas

```powershell
psql -U postgres -c "CREATE DATABASE nhs_antibiotics;"
psql -U postgres -d nhs_antibiotics -f .\sql\01_schema.sql
```

### 5. Load processed CSV

```powershell
psql -U postgres -d nhs_antibiotics -f .\sql\02_load.sql
```

### 6. Transform and build mart tables

```powershell
psql -U postgres -d nhs_antibiotics -f .\sql\03_transform.sql
```

### 7. Run analysis and export outputs

```powershell
psql -U postgres -d nhs_antibiotics -f .\sql\04_analysis.sql
psql -U postgres -d nhs_antibiotics -f .\sql\05_export_outputs.sql
```

---

## Notes and limitations

Only the latest 3 months were used due to very large monthly file sizes (~7GB each)

Antibiotics are identified using reproducible rules, but not an official BNF-SNOMED mapping table

No population normalisation is applied (items per capita), only raw counts and costs

This is designed as a SQL analytics portfolio project rather than a clinical study

---

## Skills demonstrated

PostgreSQL • SQL analytics • Window functions • ETL design • Chunked data processing • Python + SQL workflow • Open data handling • Reproducible pipelines




