# Energy-Data-Engineering-BayWa
This project implements a PySpark-based data pipeline that ingests public energy and price data from api.energy-charts.info and structures it using the bronze-silver-gold medallion architecture. 

The pipeline follows the Bronze-Silver-Gold pattern to ensure data lineage and quality:

Bronze (Raw): Stores immutable, raw JSON responses from the API, partitioned by ingestion date.

Silver (Cleaned): Normalizes data types, converts all timestamps to UTC, and handles the 15-minute/hourly resolution transition seamlessly.

Gold (Curated): Aggregates data into daily business KPIs:

Daily Power Mix: Total MWh production per day by type.

Price vs. Offshore Correlation: Daily average price joined with offshore wind production.

## 💻 Windows Setup (WSL)
If you are on Windows, it is strongly recommended to use **WSL2 with Ubuntu**. 

1. Install WSL: `wsl --install`
2. Install Java 11: `sudo apt update && sudo apt install openjdk-11-jdk`
3. Verify Java: `java -version`

Using WSL avoids common `winutils.exe` errors associated with running Spark directly on Windows.

# Virtuel environment
python -m venv venv
source venv/bin/activate

# Prerequisites
Python 3.12+
Java 8 or 11 (Required for Apache Spark)
pip install -r requirements.txt


# Run Ingestion -> Silver -> Gold -> Quality Checks
python -m src.pipeline.run_all

# Generate Analysis Diagrams
python -m src.visualization.plot_results

# Reset
rm -rf data/bronze
rm -rf data/silver
rm -rf data/gold

# Configuration & Backfilling
You can control the ingestion window via environment variables. This allows for easy historical backfilling:

export START_DATE="2024-01-01"
export END_DATE="2024-12-31"
python -m src.pipeline.run_all


# Project Structre
energy-data-engineering/
├── data/                   # Local Delta Lake storage (Exclude from Git)
│   ├── bronze/             # Raw ingestion: exact copies of API responses
│   ├── silver/             # Cleaned data: normalized types, UTC timestamps
│   └── gold/               # Analytics ready: daily aggregations and KPIs
├── diagrams/               # Documentation assets and generated plots (.png)
├── src/                    # Source code root
│   ├── api/                # Low-level API clients and request handling
│   ├── bronze/             # Phase 1: Ingestion logic (Raw -> Bronze)
│   ├── silver/             # Phase 2: Transformation logic (Bronze -> Silver)
│   ├── gold/               # Phase 3: Business logic (Silver -> Gold)
│   ├── config/             # Settings, environment variables, and constants
│   ├── ingestion/          # Ingestion orchestration and backfill logic
│   ├── pipeline/           # Master orchestration scripts (e.g., run_all.py)
│   ├── quality/            # Data validation gates and integrity checks
│   ├── utils/              # Shared helpers (Spark session, logging, I/O)
│   └── visualization/      # Scripts for generating analysis plots/diagrams
├── .gitignore              # Prevents /data, /venv, and caches from being pushed
├── README.md               # Setup instructions, assumptions, and architecture
├── requirements.txt        # Project dependencies (Spark, Delta, Pandas, etc.)
├── daily_power_mix_trend.png      # Use Case 1 Output
└── price_offshore_correlation.png # Use Case 2 Output