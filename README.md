# Energy-Data-Engineering-BayWa
This project implements a PySpark-based data pipeline that ingests public energy and price data from api.energy-charts.info and structures it using the bronze-silver-gold medallion architecture.

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m src.pipeline.run_all
