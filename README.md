# Energy-Data-Engineering-BayWa
This project implements a PySpark-based data pipeline that ingests public energy and price data from api.energy-charts.info and structures it using the bronze-silver-gold medallion architecture.


# Virtuel environment
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python -m src.pipeline.run_all


# Reset
rm -rf data/bronze
rm -rf data/silver
rm -rf data/gold


# Adds all changed files in the current directory and subdirectories
git add .
# Creates a snapshot of your staged changes
git commit -m "pipe line end to end"
# Replace 'main' with 'master' if your project uses the older default branch name
git push origin main
