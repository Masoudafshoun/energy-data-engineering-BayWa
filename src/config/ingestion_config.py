from datetime import date, timedelta

INGESTION_CONFIG = {
    # ===== MODE =====
    # "daily" or "backfill"
    "mode": "daily",

    # ===== DATE RANGE (used for backfill) =====
    "backfill": {
        "start_date": date(2024, 12, 1),
        "end_date": date(2024, 12, 3),
    },

    # ===== SAFETY LIMITS =====
    # Max number of days allowed in one run (local safety)
    "max_days_per_run": 7,

    # ===== DATA SETTINGS =====
    "country": "de",
    "bidding_zone": "DE-LU",

    # ===== OUTPUT PATHS =====
    "paths": {
        "public_power": "data/bronze/public_power",
        "prices": "data/bronze/prices",
    }
}
