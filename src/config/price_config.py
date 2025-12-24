from datetime import date
import os

BIDDING_ZONE = "DE-LU"

START_DATE = os.getenv("START_DATE", "2024-01-01")
END_DATE = os.getenv("END_DATE", "2024-01-07")

MAX_DAYS = int(os.getenv("MAX_DAYS", "7"))

# Resolution change date (business rule)
RESOLUTION_CHANGE_DATE = date(2025, 10, 1)
