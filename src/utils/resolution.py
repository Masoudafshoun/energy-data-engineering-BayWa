from datetime import date
from src.config.price_config import RESOLUTION_CHANGE_DATE

def detect_resolution(data_date: date) -> str:
    if data_date < RESOLUTION_CHANGE_DATE:
        return "1h"
    return "15min"
