from datetime import timedelta

def generate_daily_windows(start_date, end_date, max_days):
    current = start_date
    while current <= end_date:
        batch_end = min(
            current + timedelta(days=max_days - 1),
            end_date
        )
        yield current, batch_end
        current = batch_end + timedelta(days=1)