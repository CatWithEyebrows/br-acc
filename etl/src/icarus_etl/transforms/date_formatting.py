import pandas as pd


def parse_date(value: str) -> str:
    """Parse a date string to ISO format (YYYY-MM-DD) or empty string.

    Handles: DD/MM/YYYY, DD/MM/YYYY HH:MM:SS, YYYY-MM-DD, YYYYMMDD.
    """
    value = value.strip()
    if not value:
        return ""
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return str(pd.to_datetime(value, format=fmt).strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return value
