from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def parse_datetime_as_utc(datetime_str: str, format: str | None = None) -> datetime:
    """
    Parse scrapedAt timestamp, ensuring it's timezone-aware and in UTC.

    Args:
        datetime_str: datetime string (with or without timezone info)
        format: datetime string format; None means ISO format

    Returns:
        datetime object in UTC timezone
    """
    # Parse the timestamp (works for both timezone-aware and timezone-naive formats)
    dt: datetime = datetime.strptime(datetime_str, format) if format else datetime.fromisoformat(datetime_str)

    if dt.tzinfo is None:
        # Timezone-naive datetime - assume Pacific Time (UTC-8)
        pacific_tz = ZoneInfo("America/Los_Angeles")
        dt = dt.replace(tzinfo=pacific_tz)
        return dt.astimezone(timezone.utc)
    else:
        # Already timezone-aware - convert to UTC if not already
        return dt.astimezone(timezone.utc)