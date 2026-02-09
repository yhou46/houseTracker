from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import uuid

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

def generate_unique_time_based_str(prefix: str | None = None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]

    if prefix is not None:
        return f"{prefix}_{short_uuid}_{timestamp}"
    else:
        return f"{short_uuid}_{timestamp}"