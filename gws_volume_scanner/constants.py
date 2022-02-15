"""Constants for the GWS Scanner."""
import datetime as dt
import typing

SizeBucket = typing.TypedDict(
    "SizeBucket", {"key": str, "from": int, "to": int}, total=False
)
SIZE_BUCKETS: typing.List[SizeBucket] = [
    {"key": "*-10B", "from": -1, "to": 10},
    {"key": "10B-100B", "from": 10, "to": 100},
    {"key": "100B-1kB", "from": 100, "to": 1000},
    {"key": "1kB-10kB", "from": 1000, "to": 10_000},
    {"key": "10kB-100kB", "from": 10_000, "to": 100_000},
    {"key": "100kb-1MB", "from": 100_000, "to": 1000_000},
    {"key": "1MB-10MB", "from": 1000_000, "to": 10_000_000},
    {"key": "10MB-100MB", "from": 10_000_000, "to": 100_000_000},
    {"key": "100MB-1GB", "from": 100_000_000, "to": 1000_000_000},
    {"key": "1GB-10GB", "from": 1000_000_000, "to": 10_000_000_000},
    {"key": "10GB-100GB", "from": 10_000_000_000, "to": 100_000_000_000},
    {"key": "100GB-1TB", "from": 100_000_000_000, "to": 1000_000_000_000},
    {"key": "1TB-10TB", "from": 1000_000_000_000, "to": 10_000_000_000_000},
    {"key": "10TB-*", "from": 10_000_000_000_000},
]


TimeBucket = typing.TypedDict(
    "TimeBucket", {"key": str, "from": dt.timedelta, "to": dt.timedelta}, total=False
)
TIME_BUCKETS: typing.List[TimeBucket] = [
    {"key": "*-1h", "from": dt.timedelta(days=-1), "to": dt.timedelta(hours=1)},
    {"key": "1h-1d", "from": dt.timedelta(hours=1), "to": dt.timedelta(days=1)},
    {"key": "1d-1w", "from": dt.timedelta(days=1), "to": dt.timedelta(weeks=1)},
    {"key": "1w-1m", "from": dt.timedelta(weeks=1), "to": dt.timedelta(days=30)},
    {"key": "1m-3m", "from": dt.timedelta(days=30), "to": dt.timedelta(days=90)},
    {"key": "3m-6m", "from": dt.timedelta(days=90), "to": dt.timedelta(days=180)},
    {"key": "6m-1y", "from": dt.timedelta(days=180), "to": dt.timedelta(days=365)},
    {"key": "1y-2y", "from": dt.timedelta(days=365), "to": dt.timedelta(days=730)},
    {"key": "2y-5y", "from": dt.timedelta(days=730), "to": dt.timedelta(days=1825)},
    {"key": "5y-*", "from": dt.timedelta(days=1825)},
]
