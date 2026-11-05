"""EPSS loader helpers: pure Python (no Spark), unit-tested in tests/unit/test_epss.py.
The Spark parts (read_scores, to_history) are in transform.py."""
from __future__ import annotations

import datetime
import gzip
import io
from typing import Iterable, TypeVar

# No EPSS scores were published before this date.
FIRST_SCORE_DATE = datetime.date(2021, 4, 14)
# Column layouts seen in the daily files: `percentile` was added in 2021-09.
COLUMNS = ["cve", "epss", "percentile"]

T = TypeVar("T")


def score_filename(d: datetime.date) -> str:
    return f"epss_scores-{d.isoformat()}.csv.gz"


def score_url(base_url: str, d: datetime.date) -> str:
    """URL of the daily scores file for `d`."""
    return f"{base_url}/{score_filename(d)}"


def dates_to_load(today: datetime.date, lookback_days: int,
                  loaded: Iterable[datetime.date]) -> list[datetime.date]:
    """Dates in [today - lookback_days, today] (not before FIRST_SCORE_DATE) that are
    not in `loaded`, oldest first."""
    start = max(FIRST_SCORE_DATE, today - datetime.timedelta(days=lookback_days))
    done = set(loaded)
    return [d for d in (start + datetime.timedelta(days=i) for i in range((today - start).days + 1))
            if d not in done]


def batches(items: list[T], size: int) -> list[list[T]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def parse_header(data: bytes) -> tuple[str | None, list[str]]:
    """(model_version, columns) of a gzip daily scores file.

    Since 2022-02-04 the files start with `#model_version:<v>,score_date:<ts>`, in
    2022-2025 as a separate gzip member (GzipFile reads multi-member files); older
    files have no comment line. Raises ValueError on an unknown column layout, so a
    format change fails the load instead of loading wrong columns."""
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
        line = f.readline().decode("utf-8").strip()
        model_version = None
        if line.startswith("#"):
            meta = dict(kv.split(":", 1) for kv in line[1:].split(",") if ":" in kv)
            model_version = meta.get("model_version")
            line = f.readline().decode("utf-8").strip()
    columns = line.split(",")
    if columns not in (COLUMNS[:2], COLUMNS):
        raise ValueError(f"unexpected EPSS columns: {line!r}")
    return model_version, columns
