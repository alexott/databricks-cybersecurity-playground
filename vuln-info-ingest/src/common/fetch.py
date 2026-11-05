"""HTTP download with retries for single-record fetches (pure Python)."""
from __future__ import annotations

import http.client
import time
import urllib.error
import urllib.request

_RETRY_CODES = (429, 500, 502, 503, 504)


def fetch_with_retries(url: str, attempts: int = 4, timeout: int = 60, sleep=time.sleep,
                       missing_codes: tuple[int, ...] = (404,), strip: bool = True) -> bytes | None:
    """Body of `url` (stripped unless `strip=False`, e.g. for gzip), or None on one of
    `missing_codes` (record deleted after it was listed; S3 answers 403 for a missing key).
    Retries 429/5xx, connection and read errors with exponential backoff; other HTTP errors raise."""
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                data = r.read()
                return data.strip() if strip else data
        except urllib.error.HTTPError as e:
            if e.code in missing_codes:
                return None
            if e.code not in _RETRY_CODES or attempt == attempts - 1:
                raise
        # URLError covers failures to connect; resets, timeouts and truncated bodies
        # while reading the response surface as plain OSError / HTTPException.
        except (OSError, http.client.HTTPException):
            if attempt == attempts - 1:
                raise
        sleep(2 ** attempt)
    return None
