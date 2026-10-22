"""HTTP download with retries for single-record fetches (pure Python)."""
from __future__ import annotations

import time
import urllib.error
import urllib.request

_RETRY_CODES = (429, 500, 502, 503, 504)


def fetch_with_retries(url: str, attempts: int = 4, timeout: int = 60, sleep=time.sleep) -> bytes | None:
    """Body of `url` (stripped), or None on 404 (record deleted after it was listed).
    Retries 429/5xx and connection errors with exponential backoff; other errors raise."""
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read().strip()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code not in _RETRY_CODES or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        sleep(2 ** attempt)
    return None
