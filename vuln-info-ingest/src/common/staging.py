"""Staging of JSON records as gzip JSON-line parts on a Volume (pure Python)."""
from __future__ import annotations

import glob
import gzip
import json
import os
from typing import Iterable


def to_single_line(data: bytes) -> bytes:
    """Compact JSON is passed through; JSON with newlines is re-serialized onto one line."""
    if b"\n" in data or b"\r" in data:
        return json.dumps(json.loads(data), separators=(",", ":")).encode("utf-8")
    return data


def write_text_parts(records: Iterable[bytes], out_dir: str, batch: int) -> tuple[int, int]:
    """Write JSON lines into gzip parts (part-00000.jsonl.gz, ...) of `batch` records.
    Returns (records written, parts written)."""
    os.makedirs(out_dir, exist_ok=True)
    n, parts, out = 0, 0, None
    for rec in records:
        if n % batch == 0:
            if out:
                out.close()
            # compresslevel=1: full snapshots are many GB of JSON; speed matters more than size
            out = gzip.open(f"{out_dir}/part-{parts:05d}.jsonl.gz", "wb", compresslevel=1)
            parts += 1
        out.write(rec)
        out.write(b"\n")
        n += 1
    if out:
        out.close()
    return n, parts


def clear_parts(out_dir: str) -> None:
    """Remove only our own staging parts, never other files in the directory."""
    for f in glob.glob(f"{out_dir}/part-*.jsonl.gz"):
        os.remove(f)
