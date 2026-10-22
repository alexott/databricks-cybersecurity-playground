"""Per-run fetch-log tables: one appended row per load run; the max `watermark`
column is the resume point for incremental loads."""
from __future__ import annotations

import datetime

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def append_fetch_log(spark, table: str, row: tuple, schema: str) -> None:
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(table)


def read_watermark(spark, table: str) -> datetime.datetime | None:
    """Max `watermark` of the fetch log as a UTC datetime, None if no table/rows.
    Read as epoch micros: collect() of a TIMESTAMP gives a naive datetime in the
    driver's local time zone, which could shift the watermark."""
    if not spark.catalog.tableExists(table):
        return None
    us = spark.table(table).selectExpr("unix_micros(max(watermark))").first()[0]
    return None if us is None else _EPOCH + datetime.timedelta(microseconds=us)
