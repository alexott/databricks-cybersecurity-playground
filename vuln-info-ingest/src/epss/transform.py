"""Parse staged EPSS daily CSV files into the typed history schema.

Files are read with one 3-column schema: Spark assigns columns by position
(enforceSchema), so the 2021 two-column files get a NULL percentile. The column
layout of every file is validated in Python at download time (helpers.parse_header).
The score date comes from the file name; the model version is not in the CSV rows,
so it is passed in per date.

No module-level pyspark import, so the module imports without a local Spark install."""
from __future__ import annotations

import datetime

_SCHEMA = "cve STRING, epss STRING, percentile STRING"
_DATE_RE = r"epss_scores-(\d{4}-\d{2}-\d{2})\.csv"


def read_scores(spark, path: str):
    """Staged `epss_scores-YYYY-MM-DD.csv.gz` files (a directory or file) -> string
    columns cve, epss, percentile plus the source `file_path`."""
    return (spark.read.schema(_SCHEMA)
            .option("header", True).option("comment", "#")
            .csv(path)
            .select("*", "_metadata.file_path"))


def to_history(df, model_versions: dict[datetime.date, str | None]):
    """read_scores output -> cve_id, score_date, epss, percentile, model_version.
    `model_versions` maps each staged score date to its model version (None before
    2022-02-04, when the files had no `#model_version` line)."""
    from pyspark.sql import functions as F

    score_date = F.to_date(F.regexp_extract("file_path", _DATE_RE, 1))
    versions = {str(d): v for d, v in model_versions.items() if v is not None}
    if versions:
        mapping = F.create_map(*[F.lit(x) for kv in versions.items() for x in kv])
        model_version = mapping[score_date.cast("string")]
    else:
        model_version = F.lit(None).cast("string")
    return (df.select(
                F.col("cve").alias("cve_id"),
                score_date.alias("score_date"),
                F.col("epss").cast("double").alias("epss"),
                F.col("percentile").cast("double").alias("percentile"),
                model_version.alias("model_version"))
            .filter("cve_id IS NOT NULL"))
