"""Row-selection helpers used before Delta MERGE (a MERGE fails when several source
rows match one target row). pyspark is imported lazily."""
from __future__ import annotations


def dedupe_latest(df, key: str, order_col: str):
    """One row per `key`: the one with the highest `order_col` (nulls last)."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def latest_changes(df, key: str, apply_deletes: bool):
    """Change Data Feed rows -> the last change per `key` (highest _commit_version).
    Keeps inserts and update post-images, plus deletes when `apply_deletes`.
    `_change_type` is kept; the other CDF metadata columns are dropped."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    types = ["insert", "update_postimage"] + (["delete"] if apply_deletes else [])
    w = Window.partitionBy(key).orderBy(F.col("_commit_version").desc())
    return (df.filter(F.col("_change_type").isin(types))
            .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .drop("_rn", "_commit_version", "_commit_timestamp"))
