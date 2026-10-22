"""Generic raw -> silver sync: stream the raw table's Change Data Feed and MERGE
each micro-batch, transformed, into the silver table on `key`."""
from __future__ import annotations

import os
import sys
from typing import Callable

from common.merge import latest_changes

_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def register_src_modules_by_value() -> None:
    """Make cloudpickle ship our src/ packages by value. On serverless (Spark Connect)
    the foreachBatch function is unpickled in a separate Python process where src/
    is not on sys.path, so references to e.g. common.merge or cve_v5.transform must
    not be pickled as imports."""
    from pyspark import cloudpickle

    for name in {n.split(".")[0] for n in list(sys.modules)}:
        mod = sys.modules.get(name)
        path = getattr(mod, "__file__", None)   # None placeholders, built-in modules
        if path and os.path.abspath(path).startswith(_SRC + os.sep):
            cloudpickle.register_pickle_by_value(mod)


def stream_cdf_to_silver(spark, raw_table: str, silver_table: str, key: str,
                         transform_fn: Callable, checkpoint: str, apply_deletes: bool = False) -> None:
    """If silver is empty, MERGE the full raw snapshot first (CDF history may not
    cover the whole table) and start the stream at the latest raw version; then
    process all pending changes (availableNow) and return.

    `transform_fn(df)` gets raw rows plus `_change_type`. Without `apply_deletes` it
    must return exactly the silver columns. With `apply_deletes` it must also keep
    `_change_type`; a last change of type 'delete' removes the silver row."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    register_src_modules_by_value()

    def merge(df):
        src = transform_fn(latest_changes(df, key, apply_deletes)).alias("s")
        m = DeltaTable.forName(spark, silver_table).alias("t").merge(src, f"t.{key} = s.{key}")
        if apply_deletes:
            values = {c: f"s.{c}" for c in spark.table(silver_table).columns}
            m = (m.whenMatchedDelete(condition="s._change_type = 'delete'")
                 .whenMatchedUpdate(set=values)
                 .whenNotMatchedInsert(condition="s._change_type != 'delete'", values=values))
        else:
            m = m.whenMatchedUpdateAll().whenNotMatchedInsertAll()
        m.execute()

    def merge_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            merge(batch_df)

    stream = spark.readStream.option("readChangeFeed", "true")
    if spark.table(silver_table).isEmpty():
        print("Initial load: processing full raw snapshot...")
        merge(spark.table(raw_table)
              .withColumn("_change_type", F.lit("insert"))
              .withColumn("_commit_version", F.lit(0).cast("long")))
        print(f"Initial load complete: {spark.table(silver_table).count()} rows")
        version = spark.sql(f"DESCRIBE HISTORY {raw_table} LIMIT 1").select("version").first()[0]
        print(f"Starting streaming from version {version}...")
        stream = stream.option("startingVersion", version)

    (stream.table(raw_table)
     .writeStream
     .foreachBatch(merge_batch)
     .option("checkpointLocation", checkpoint)
     .trigger(availableNow=True)
     .start()
     .awaitTermination())
    print("Streaming merge complete")
