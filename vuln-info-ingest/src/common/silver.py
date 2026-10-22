"""Generic raw -> silver sync: stream the raw table's Change Data Feed and MERGE
each micro-batch, transformed, into the silver table on `key`."""
from __future__ import annotations

from typing import Callable

from common.merge import latest_changes


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
