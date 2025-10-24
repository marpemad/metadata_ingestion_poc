import logging
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from typing import List, Optional

logger = logging.getLogger(__name__)

def write_raw(df: DataFrame, path: str, partitions: List[str]):
    logger.info("Writing RAW data to %s with partitions=%s", path, partitions or "[]")
    (df.write
       .mode("append")
       .partitionBy(*partitions)
       .format("parquet")
       .save(path))
    logger.info("RAW write completed for %s", path)

def write_hub(df: DataFrame, path: str, keys: List[str], checkpoint_base: Optional[str], source_id: str):
    # If table exists: MERGE (upsert). Else: write in Delta.
    df.sparkSession.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    logger.info("Writing HUB data for source=%s to %s (keys=%s, checkpoint_base=%s)", source_id, path, keys or "[]", checkpoint_base)
    if DeltaTable.isDeltaTable(df.sparkSession, path):
        delta = DeltaTable.forPath(df.sparkSession, path)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in keys]) if keys else "false"
        (delta.alias("t")
              .merge(df.alias("s"), cond)
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute())
        logger.info("HUB merge completed for %s", path)
    else:
        (df.write
           .format("delta")
           .mode("overwrite" if not keys else "append")
           .save(path))
        logger.info("HUB initial load completed for %s", path)
