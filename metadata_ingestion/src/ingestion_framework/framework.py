import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .metadata import load_sources
from config import Config
from .writer import write_raw, write_hub
from .transform import to_hub
from .readers.base_reader import get_reader
from datetime import datetime

logger = logging.getLogger(__name__)

def run(spark: SparkSession, sources_yaml: str, env: str = "dev"):
    ss = load_sources(sources_yaml)
    cfg = Config.from_defaults(raw_base=ss.defaults["raw_base"],
                               hub_base=ss.defaults["hub_base"],
                               checkpoint_base=ss.defaults["checkpoint_base"],
                               env=env)
    for s in ss.sources:
        if not s.enabled:
            logger.info("Source %s disabled; skipping.", s.id)
            continue
        reader = get_reader(s.type)
        logger.info("Starting ingestion for source=%s type=%s domain=%s entity=%s", s.id, s.type, s.domain, s.entity)
        logger.debug("Reader options for source %s: %s", s.id, s.options)
        df = reader(spark, s.options)
        # audit columns
        ingest_date = datetime.utcnow().date().isoformat()
        df_with_meta = (
            df.withColumn("_source_id", F.lit(s.id))
              .withColumn("_ingest_ts_utc", F.current_timestamp())
              .withColumn("ingest_date", F.lit(ingest_date))
        )
        # write RAW
        raw_path = f"{cfg.raw_base}/{s.domain}/{s.entity}/"
        write_raw(df_with_meta, raw_path, s.raw_partitions)
        # transform -> HUB
        hub_df = to_hub(df_with_meta, s)
        hub_path = f"{cfg.hub_base}/{s.domain}/{s.entity}"
        write_hub(hub_df, hub_path, s.hub_primary_keys, checkpoint_base=cfg.checkpoint_base, source_id=s.id)
        logger.info("Finished ingestion for source=%s", s.id)
