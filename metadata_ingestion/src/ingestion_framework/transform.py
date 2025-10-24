from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .metadata import Source

def to_hub(df: DataFrame, s: Source) -> DataFrame:
    # Minimal standardization: drop RAW‑only audit columns if desired, keep ingest_date
    #keep_cols = [c for c in df.columns]
    #return df.select(*keep_cols)
    return df
