from typing import Callable, Dict, Any
from pyspark.sql import SparkSession, DataFrame

def csv_reader(spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
    path = options.pop("path")
    return spark.read.options(**options).csv(path)

def json_reader(spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
    path = options.pop("path")
    return spark.read.options(**options).json(path)

def parquet_reader(spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
    path = options.pop("path")
    return spark.read.options(**options).parquet(path)

def jdbc_reader(spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
    url = options.get("url")
    driver = options.get("driver", None)
    dbtable = options.get("dbtable")
    reader = spark.read.format("jdbc").option("url", url).option("dbtable", dbtable)
    if driver:
        reader = reader.option("driver", driver)
    user = options.get("user")
    password = options.get("password")
    if user:
        reader = reader.option("user", user)
    if password:
        reader = reader.option("password", password)
    return reader.load()

def olap_reader(spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
    # if XMLA libs are not present, we **fallback** to a sample CSV export.
    
    try:
        # Example with 'olap.xmla' (if installed): you'd implement MDX -> rows here.
        # from olap.xmla.xmla import XMLAProvider
        # provider = XMLAProvider()
        # connect & execute MDX... convert rows -> Spark DataFrame
        raise ImportError("XMLA client not installed in this runtime")
    except Exception:
        fallback = options.get("fallback_csv_path")
        return spark.read.option("header", True).csv(fallback)

READERS = {
    "csv": csv_reader,
    "json": json_reader,
    "parquet": parquet_reader,
    "jdbc": jdbc_reader,
    "olap": olap_reader
}

def get_reader(kind: str) -> Callable[[SparkSession, Dict[str, Any]], DataFrame]:
    if kind not in READERS:
        raise ValueError(f"Unsupported source type: {kind}")
    return READERS[kind]
