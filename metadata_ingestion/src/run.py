import logging
import sys
from pathlib import Path
from typing import List, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Ensures local and remote databricks execution can find src/
try:
    repo_root = Path(__file__).resolve().parents[1]
except NameError:
    repo_root = Path.cwd().resolve()
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from ingestion_framework.framework import run

SOURCES_YAML_PATH = (
    repo_root / "metadata" / "sources.yaml"
).resolve()


def main(argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    packages = ",".join([
        "io.delta:delta-spark_2.13:4.0.0",
        "org.apache.hadoop:hadoop-azure:3.3.4",
        "com.microsoft.azure:azure-storage:8.6.6",
    ])

    # OJO: primero crea el builder con Delta...
    builder = configure_spark_with_delta_pip(
        SparkSession.builder
            .appName("contoso_poc")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # ...y LUEGO añade los packages (así no te los pisa)
    builder = builder.config("spark.jars.packages", packages)

    spark = builder.getOrCreate()

    spark.conf.set(
        "fs.azure.account.key.storagepoc129.blob.core.windows.net",
        "GfPfsGtoFRGoIbJ3puTo+mIJnjGlaEPCPxKvpHcz4+yeghcZn5m8HgsSoD9fT4743pyr/Vn6TGWb+AStn1I3og=="
        #os.environ["AZURE_STORAGE_KEY"],
        )
    run(spark, SOURCES_YAML_PATH)
    print("PoC finished successfully.")


if __name__ == "__main__":
    main()
