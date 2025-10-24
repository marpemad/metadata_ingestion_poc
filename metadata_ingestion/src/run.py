import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Ensure local runs can import packages living under src/
repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from ingestion_framework.framework import run


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Metadata ingestion entry point.")
    parser.add_argument(
        "--sources_yaml",
        default=str(repo_root / "metadata" / "sources.yaml"),
        help="Ruta al archivo YAML con la configuración de fuentes.",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Nombre del entorno (por ejemplo: dev, qa, prod).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

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
    run(spark, args.sources_yaml, env=args.env)
    print("PoC finished successfully.")


if __name__ == "__main__":
    main()
