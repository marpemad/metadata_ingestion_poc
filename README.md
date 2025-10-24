# Contoso — Metadata‑Driven Ingestion PoC (PySpark + Delta Lake)

This PoC implements a reusable **metadata‑driven** ingestion framework capable of:

- Reading a list of sources (CSV / JSON / JDBC / OLAP fallback) from `metadata_ingestion/metadata/sources.yaml`.
- Landing data **as‑is** into a RAW zone and conforming it into a **Delta Lake “Hub”** zone.
- Running both **locally** (PySpark + local filesystem) and in **cloud environments** (Databricks / Azure Storage).

Everything is orchestrated from a single Python entrypoint (`metadata_ingestion/src/run.py`) so Databricks Jobs, ADF pipelines, or cron jobs can schedule it without relying on notebooks or widgets.

## Architecture

```
sources.yaml  ──►  ingestion_framework.framework.run(...)
                              │
                              ├─► RAW zone  (append-only, audit columns, partitions)
                              └─► HUB zone  (Delta tables, upserts with merge)
```

- **Source metadata**: declarative YAML (can evolve into SQL/REST/Unity Catalog later).
- **Readers**: pluggable adapters (`csv`, `json`, `parquet`, `jdbc`, `olap` with CSV fallback).
- **Writers**: RAW writes Parquet; HUB writes Delta with schema merge enabled.

## Project layout (updated)

```
metadata_ingestion_poc/
├── metadata_ingestion/
│   ├── metadata/sources.yaml      # “source system” definitions (RAW/HUB defaults included)
│   ├── data/samples/              # CSV / JSON / SQLite fallback datasets
│   └── src/
│       ├── run.py                 # CLI entrypoint (no notebooks/widgets)
│       └── ingestion_framework/
│           ├── config.py
│           ├── framework.py
│           ├── metadata.py
│           ├── transform.py
│           ├── writer.py
│           └── readers/
│               └── base_reader.py (and helpers)
├── notebooks/                     # legacy notebook kept for reference (not required anymore)
├── azure_data_factory/databricks_job.json
├── Pipfile / requirements.txt
└── README.md
```

## Running the ingestion script

```bash
# (Recommended) create a virtualenv / pyenv shell with Python 3.10+
pip install -r requirements.txt

# Default parameters (can be omitted)
python metadata_ingestion/src/run.py \
    --sources_yaml metadata_ingestion/metadata/sources.yaml \
    --env dev
```

`run.py` wires the Spark session with Delta (`configure_spark_with_delta_pip`) and expects the YAML file to define reader options. The default YAML includes:

- `metadata_ingestion/data/samples/customers.csv`
- `metadata_ingestion/data/samples/orders.json`
- Optional JDBC + OLAP examples (disabled by default).

### CLI arguments

- `--sources_yaml`: path to a metadata file (defaults to the repo path above).
- `--env`: string tag that flows into audit columns and configuration (`dev`, `local`, `prod`, ...).

## Local filesystem mode

If you keep the defaults in `metadata_ingestion/metadata/sources.yaml` you will land data into the repo under `_lake/`:

```
defaults:
  raw_base: ./_lake/raw
  hub_base: ./_lake/hub
  checkpoint_base: ./_lake/_checkpoints
```

Nothing else is required besides installing Python dependencies. Spark will create directories on demand.

## Writing to Azure Storage

You can switch the defaults in `sources.yaml` to Azure URIs. Two common setups:

### 1. ADLS Gen2 (hierarchical namespace enabled)

```yaml
defaults:
  raw_base: abfss://raw@<account>.dfs.core.windows.net/contoso
  hub_base: abfss://hub@<account>.dfs.core.windows.net/contoso
  checkpoint_base: abfss://checkpoints@<account>.dfs.core.windows.net/contoso/_checkpoints
```

Spark config (before running `run.py`):

```python
spark.conf.set(
    "fs.azure.account.key.<account>.dfs.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"]  # or use OAuth configs
)
```

If you use OAuth / service principals, set the `fs.azure.account.oauth.*` configs instead of the key.

### 2. Azure Blob Storage (no hierarchical namespace)

```yaml
defaults:
  raw_base: wasbs://raw@<account>.blob.core.windows.net/raw
  hub_base: wasbs://hub@<account>.blob.core.windows.net/hub
  checkpoint_base: wasbs://checkpoints@<account>.blob.core.windows.net/checkpoints
```

Spark config:

```python
spark.conf.set(
    "fs.azure.account.key.<account>.blob.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"]
)
```

> **Important**: `configure_spark_with_delta_pip` already downloads the Delta JARs.  
> To enable `wasbs://` support we pass `org.apache.hadoop:hadoop-azure:3.3.4` via the `extra_packages` argument inside `run.py`. No additional manual `spark-submit --packages` is required.

## Databricks / ADF orchestration

- Reuse the same script (`python metadata_ingestion/src/run.py ...`) inside a Databricks Job task or ADF Databricks activity.
- Provide the `--sources_yaml` path relative to DBFS or workspace.
- Manage secrets via Databricks Secret Scope (map them to `fs.azure.account.key...`).
- The legacy notebook in `notebooks/` can be deleted once existing pipelines migrate to the script entrypoint.

## Modifying the source metadata

`metadata_ingestion/metadata/sources.yaml` controls:

- `defaults`: RAW/HUB/checkpoint roots and common domain.
- `sources`: list of ingestion tasks. Each entry includes reader options (e.g., `header: true` for CSV) and hub keys for Delta merges.

Add new sources by appending YAML entries; the framework will automatically iterate over them. To add a brand‑new reader type implement a function in `readers/base_reader.py` and register it in the `READERS` map.

## Dependencies and packages

- Python: see `requirements.txt` / `Pipfile` (PySpark 4.x, delta-spark, pydantic, PyYAML, etc.).
- Spark session:
  - Delta Lake JARs (handled by `configure_spark_with_delta_pip`).
  - Azure drivers when targeting storage (`org.apache.hadoop:hadoop-azure:3.3.4`).
- Optional JDBC drivers (e.g., `org.xerial:sqlite-jdbc`) can be added via cluster libraries or `spark.jars.packages`.

## Troubleshooting tips

- **`ClassNotFoundException` for Delta**: ensure the script is using `configure_spark_with_delta_pip` and you are not overriding `spark.jars.packages` afterwards.
- **`NativeAzureFileSystem$Secure not found`**: confirm `hadoop-azure` is being added through `extra_packages`, and the storage account key is set for the correct domain (`.dfs.core.windows.net` vs `.blob.core.windows.net`).
- **Headers missing in CSV**: set `header: true` in the YAML options so merge keys exist when writing to Delta.

## Roadmap & talking points

- Why **metadata-driven**? → zero-code onboarding, centralized governance, and easier observability.
- Why **RAW + Hub**? → RAW is the immutable system-of-record; Hub is conformed and query-friendly (Delta allows upserts, schema evolution, and time travel).
- Next steps: automated validation (Great Expectations), orchestration with Databricks Workflows/ADF, data contracts & catalog integration.

## Sample JDBC helper (optional)

To play with the JDBC reader locally:

```bash
sqlite3 metadata_ingestion/data/samples/contoso.sqlite \
  'CREATE TABLE products (product_id INT PRIMARY KEY, name TEXT, price DOUBLE);'

sqlite3 metadata_ingestion/data/samples/contoso.sqlite \
  "INSERT INTO products VALUES (10,'Phone',699.0),(20,'Headphones',199.0),(30,'Tablet',499.0);"
```

Update the corresponding YAML entry and add the SQLite JDBC jar (`org.xerial:sqlite-jdbc:3.45.1.0`) to Spark if you enable that source.
