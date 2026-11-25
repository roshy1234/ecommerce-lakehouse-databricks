"""Bronze layer ingestion using Databricks Auto Loader (cloudFiles).

Reads incremental raw CSV and NDJSON files from Unity Catalog Volumes and
writes raw Delta tables with audit metadata and schema-drift rescue.

Usage:
    python src/pipelines/01_bronze_ingestion.py
    python src/pipelines/01_bronze_ingestion.py --catalog workspace
    python src/pipelines/01_bronze_ingestion.py --profile DEFAULT
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 


@dataclass(frozen=True)
class BronzeEntity:
    name: str
    file_format: str
    reader_options: dict[str, str] = field(default_factory=dict)


ENTITIES: tuple[BronzeEntity, ...] = (
    BronzeEntity(
        name="customers",
        file_format="csv",
        reader_options={"header": "true"},
    ),
    BronzeEntity(
        name="products",
        file_format="json",
    ),
    BronzeEntity(
        name="orders",
        file_format="json",
    ),
)


def get_spark(profile: str | None = None) -> SparkSession:
    """Use the active Spark session in Jobs/Notebooks, or attach to Serverless over SSH."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        from databricks.connect import DatabricksSession
    
        builder = DatabricksSession.builder.serverless()
        if profile:
            builder = builder.profile(profile)
        return builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def ingest_entity(spark: SparkSession, catalog: str, entity: BronzeEntity) -> None:
    """Incrementally load one entity from raw.landing into bronze.<entity>."""
    source_path = f"/Volumes/{catalog}/raw/landing/{entity.name}"
    schema_location = f"/Volumes/{catalog}/raw/checkpoints/bronze/{entity.name}/schema"
    checkpoint_location = f"/Volumes/{catalog}/raw/checkpoints/bronze/{entity.name}/checkpoint"
    target_table = f"{catalog}.bronze.{entity.name}"

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", entity.file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
    )

    for opt_key, opt_val in entity.reader_options.items():
        reader = reader.option(opt_key, opt_val)

    raw_stream = reader.load(source_path).select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )

    query = (
        raw_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    total_rows = spark.table(target_table).count()
    rescued_rows = spark.table(target_table).filter(F.col("_rescued_data").isNotNull()).count()
    print(f"[OK] {target_table:<38} | total rows: {total_rows:>5} | rescued rows: {rescued_rows}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze Auto Loader ingestion.")
    parser.add_argument(
        "--catalog",
        type=str,
        default="ecommerce_lakehouse",
        help="Unity Catalog name (default: ecommerce_lakehouse)",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Optional ~/.databrickscfg profile name for Databricks Connect",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark(profile=args.profile)

    print(f"Starting Bronze Auto Loader ingestion for catalog: {args.catalog}")
    for entity in ENTITIES:
        ingest_entity(spark, args.catalog, entity)
    print("Bronze ingestion complete.")


if __name__ == "__main__":
    main()