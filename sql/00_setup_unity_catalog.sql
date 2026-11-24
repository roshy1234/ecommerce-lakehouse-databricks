-- 1. Create a dedicated catalog for this project
CREATE CATALOG IF NOT EXISTS ecommerce_lakehouse
COMMENT 'End-to-end E-Commerce Medallion Lakehouse project';

USE CATALOG ecommerce_lakehouse;

-- 2. Create schemas for raw volumes and each Medallion layer
CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Managed volumes for incoming raw files and Auto Loader metadata';

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingested Delta tables with minimal transformation';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, deduplicated, quarantine-checked, and SCD Type 2 Delta tables';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-level star schema (facts and dimensions) for analytics';

-- 3. Create managed Volumes for raw files and streaming checkpoints
CREATE VOLUME IF NOT EXISTS ecommerce_lakehouse.raw.landing
COMMENT 'Landing zone for incremental orders, customers, and products batches';

CREATE VOLUME IF NOT EXISTS ecommerce_lakehouse.raw.checkpoints
COMMENT 'Auto Loader schema tracking and Structured Streaming checkpoints';