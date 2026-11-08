-- Two schemas (layers) inside the Iceberg catalog:
--   raw  : query events exactly as the Trino event listener produced them
--   meta : the Meta DWH model (dimensions, facts, daily aggregates)
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.meta;
