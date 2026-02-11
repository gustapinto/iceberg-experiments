# Iceberg Experiments

Some data engineering experiments with [Apache Iceberg](https://iceberg.apache.org/) as the lakehouse table format

## Architectural components

This project uses the following services, as defined in the `docker-compose.yaml` file:
- [Trino](https://trino.io/) as query engine
- [Nessie](https://projectnessie.org/) as a Iceberg REST catalog with:
  - [PostgreSQL](https://www.postgresql.org/) as JDBC store
  - [MinIo](https://github.com/minio/minio) as S3-compatible storage layer

## Python

For the love of God in your soul, use [UV](https://docs.astral.sh/uv/) to run it or at least as a pip interface, or those damned rust dependencies will break every single thing in those scripts

- **pyiceberg-rest-catalog.py:** Example of schema/catalog setup with pyiceberg
- **pyiceberg-duckdb.py:** Example using DuckDB to insert and query data from a Iceberg Table. Depends on:
  - pyiceberg-rest-catalog.py