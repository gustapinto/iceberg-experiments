# Iceberg Experiments

## Python

For the love of God in your soul, use [UV](https://docs.astral.sh/uv/) to run it or at least as a pip interface, or those damned rust dependencies will break every single thing in those scripts

- **pyiceberg-rest-catalog.py:** Example of schema/catalog setup with pyiceberg
- **pyiceberg-duckdb.py:** Example using DuckDB to insert and query data from a Iceberg Table. Depends on:
  - pyiceberg-rest-catalog.py