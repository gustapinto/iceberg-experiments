from duckdb import connect, DuckDBPyRelation
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table as IcebergTable, DataScan


# OBS: Criar tabelas e carregar dados no Iceberg usando o script "pyiceberg-rest-catalog.py"
def main() -> None:
    # Se conectando a um catálogo Iceberg REST
    catalog: Catalog = RestCatalog(
        name="nessie_catalog",
        uri="http://127.0.0.1:19120/iceberg/main",
        **{
            "s3.endpoint": "http://localhost:9000/",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        }
    )

    # Usando uma tabela Iceberg já existente usandp o formato (<namespace>, <tabela>)
    table: IcebergTable = catalog.load_table(("nessie-test-ns-1", "sale-products"))

    # Consultando dados da tabela
    scan: DataScan = table.scan()

    # Abrindo uma conexão DuckDB
    with connect(database=":memory:") as conn:
        # Manipulando os dados da tabela usando DuckDB
        scan.to_duckdb(table_name="sale_products", connection=conn)

        r1: DuckDBPyRelation = conn.sql(
            """
            SELECT
                code,
                (CASE
                    WHEN value <= 5 THEN 'LOW'
                    WHEN value > 5 AND value <= 10 THEN 'MEDIUM'
                    ELSE 'HIGH'
                END) AS "value_tier",
                salesperson_code,
                product_code,
                value,
                ts,
                CURRENT_TIMESTAMP as "classification_ts"
            FROM
                sale_products
            """
        )
        r1.show()


if __name__ == "__main__":
    main()
