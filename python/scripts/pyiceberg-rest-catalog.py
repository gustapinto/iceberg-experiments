from datetime import datetime
from json import load, dumps

from pyarrow import Table as ArrowTable
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable, DataScan
from pyiceberg.transforms import DayTransform
from pyiceberg.types import StringType, DoubleType, TimestampType, NestedField


TABLE_SPEC: Schema = Schema(
    NestedField(field_id=1, name="code", field_type=StringType(), required=False),
    NestedField(
        field_id=2, name="salesperson_code", field_type=StringType(), required=False
    ),
    NestedField(
        field_id=3, name="product_code", field_type=StringType(), required=False
    ),
    NestedField(field_id=4, name="value", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="ts", field_type=TimestampType(), required=False),
    NestedField(field_id=6, name="person", field_type=StringType(), required=False)
)
PARTITION_SPEC: PartitionSpec = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="ts_day"),
)

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

    # Criando um novo namespace/tabela Iceberg
    catalog.create_namespace_if_not_exists(namespace="nessie-test-ns-1")
    catalog.create_table_if_not_exists(
        identifier="nessie-test-ns-1.sale-products",
        location="s3://nessie-test-warehouse-1/tables/sale-products-1/",
        schema=TABLE_SPEC,
        partition_spec=PARTITION_SPEC,
    )

    # Usando uma tabela Iceberg já existente
    table: IcebergTable = catalog.load_table(
        identifier="nessie-test-ns-1.sale-products"
    )

    # Populando tabela com dados mockados
    with open("datasets/sale-products.json") as r:
        def _dataset_mapper(d):
            return {
                **d,
                "person": dumps(d.get("person")),
                "ts": datetime.now(),
            }

        dataset: list[dict] = list(map(_dataset_mapper, load(r)))

        # Montando ArrowTable com base em uma lista
        arrow_dataset: ArrowTable = ArrowTable.from_pylist(dataset)

        # Sobreescrevendo dados na tabela
        table.overwrite(arrow_dataset)

    # Consultando dados da tabela
    scan: DataScan = table.scan(row_filter="value > 5.0", limit=10)
    df: ArrowTable = scan.to_arrow()

    print(df)


if __name__ == "__main__":
    main()
