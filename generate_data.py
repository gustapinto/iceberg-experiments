from datetime import datetime

from pyarrow import Table
from pyiceberg.catalog import Catalog
from pyiceberg.types import *
from pyiceberg.partitioning import *
from pyiceberg.catalog.rest import RestCatalog


TABLE_SPEC = Schema(
    NestedField(field_id=1, name="code", field_type=StringType(), required=False),
    NestedField(field_id=2, name="salesperson_code", field_type=StringType(), required=False),
    NestedField(field_id=3, name="product_code", field_type=StringType(), required=False),
    NestedField(field_id=4, name="value", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="ts", field_type=TimestampType(), required=False),
)
PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=5,
        field_id=1000,
        transform=DayTransform(),
        name="ts_day"
    ),
)
SAMPLE_DATA = Table.from_pylist(
    [
        {"code": "1234567890", "salesperson_code": "1234567890", "product_code": "1234567890", "value": 4.90, "ts": datetime.now()},
        {"code": "1234567891", "salesperson_code": "1234567891", "product_code": "1234567891", "value": 4.90, "ts": datetime.now()},
        {"code": "1234567892", "salesperson_code": "1234567892", "product_code": "1234567892", "value": 4.90, "ts": datetime.now()},
        {"code": "1234567893", "salesperson_code": "1234567893", "product_code": "1234567893", "value": 4.90, "ts": datetime.now()},
        {"code": "1234567894", "salesperson_code": "1234567894", "product_code": "1234567894", "value": 4.90, "ts": datetime.now()},
    ],
)


def main() -> None:
    catalog = RestCatalog(name="nessie_catalog",
                          uri="http://127.0.0.1:19120/iceberg/main",
                          **{
                            "s3.endpoint": "http://localhost:9000/",
                            "s3.access-key-id": "admin",
                            "s3.secret-access-key": "password",
                          })

    catalog.create_namespace_if_not_exists(namespace="nessie-test-ns-1")
    catalog.create_table_if_not_exists(identifier="nessie-test-ns-1.sale-products",
                                       location="s3://nessie-test-warehouse-1/tables/sale-products-1/",
                                       schema=TABLE_SPEC,
                                       partition_spec=PARTITION_SPEC)

    table = catalog.load_table(identifier="nessie-test-ns-1.sale-products")
    table.append(SAMPLE_DATA)


if __name__ == "__main__":
    main()
