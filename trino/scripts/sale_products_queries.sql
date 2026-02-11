select
    code,
    salesperson_code,
    json_value(person, 'lax $.email') as salesperson_contact,
    product_code,
    format('%.2f', value) || ' BRL' as value,
    (value < 5) as is_fraud
from
    "nessie"."nessie-test-ns-1"."sale-products";
