WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'orders') }}
)
SELECT
    orderid AS order_id
    ,customerid AS customer_id
    ,employeeid::CHAR AS employee_id
    ,orderdate AS order_date
    ,requireddate AS required_date
    ,shippeddate AS shipped_date
    ,shipvia AS ship_via
    ,shipcity AS ship_city
    ,shipcountry AS ship_country
FROM source_data