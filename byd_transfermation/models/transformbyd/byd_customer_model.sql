-- customer_transformation.sql

WITH customer_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'CUSTOMER') }}
),

final AS (
    SELECT
        cust.value:C_BpIntId::STRING AS customerid,
        cust.value:C_OrgName::STRING AS customerorgName,
        cust.value:C_CityName::STRING AS cityName
    FROM
        customer_data,
        LATERAL FLATTEN(input => D:results) AS cust
)

SELECT * FROM final