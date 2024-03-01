-- purchaseorder_transformation.sql

WITH purchaseorder_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'PURCHASEORDER') }}
),

final AS (
    SELECT
        po.value:C_Id::STRING AS customerid,
        po.value:C_CrIdentity::STRING AS crid
    FROM
        purchaseorder_data,
        LATERAL FLATTEN(input => D:results) AS po
)

SELECT * FROM final