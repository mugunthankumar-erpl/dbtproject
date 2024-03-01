-- salesorder_transformation.sql

WITH salesorder_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'SALESORDER') }}
),

final AS (
    SELECT
        so.value:C_DocId::STRING AS salesid,
        so.value:T_DocName::STRING AS docName
    FROM
        salesorder_data,
        LATERAL FLATTEN(input => D:results) AS so
)

SELECT * FROM final