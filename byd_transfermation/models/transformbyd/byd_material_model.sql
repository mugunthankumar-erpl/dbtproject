-- material_transformation.sql

WITH material_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'MATERIAL') }}
),

final AS (
    SELECT
        matrl.value:C_MatrIntId::STRING AS materialid,
        matrl.value:T_DescContent::STRING AS description,
        matrl.value:C_BaseMsrUnit::STRING AS unit
    FROM
        material_data,
        LATERAL FLATTEN(input => D:results) AS matrl
)

SELECT * FROM final