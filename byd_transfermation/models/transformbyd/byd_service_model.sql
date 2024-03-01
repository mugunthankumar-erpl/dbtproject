-- service_transformation.sql

WITH service_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'SERVICE') }}
),

final AS (
    SELECT
        ser.value:C_ServIntId::STRING AS serviceid,
        ser.value:T_DescContent::STRING AS descriptionContent
    FROM
        service_data,
        LATERAL FLATTEN(input => D:results) AS ser
)

SELECT * FROM final