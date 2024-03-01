-- material_transformation.sql

WITH employee_data AS (
    SELECT * FROM {{ source('BYD_DATA_DB', 'EMPLOYEE') }}
),

final AS (
    SELECT
        emp.value:C_EeId::STRING AS employeeid,
        emp.value:C_EeGivenName::STRING AS employeeName,
        emp.value:C_WrkadrsEmail::STRING AS email
    FROM
        employee_data,
        LATERAL FLATTEN(input => D:results) AS emp
)

SELECT * FROM final