{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}
select *,
    current_timestamp() as processed_at
from {{
    source('walmart_databricks', 'employees')
}}


{% if is_incremental() %}
    where updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}