{# Config block at beginning to handle incremental loads #}

{{ 
    config(
    materialized='incremental',
    unique_key='call_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    ) 
}}

with source as (
    select * 
    from {{ source('ingest_calls', 'calls') }}
    {# In the incremental block, can add a lookback window that will subtract x days from the start date #}
    {% if is_incremental() %}
        where start_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
  )

{ with latest_records AS (
  SELECT call_id, agent_id, customer_id, queue_hold_time, start_ts, end_ts, 
duration_s, hold_time_during_call_s, transfer_flag, NOW() as warehouse_updated_ts, row_number() OVER (PARTITION BY <call_id> ORDER BY dlt.inserted_at DESC) 
  FROM SOURCE S INNER JOIN _dlt_loads ON _dlt_load_id = load_id 
  )

SELECT *
FROM latest_records
WHERE row_number = 1 
    }

select
    call_id
    ,agent_id
    ,customer_id
    ,queue_hold_time
    ,start_ts
    ,end_ts
    ,duration_s
    ,hold_time_during_call_s
    ,transfer_flag
    ,NOW() as warehouse_updated_ts

    from source
