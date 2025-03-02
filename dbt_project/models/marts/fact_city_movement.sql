{{ config(
    materialized='incremental',
    unique_key='train_id'
) }}

with train_delay as (
    select tm.TRAIN_ID,
           DATE(tm.ACTUAL_TIMESTAMP) as Date,
           TIME(tm.ACTUAL_TIMESTAMP) as arrival_time,
           coalesce(sc.FULL_NAME, 'Ending Station') as current_station,
           TIME(tm.PLANNED_TIMESTAMP) as planned_arrival_time,
           coalesce(TIMESTAMPDIFF(minute, tm.PLANNED_TIMESTAMP, tm.ACTUAL_TIMESTAMP), 0) as delay_in_minutes,
           tm.PLANNED_EVENT_TYPE,
           tm.EVENT_TYPE,
           tm.PLATFORM,
           tm.DIRECTION_IND,
           tm.VARIATION_STATUS,
           coalesce(sc_next.FULL_NAME, 'UnKnown') as next_station,
           m.CITY as city,
           tm.LAST_UPDATED_DT,
           ROW_NUMBER() OVER (PARTITION BY tm.TRAIN_ID ORDER BY tm.ACTUAL_TIMESTAMP DESC) AS row_num
    from {{ ref('stg_train_movement') }} tm
             left join {{ ref('station_codes') }} sc on sc.STANOX_NO = tm.LOC_STANOX
             left join {{ ref('station_codes') }} sc_next on sc_next.STANOX_NO = tm.NEXT_REPORT_STANOX
             left join {{ source('train_data', 'mapping') }} m on sc.ROUTE_DESCRIPTION = m.ROUTE_DESCRIPTION
),
latest_train_data as (
    select *
    from train_delay
    where row_num = 1
    order by arrival_time
),
city_weather_data as (
    select ltd.*,
           wd.temperature
    from latest_train_data ltd
             left join {{ ref('stg_weather_data') }} wd on wd.CITY = ltd.CITY
),
max_last_updated_dt as (
    -- Calculate the max LAST_UPDATED_DT from the existing table
    select max(LAST_UPDATED_DT) as max_last_updated_dt
    from {{ this }}
)

select 
    TRAIN_ID,
    Date,
    ARRIVAL_TIME,
    CURRENT_STATION,
    planned_arrival_time,
    PLANNED_EVENT_TYPE,
    DIRECTION_IND,
    PLATFORM,
    VARIATION_STATUS,
    NEXT_STATION,
    delay_in_minutes,
    CITY,
    TEMPERATURE,
    LAST_UPDATED_DT
from 
    city_weather_data

{% if is_incremental() %}
-- Use the precomputed MAX(LAST_UPDATED_DT) from the CTE
where LAST_UPDATED_DT >= (select max_last_updated_dt from max_last_updated_dt)
{% endif %}
