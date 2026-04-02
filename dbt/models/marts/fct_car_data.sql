with car_data as (
    select * from {{ ref('stg_car_data') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    c.session_key,
    c.driver_number,
    c.recorded_at,
    c.speed_kmh,
    c.throttle_pct,
    c.is_braking,
    c.drs_status,
    c.gear,
    c.rpm,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name
from car_data c
left join drivers d
    on  c.session_key   = d.session_key
    and c.driver_number = d.driver_number
