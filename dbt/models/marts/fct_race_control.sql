with race_control as (
    select * from {{ ref('stg_race_control') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    rc.session_key,
    rc.recorded_at,
    rc.category,
    rc.flag,
    rc.lap_number,
    rc.message,
    rc.scope,
    rc.sector,
    rc.driver_number,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    s.session_name,
    s.circuit_short_name,
    s.country_name
from race_control rc
left join sessions s
    on rc.session_key = s.session_key
left join drivers d
    on  rc.session_key   = d.session_key
    and rc.driver_number = d.driver_number
