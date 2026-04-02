with overtakes as (
    select * from {{ ref('stg_overtakes') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    o.session_key,
    o.driver_overtaking,
    o.driver_overtaken,
    o.lap_number,
    o.recorded_at,
    d1.full_name    as driver_overtaking_name,
    d1.acronym      as driver_overtaking_acronym,
    d1.team_name    as team_overtaking,
    d2.full_name    as driver_overtaken_name,
    d2.acronym      as driver_overtaken_acronym,
    d2.team_name    as team_overtaken
from overtakes o
left join drivers d1
    on  o.session_key      = d1.session_key
    and o.driver_overtaking = d1.driver_number
left join drivers d2
    on  o.session_key      = d2.session_key
    and o.driver_overtaken  = d2.driver_number
