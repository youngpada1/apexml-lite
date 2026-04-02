with location as (
    select * from {{ ref('stg_location') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    l.session_key,
    l.driver_number,
    l.recorded_at,
    l.pos_x,
    l.pos_y,
    l.pos_z,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name
from location l
left join drivers d
    on  l.session_key   = d.session_key
    and l.driver_number = d.driver_number
