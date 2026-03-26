with positions as (
    select * from {{ ref('stg_position') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

result as (
    select * from {{ ref('stg_session_result') }}
),

grid as (
    select * from {{ ref('stg_starting_grid') }}
)

select
    p.session_key,
    p.driver_number,
    p.position,
    p.recorded_at,
    d.full_name         as driver_name,
    d.acronym           as driver_acronym,
    d.team_name,
    g.grid_position,
    r.finish_position,
    r.points,
    r.classified_position,
    r.finish_position - g.grid_position as positions_gained
from positions p
left join drivers d
    on  p.session_key   = d.session_key
    and p.driver_number = d.driver_number
left join grid g
    on  p.session_key   = g.session_key
    and p.driver_number = g.driver_number
left join result r
    on  p.session_key   = r.session_key
    and p.driver_number = r.driver_number
