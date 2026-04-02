with results as (
    select * from {{ ref('stg_session_result') }}
),

grid as (
    select * from {{ ref('stg_starting_grid') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    r.session_key,
    r.driver_number,
    r.finish_position,
    r.points,
    r.classified_position,
    g.grid_position,
    r.finish_position - g.grid_position  as positions_gained,
    d.full_name                          as driver_name,
    d.acronym                            as driver_acronym,
    d.team_name
from results r
left join grid g
    on  r.session_key   = g.session_key
    and r.driver_number = g.driver_number
left join drivers d
    on  r.session_key   = d.session_key
    and r.driver_number = d.driver_number
