with results as (
    select * from {{ ref('stg_session_result') }}
),

grid as (
    select * from {{ ref('stg_starting_grid') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

session_to_quali as (
    select
        race.session_key        as race_session_key,
        quali.session_key       as quali_session_key
    from {{ ref('stg_sessions') }} race
    left join {{ ref('stg_sessions') }} quali
        on  race.meeting_key    = quali.meeting_key
        and quali.session_type  in ('Qualifying', 'Shootout')
    where race.session_type = 'Race'
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
left join session_to_quali sq
    on  r.session_key   = sq.race_session_key
left join grid g
    on  sq.quali_session_key = g.session_key
    and r.driver_number      = g.driver_number
left join drivers d
    on  r.session_key   = d.session_key
    and r.driver_number = d.driver_number
