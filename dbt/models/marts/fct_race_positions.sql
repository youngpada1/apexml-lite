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
),

-- Map race session_key to its qualifying session_key via meeting_key
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
left join session_to_quali sq
    on  p.session_key   = sq.race_session_key
left join grid g
    on  sq.quali_session_key = g.session_key
    and p.driver_number      = g.driver_number
left join result r
    on  p.session_key   = r.session_key
    and p.driver_number = r.driver_number
