with starting_grid as (
    select * from {{ ref('stg_starting_grid') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
)

select
    sg.session_key,
    sg.driver_number,
    sg.grid_position,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name,
    s.meeting_name,
    s.session_name,
    s.year
from starting_grid sg
left join drivers d
    on  sg.session_key   = d.session_key
    and sg.driver_number = d.driver_number
left join sessions s
    on  sg.session_key   = s.session_key
