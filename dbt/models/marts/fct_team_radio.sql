with team_radio as (
    select * from {{ ref('stg_team_radio') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
)

select
    tr.session_key,
    tr.driver_number,
    tr.recording_url,
    tr.recorded_at,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name,
    s.meeting_name,
    s.session_name,
    s.year
from team_radio tr
left join drivers d
    on  tr.session_key   = d.session_key
    and tr.driver_number = d.driver_number
left join sessions s
    on  tr.session_key   = s.session_key
