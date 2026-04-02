with intervals as (
    select * from {{ ref('stg_intervals') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
)

select
    i.session_key,
    i.driver_number,
    i.gap_to_leader_s,
    i.interval_to_ahead_s,
    i.recorded_at,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name
from intervals i
left join drivers d
    on  i.session_key   = d.session_key
    and i.driver_number = d.driver_number
