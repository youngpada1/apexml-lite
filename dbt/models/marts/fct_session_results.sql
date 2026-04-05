with results as (
    select * from {{ ref('stg_session_result') }}
),

grid as (
    select
        driver_number,
        grid_position,
        meeting_key
    from {{ ref('stg_starting_grid') }}
    qualify row_number() over (
        partition by meeting_key, driver_number
        order by session_key asc
    ) = 1
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

sessions as (
    select session_key, meeting_key
    from {{ ref('stg_sessions') }}
)

select
    r.session_key,
    r.driver_number,
    r.finish_position,
    r.points,
    r.is_dnf,
    r.is_dns,
    r.is_dsq,
    r.laps_completed,
    r.race_duration_s,
    r.gap_to_leader,
    case
        when r.is_dsq = true                                                 then 'DSQ'
        when r.is_dns = true                                                 then 'DNS'
        when r.is_dnf = true                                                 then 'DNF'
        when r.finish_position is null and coalesce(r.laps_completed, 0) > 0 then 'DNF'
        when r.finish_position is null                                        then 'DNS'
        else 'Classified'
    end                                  as classified_position,
    g.grid_position,
    r.finish_position - g.grid_position  as positions_gained,
    d.full_name                          as driver_name,
    d.acronym                            as driver_acronym,
    d.team_name
from results r
left join sessions s
    on  r.session_key   = s.session_key
left join grid g
    on  s.meeting_key   = g.meeting_key
    and r.driver_number = g.driver_number
left join drivers d
    on  r.session_key   = d.session_key
    and r.driver_number = d.driver_number
