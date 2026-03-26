with stints as (
    select * from {{ ref('stg_stints') }}
),

laps as (
    select * from {{ ref('stg_laps') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

stint_laps as (
    select
        s.session_key,
        s.driver_number,
        s.stint_number,
        s.tyre_compound,
        s.tyre_age_at_start,
        s.lap_start,
        s.lap_end,
        s.lap_end - s.lap_start + 1                     as stint_length,
        avg(l.lap_duration_s)                           as avg_lap_time_s,
        min(l.lap_duration_s)                           as fastest_lap_s,
        max(l.lap_duration_s)                           as slowest_lap_s,
        -- Degradation: difference between last and first lap of stint
        max(case when l.lap_number = s.lap_end   then l.lap_duration_s end)
      - min(case when l.lap_number = s.lap_start then l.lap_duration_s end)
                                                        as lap_time_delta_s
    from stints s
    left join laps l
        on  s.session_key   = l.session_key
        and s.driver_number = l.driver_number
        and l.lap_number    between s.lap_start and s.lap_end
        and l.is_pit_out_lap = false
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    sl.*,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name
from stint_laps sl
left join drivers d
    on  sl.session_key   = d.session_key
    and sl.driver_number = d.driver_number
