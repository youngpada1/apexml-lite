with stints as (
    select * from {{ ref('stg_stints') }}
),

clean_laps as (
    select
        session_key,
        driver_number,
        lap_number,
        lap_duration_s
    from {{ ref('stg_laps') }}
    where is_pit_out_lap = false
      and lap_duration_s < 120
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
        s.lap_end - s.lap_start + 1                                 as stint_length,
        count(cl.lap_number)                                        as clean_laps,
        min(cl.lap_duration_s)                                      as fastest_lap_s,
        avg(cl.lap_duration_s)                                      as avg_lap_time_s,
        avg(cl.lap_duration_s) - min(cl.lap_duration_s)             as gap_to_best_s,
        round(
            (max(case when cl.lap_number = s.lap_end   then cl.lap_duration_s end)
           - min(case when cl.lap_number = s.lap_start then cl.lap_duration_s end))
            / nullif(s.lap_end - s.lap_start, 0), 3
        )                                                           as deg_per_lap_s,
        round(regr_slope(cl.lap_duration_s, cl.lap_number), 3)      as deg_slope_s
    from stints s
    left join clean_laps cl
        on  s.session_key   = cl.session_key
        and s.driver_number = cl.driver_number
        and cl.lap_number   between s.lap_start and s.lap_end
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
