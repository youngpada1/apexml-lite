with laps as (
    select * from {{ ref('stg_laps') }}
),

stints as (
    select * from {{ ref('stg_stints') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

-- Join laps with stints to get tyre context per lap
laps_with_stints as (
    select
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_duration_s,
        l.sector_1_s,
        l.sector_2_s,
        l.sector_3_s,
        l.speed_trap_i1_kmh,
        l.speed_trap_i2_kmh,
        l.speed_trap_fl_kmh,
        l.is_pit_out_lap,
        l.lap_start_at,
        s.tyre_compound,
        s.tyre_age_at_start,
        (l.lap_number - s.lap_start + s.tyre_age_at_start) as tyre_age_on_lap
    from laps l
    left join stints s
        on  l.session_key   = s.session_key
        and l.driver_number = s.driver_number
        and l.lap_number    between s.lap_start and s.lap_end
)

select
    lws.*,
    d.full_name         as driver_name,
    d.acronym           as driver_acronym,
    d.team_name
from laps_with_stints lws
left join drivers d
    on  lws.session_key   = d.session_key
    and lws.driver_number = d.driver_number
