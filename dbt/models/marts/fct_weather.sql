with weather as (
    select * from {{ ref('stg_weather') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
)

select
    w.session_key,
    w.recorded_at,
    w.air_temp_c,
    w.track_temp_c,
    w.humidity_pct,
    w.pressure_mbar,
    w.wind_speed_ms,
    w.wind_direction_deg,
    w.is_raining,
    s.session_name,
    s.circuit_short_name,
    s.country_name,
    s.session_start_at
from weather w
left join sessions s
    on w.session_key = s.session_key
