with pit as (
    select * from {{ ref('stg_pit') }}
),

stints as (
    select * from {{ ref('stg_stints') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

pit_with_tyres as (
    select
        p.session_key,
        p.driver_number,
        p.lap_number,
        p.pit_duration_s,
        p.pit_at,
        s_before.tyre_compound  as tyre_compound_out,
        s_after.tyre_compound   as tyre_compound_in,
        s_before.stint_number   as stint_number
    from pit p
    left join stints s_before
        on  p.session_key   = s_before.session_key
        and p.driver_number = s_before.driver_number
        and p.lap_number    = s_before.lap_end
    left join stints s_after
        on  p.session_key   = s_after.session_key
        and p.driver_number = s_after.driver_number
        and p.lap_number + 1 = s_after.lap_start
)

select
    pwt.*,
    d.full_name     as driver_name,
    d.acronym       as driver_acronym,
    d.team_name
from pit_with_tyres pwt
left join drivers d
    on  pwt.session_key   = d.session_key
    and pwt.driver_number = d.driver_number
qualify row_number() over (
    partition by pwt.session_key, pwt.driver_number, pwt.lap_number
    order by pwt.stint_number
) = 1
