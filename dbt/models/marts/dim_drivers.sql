with drivers as (
    select * from {{ ref('stg_drivers') }}
),

-- Keep the most recent record per driver per session
deduped as (
    select *,
        row_number() over (
            partition by session_key, driver_number
            order by loaded_at desc
        ) as rn
    from drivers
)

select
    session_key,
    driver_number,
    broadcast_name,
    full_name,
    acronym,
    team_name,
    team_colour,
    country_code,
    headshot_url
from deduped
where rn = 1
