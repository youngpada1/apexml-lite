with championship as (
    select * from {{ ref('stg_championship_drivers') }}
)

select
    driver_number,
    broadcast_name,
    team_name,
    points,
    championship_position,
    year
from championship
