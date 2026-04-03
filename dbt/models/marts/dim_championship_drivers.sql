with championship as (
    select * from {{ ref('stg_championship_drivers') }}
)

select
    session_key,
    meeting_key,
    driver_number,
    points_current,
    points_start,
    championship_position,
    championship_position_start,
    year
from championship
