with championship as (
    select * from {{ ref('stg_championship_teams') }}
)

select
    session_key,
    meeting_key,
    team_name,
    points_current,
    points_start,
    championship_position,
    championship_position_start,
    year
from championship
