with championship as (
    select * from {{ ref('stg_championship_teams') }}
)

select
    team_name,
    points,
    championship_position,
    year
from championship
