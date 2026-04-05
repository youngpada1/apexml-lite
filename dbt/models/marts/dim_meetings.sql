with meetings as (
    select * from {{ ref('stg_meetings') }}
)

select
    meeting_key,
    meeting_name,
    meeting_official_name,
    circuit_key,
    circuit_short_name,
    country_name,
    location,
    year,
    meeting_start_at
from meetings
