with sessions as (
    select * from {{ ref('stg_sessions') }}
),

meetings as (
    select * from {{ ref('stg_meetings') }}
),

joined as (
    select
        s.session_key,
        s.session_name,
        s.session_type,
        s.circuit_short_name,
        s.country_name,
        s.location,
        s.year,
        s.session_start_at,
        s.session_end_at,
        m.meeting_name,
        m.meeting_official_name
    from sessions s
    left join meetings m on s.meeting_key = m.meeting_key
)

select * from joined
