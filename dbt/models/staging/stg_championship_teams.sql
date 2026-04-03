with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'CHAMPIONSHIP_TEAMS') }}
),

sessions as (
    select session_key, year
    from {{ ref('stg_sessions') }}
),

renamed as (
    select
        s.raw_data:session_key::integer       as session_key,
        s.raw_data:meeting_key::integer       as meeting_key,
        s.raw_data:team_name::string          as team_name,
        s.raw_data:points_current::float      as points_current,
        s.raw_data:points_start::float        as points_start,
        s.raw_data:position_current::integer  as championship_position,
        s.raw_data:position_start::integer    as championship_position_start,
        sess.year,
        s.loaded_at
    from source s
    left join sessions sess
        on s.raw_data:session_key::integer = sess.session_key
    where s.raw_data:team_name is not null
      and s.raw_data:session_key is not null
    qualify row_number() over (
        partition by s.raw_data:session_key::integer, s.raw_data:team_name::string
        order by s.loaded_at desc
    ) = 1
)

select * from renamed
