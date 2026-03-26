with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'CHAMPIONSHIP_TEAMS') }}
),

renamed as (
    select
        raw_data:team_name::string          as team_name,
        raw_data:points::float              as points,
        raw_data:position::integer          as championship_position,
        raw_data:year::integer              as year,
        loaded_at
    from source
)

select * from renamed
