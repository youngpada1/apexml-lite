with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'CHAMPIONSHIP_DRIVERS') }}
),

renamed as (
    select
        raw_data:driver_number::integer     as driver_number,
        raw_data:broadcast_name::string     as broadcast_name,
        raw_data:team_name::string          as team_name,
        raw_data:points::float              as points,
        raw_data:position::integer          as championship_position,
        raw_data:year::integer              as year,
        loaded_at
    from source
    where raw_data:driver_number is not null
      and raw_data:year is not null
    qualify row_number() over (partition by raw_data:driver_number::integer, raw_data:year::integer order by loaded_at desc) = 1
)

select * from renamed
