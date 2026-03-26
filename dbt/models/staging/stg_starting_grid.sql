with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'STARTING_GRID') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:position::integer          as grid_position,
        loaded_at
    from source
)

select * from renamed
