with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'SESSION_RESULT') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:position::integer          as finish_position,
        raw_data:points::float              as points,
        raw_data:classified_position::string as classified_position,
        loaded_at
    from source
)

select * from renamed
