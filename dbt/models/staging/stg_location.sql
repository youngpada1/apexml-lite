with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'LOCATION') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:x::float                   as pos_x,
        raw_data:y::float                   as pos_y,
        raw_data:z::float                   as pos_z,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:driver_number::integer, raw_data:date::timestamp_ntz
        order by loaded_at desc
    ) = 1
)

select * from renamed
