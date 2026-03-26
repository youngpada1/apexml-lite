with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'PIT') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:lap_number::integer        as lap_number,
        raw_data:pit_duration::float        as pit_duration_s,
        raw_data:date::timestamp_ntz        as pit_at,
        loaded_at
    from source
)

select * from renamed
