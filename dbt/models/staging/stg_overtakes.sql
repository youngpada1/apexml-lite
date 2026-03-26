with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'OVERTAKES') }}
),

renamed as (
    select
        raw_data:session_key::integer           as session_key,
        raw_data:driver_number_overtaking::integer as driver_overtaking,
        raw_data:driver_number_overtaken::integer  as driver_overtaken,
        raw_data:lap_number::integer            as lap_number,
        raw_data:date::timestamp_ntz            as recorded_at,
        loaded_at
    from source
)

select * from renamed
