with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'CAR_DATA') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:speed::integer             as speed_kmh,
        raw_data:throttle::integer          as throttle_pct,
        raw_data:brake::boolean             as is_braking,
        raw_data:drs::integer               as drs_status,
        raw_data:n_gear::integer            as gear,
        raw_data:rpm::integer               as rpm,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:driver_number::integer, raw_data:date::timestamp_ntz
        order by loaded_at desc
    ) = 1
)

select * from renamed
