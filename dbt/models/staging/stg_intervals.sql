with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'INTERVALS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        try_cast(raw_data:gap_to_leader::string as float)      as gap_to_leader_s,
        try_cast(raw_data:interval::string as float)            as interval_to_ahead_s,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:driver_number::integer, raw_data:date::timestamp_ntz
        order by loaded_at desc
    ) = 1
)

select * from renamed
