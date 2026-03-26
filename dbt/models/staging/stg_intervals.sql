with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'INTERVALS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:gap_to_leader::float       as gap_to_leader_s,
        raw_data:interval::float            as interval_to_ahead_s,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
)

select * from renamed
