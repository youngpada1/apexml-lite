with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'TEAM_RADIO') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:recording_url::string      as recording_url,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
)

select * from renamed
