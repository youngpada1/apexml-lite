with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'SESSIONS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:session_name::string       as session_name,
        raw_data:session_type::string       as session_type,
        raw_data:meeting_key::integer       as meeting_key,
        raw_data:circuit_key::integer       as circuit_key,
        raw_data:circuit_short_name::string as circuit_short_name,
        raw_data:country_name::string       as country_name,
        raw_data:location::string           as location,
        raw_data:year::integer              as year,
        raw_data:date_start::timestamp_ntz  as session_start_at,
        raw_data:date_end::timestamp_ntz    as session_end_at,
        loaded_at
    from source
)

select * from renamed
