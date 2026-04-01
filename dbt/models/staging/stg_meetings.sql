with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'MEETINGS') }}
),

renamed as (
    select
        raw_data:meeting_key::integer          as meeting_key,
        raw_data:meeting_name::string          as meeting_name,
        raw_data:meeting_official_name::string as meeting_official_name,
        raw_data:circuit_key::integer          as circuit_key,
        raw_data:circuit_short_name::string    as circuit_short_name,
        raw_data:country_name::string          as country_name,
        raw_data:location::string              as location,
        raw_data:year::integer                 as year,
        raw_data:date_start::timestamp_ntz     as meeting_start_at,
        loaded_at
    from source
    qualify row_number() over (partition by raw_data:meeting_key::integer order by loaded_at desc) = 1
)

select * from renamed
