with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'DRIVERS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:broadcast_name::string     as broadcast_name,
        raw_data:full_name::string          as full_name,
        raw_data:name_acronym::string       as acronym,
        raw_data:team_name::string          as team_name,
        raw_data:team_colour::string        as team_colour,
        raw_data:country_code::string       as country_code,
        raw_data:headshot_url::string       as headshot_url,
        loaded_at
    from source
)

select * from renamed
