with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'WEATHER') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:air_temperature::float     as air_temp_c,
        raw_data:track_temperature::float   as track_temp_c,
        raw_data:humidity::float            as humidity_pct,
        raw_data:pressure::float            as pressure_mbar,
        raw_data:wind_speed::float          as wind_speed_ms,
        raw_data:wind_direction::integer    as wind_direction_deg,
        raw_data:rainfall::boolean          as is_raining,
        raw_data:date::timestamp_ntz        as recorded_at,
        loaded_at
    from source
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:date::timestamp_ntz
        order by loaded_at desc
    ) = 1
)

select * from renamed
