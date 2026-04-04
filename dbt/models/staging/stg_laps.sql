with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'LAPS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:lap_number::integer        as lap_number,
        raw_data:lap_duration::float        as lap_duration_s,
        raw_data:duration_sector_1::float   as sector_1_s,
        raw_data:duration_sector_2::float   as sector_2_s,
        raw_data:duration_sector_3::float   as sector_3_s,
        raw_data:i1_speed::float            as speed_trap_i1_kmh,
        raw_data:i2_speed::float            as speed_trap_i2_kmh,
        raw_data:st_speed::float            as speed_trap_fl_kmh,
        raw_data:is_pit_out_lap::boolean    as is_pit_out_lap,
        raw_data:date_start::timestamp_ntz  as lap_start_at,
        loaded_at
    from source
    where raw_data:lap_duration is not null
      and raw_data:lap_duration::float is not null
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:driver_number::integer, raw_data:lap_number::integer
        order by loaded_at desc
    ) = 1
)

select * from renamed
