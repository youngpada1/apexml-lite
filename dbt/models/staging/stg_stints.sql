with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'STINTS') }}
),

renamed as (
    select
        raw_data:session_key::integer       as session_key,
        raw_data:driver_number::integer     as driver_number,
        raw_data:stint_number::integer      as stint_number,
        raw_data:lap_start::integer         as lap_start,
        raw_data:lap_end::integer           as lap_end,
        raw_data:compound::string           as tyre_compound,
        raw_data:tyre_age_at_start::integer as tyre_age_at_start,
        loaded_at
    from source
    where raw_data:session_key is not null
      and raw_data:driver_number is not null
      and raw_data:compound is not null
)

select * from renamed
