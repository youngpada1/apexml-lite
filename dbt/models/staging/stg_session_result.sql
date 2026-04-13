with source as (
    select raw_data, loaded_at
    from {{ source('raw', 'SESSION_RESULT') }}
),

renamed as (
    select
        raw_data:session_key::integer        as session_key,
        raw_data:driver_number::integer      as driver_number,
        raw_data:position::integer           as finish_position,
        as_double(raw_data:points)           as points,
        raw_data:classified_position::string as classified_position,
        raw_data:dnf::boolean                as is_dnf,
        raw_data:dns::boolean                as is_dns,
        raw_data:dsq::boolean                as is_dsq,
        raw_data:number_of_laps::integer     as laps_completed,
        as_double(raw_data:duration)         as race_duration_s,
        raw_data:gap_to_leader::string       as gap_to_leader,
        loaded_at
    from source
    qualify row_number() over (
        partition by raw_data:session_key::integer, raw_data:driver_number::integer
        order by loaded_at desc
    ) = 1
)

select * from renamed
