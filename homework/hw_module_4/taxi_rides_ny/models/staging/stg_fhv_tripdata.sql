with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as varchar) as dispatching_base_num,
        cast(affiliated_base_number as varchar) as affiliated_base_number,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        {{ safe_cast('sr_flag', 'integer') }} as sr_flag

    from source
    -- FHV data has no vendor_id; filter out rows with no pickup time
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
