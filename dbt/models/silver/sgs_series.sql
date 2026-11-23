{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_file_path'
  )
}}

-- A rewritten year file replaces all of its rows (unique_key is the file), so
-- dates that BACEN removes in a revision disappear from silver too.
with newest_per_file as (
    select *
    from {{ source('bronze', 'sgs_raw') }}
    {% if is_incremental() %}
    where source_file_modification_time
        > (select max(source_file_modification_time) from {{ this }})
    {% endif %}
    qualify row_number() over (
        partition by source_file_path
        order by source_file_modification_time desc, cast(ingested_at as timestamp) desc
    ) = 1
),

exploded as (
    select
        series_code,
        source_file_path,
        source_file_modification_time,
        cast(ingested_at as timestamp) as ingested_at,
        explode(from_json(payload, 'array<struct<data:string,valor:string>>')) as record
    from newest_per_file
)

select
    series_code,
    to_date(record.data, 'dd/MM/yyyy') as date,
    cast(record.valor as decimal(18, 6)) as valor,
    source_file_path,
    source_file_modification_time,
    ingested_at
from exploded
