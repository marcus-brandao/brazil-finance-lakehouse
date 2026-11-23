-- Mirrors the reconciliation in docs/bronze.md: rows in silver for each file
-- equal the payload length of the newest bronze row for that file.
with newest as (
    select
        source_file_path,
        json_array_length(payload) as records_in_bronze
    from {{ source('bronze', 'sgs_raw') }}
    qualify row_number() over (
        partition by source_file_path
        order by source_file_modification_time desc, cast(ingested_at as timestamp) desc
    ) = 1
),

silver as (
    select source_file_path, count(*) as records_in_silver
    from {{ ref('sgs_series') }}
    group by source_file_path
)

select n.source_file_path, n.records_in_bronze, coalesce(s.records_in_silver, 0) as records_in_silver
from newest n
left join silver s using (source_file_path)
where n.records_in_bronze <> coalesce(s.records_in_silver, 0)
