with raw_rows as (
    select
        r.source_record_key,
        coalesce(c.source_file_sha256, t.source_file_sha256) as source_file_sha256,
        coalesce(c.source_file, t.source_file) as source_file,
        case when jsonb_typeof(to_jsonb(r) -> 'raw_payload') = 'object'
            then to_jsonb(r) -> 'raw_payload'
            else to_jsonb(r)
        end as raw_values
    from {{ source('stage', 'yellow_taxi_raw') }} r
    left join {{ source('stage', 'yellow_taxi_canonical') }} c
        on c.source_record_key = r.source_record_key
    left join {{ source('mage_dq', 'technical_quarantine') }} t
        on t.source_record_key = r.source_record_key
), raw_cast as (
    select
        source_file_sha256,
        source_file,
        case when pg_input_is_valid(raw_values ->> 'total_amount', 'numeric(12,2)')
            then (raw_values ->> 'total_amount')::numeric(12,2)
        end as amount_eligible
    from raw_rows
), technical_cast as (
    select
        source_file_sha256,
        source_file,
        case when pg_input_is_valid(total_amount_raw, 'numeric(12,2)')
            then total_amount_raw::numeric(12,2)
        end as amount_eligible
    from {{ source('mage_dq', 'technical_quarantine') }}
), raw_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(amount_eligible) as amount_eligible_rows,
           coalesce(sum(amount_eligible), 0)::numeric as amount_sum
    from raw_cast group by 1, 2
), canonical_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(total_amount) as amount_eligible_rows,
           coalesce(sum(total_amount), 0)::numeric as amount_sum
    from {{ source('stage', 'yellow_taxi_canonical') }} group by 1, 2
), technical_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(amount_eligible) as amount_eligible_rows,
           coalesce(sum(amount_eligible), 0)::numeric as amount_sum
    from technical_cast group by 1, 2
), fact_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(total_amount) as amount_eligible_rows,
           coalesce(sum(total_amount), 0)::numeric as amount_sum
    from {{ ref('fact_taxi_trip') }} group by 1, 2
), business_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(total_amount) as amount_eligible_rows,
           coalesce(sum(total_amount), 0)::numeric as amount_sum
    from {{ ref('business_quarantine') }} group by 1, 2
), duplicate_agg as (
    select source_file_sha256, source_file,
           count(*) as row_count,
           count(total_amount) as amount_eligible_rows,
           coalesce(sum(total_amount), 0)::numeric as amount_sum
    from {{ ref('duplicate_quarantine') }} group by 1, 2
), file_keys as (
    select source_file_sha256, source_file from raw_agg
    union select source_file_sha256, source_file from canonical_agg
    union select source_file_sha256, source_file from technical_agg
    union select source_file_sha256, source_file from fact_agg
    union select source_file_sha256, source_file from business_agg
    union select source_file_sha256, source_file from duplicate_agg
), amounts as (
    select
        k.source_file_sha256,
        k.source_file,
        coalesce(r.row_count, 0) as raw_rows,
        coalesce(c.row_count, 0) as canonical_rows,
        coalesce(t.row_count, 0) as technical_rows,
        coalesce(f.row_count, 0) as fact_rows,
        coalesce(b.row_count, 0) as business_rows,
        coalesce(d.row_count, 0) as duplicate_rows,
        coalesce(r.amount_eligible_rows, 0) as raw_amount_eligible_rows,
        coalesce(c.amount_eligible_rows, 0) as canonical_amount_eligible_rows,
        coalesce(t.amount_eligible_rows, 0) as technical_amount_eligible_rows,
        coalesce(f.amount_eligible_rows, 0) as fact_amount_eligible_rows,
        coalesce(b.amount_eligible_rows, 0) as business_amount_eligible_rows,
        coalesce(d.amount_eligible_rows, 0) as duplicate_amount_eligible_rows,
        coalesce(r.amount_sum, 0) as raw_amount,
        coalesce(c.amount_sum, 0) as canonical_amount,
        coalesce(t.amount_sum, 0) as technical_amount,
        coalesce(f.amount_sum, 0) as fact_amount,
        coalesce(b.amount_sum, 0) as business_amount,
        coalesce(d.amount_sum, 0) as duplicate_amount
    from file_keys k
    left join raw_agg r on r.source_file_sha256 is not distinct from k.source_file_sha256
        and r.source_file is not distinct from k.source_file
    left join canonical_agg c on c.source_file_sha256 is not distinct from k.source_file_sha256
        and c.source_file is not distinct from k.source_file
    left join technical_agg t on t.source_file_sha256 is not distinct from k.source_file_sha256
        and t.source_file is not distinct from k.source_file
    left join fact_agg f on f.source_file_sha256 is not distinct from k.source_file_sha256
        and f.source_file is not distinct from k.source_file
    left join business_agg b on b.source_file_sha256 is not distinct from k.source_file_sha256
        and b.source_file is not distinct from k.source_file
    left join duplicate_agg d on d.source_file_sha256 is not distinct from k.source_file_sha256
        and d.source_file is not distinct from k.source_file
)
select
    a.*,
    raw_rows - canonical_rows - technical_rows as source_row_delta,
    canonical_rows - fact_rows - business_rows - duplicate_rows as trusted_row_delta,
    raw_amount_eligible_rows - canonical_amount_eligible_rows
        - technical_amount_eligible_rows as source_amount_eligible_row_delta,
    canonical_amount_eligible_rows - fact_amount_eligible_rows
        - business_amount_eligible_rows - duplicate_amount_eligible_rows
        as trusted_amount_eligible_row_delta,
    raw_amount - canonical_amount - technical_amount as source_amount_delta,
    canonical_amount - fact_amount - business_amount - duplicate_amount
        as trusted_amount_delta
from amounts a
