{{ config(tags=['minimal_batch']) }}
-- Serialization must preserve null/empty, ordinal, type, numeric precision,
-- timestamp microseconds, and schema identity before SHA-256 is applied.
with vectors as (
    select
        {{ fingerprint_field('null::text', 1, 'text') }} as null_value,
        {{ fingerprint_field("''::text", 1, 'text') }} as empty_value,
        {{ fingerprint_field("'A'::text", 1, 'text') }} as first_ordinal,
        {{ fingerprint_field("'A'::text", 2, 'text') }} as second_ordinal,
        {{ fingerprint_field("'A'::text", 1, 'other_type') }} as other_type,
        {{ fingerprint_field("'1.000000001'::text", 1, 'double') }} as first_precision,
        {{ fingerprint_field("'1.000000002'::text", 1, 'double') }} as second_precision,
        {{ fingerprint_field("timestamp '2025-05-01 00:00:00.000001'", 1, 'timestamp[us]') }} as first_timestamp,
        {{ fingerprint_field("timestamp '2025-05-01 00:00:00.000002'", 1, 'timestamp[us]') }} as second_timestamp
)
select 1
from vectors
where null_value = empty_value
   or first_ordinal = second_ordinal
   or first_ordinal = other_type
   or first_precision = second_precision
   or first_timestamp = second_timestamp
   or encode(digest('schema_a' || first_ordinal, 'sha256'), 'hex')
      = encode(digest('schema_b' || first_ordinal, 'sha256'), 'hex')
