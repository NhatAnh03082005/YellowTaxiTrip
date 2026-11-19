{{ config(materialized='view', schema='audit', enabled=var('enable_fingerprint_history_candidate', false), tags=['fingerprint_candidate']) }}

with match_members as (
    select m.fingerprint_type, m.fingerprint, member.file_version_id
    from {{ source('audit', 'cross_file_fingerprint_match') }} m
    cross join lateral (values (m.first_file_version_id), (m.matched_file_version_id)) member(file_version_id)
    where m.first_file_version_id <> m.matched_file_version_id
)
select h.source_record_key, h.file_version_id, h.source_row_number,
       h.primary_disposition, 'DUP-002'::text as dq_rule_id
from {{ ref('v_trip_fingerprint_history') }} h
where exists (
    select 1 from match_members m
    where m.file_version_id = h.file_version_id
      and ((m.fingerprint_type = 'RAW_EXACT' and m.fingerprint = h.raw_exact_fingerprint)
        or (m.fingerprint_type = 'CANDIDATE' and m.fingerprint = h.candidate_trip_fingerprint))
)
