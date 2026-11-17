with fingerprinted as (
    select * from {{ ref('int_taxi_trip_fingerprinted') }}
), duplicate_ranked as (
    select
        f.*,
        row_number() over (
            partition by source_file_sha256, raw_exact_fingerprint
            order by source_row_number
        ) as duplicate_occurrence_number,
        first_value(source_record_key) over (
            partition by source_file_sha256, raw_exact_fingerprint
            order by source_row_number
        ) as duplicate_survivor_source_record_key
    from fingerprinted f
), cross_file_raw as (
    select raw_exact_fingerprint
    from fingerprinted
    where raw_exact_fingerprint is not null
    group by raw_exact_fingerprint
    having count(distinct source_file_sha256) > 1
), cross_file_candidate as (
    select candidate_trip_fingerprint
    from fingerprinted
    group by candidate_trip_fingerprint
    having count(distinct source_file_sha256) > 1
), lookups as (
    select
        d.*,
        (pu.location_id is null or dz.location_id is null) as has_unmapped_zone,
        (v.vendor_id is null or p.payment_type_id is null or rc.rate_code_id is null) as has_unmapped_code,
        (cr.raw_exact_fingerprint is not null or cc.candidate_trip_fingerprint is not null) as has_cross_file_match
    from duplicate_ranked d
    left join {{ ref('stg_taxi_zone') }} pu on pu.location_id = d.pu_location_id
    left join {{ ref('stg_taxi_zone') }} dz on dz.location_id = d.do_location_id
    left join {{ ref('vendor_codes') }} v on v.vendor_id = d.vendor_id
    left join {{ ref('payment_type_codes') }} p on p.payment_type_id = d.payment_type_id
    left join {{ ref('rate_codes') }} rc on rc.rate_code_id = d.rate_code_id
    left join cross_file_raw cr on cr.raw_exact_fingerprint = d.raw_exact_fingerprint
    left join cross_file_candidate cc on cc.candidate_trip_fingerprint = d.candidate_trip_fingerprint
), rule_flags as (
    select
        l.*,
        (pickup_datetime is null or dropoff_datetime is null) as time_001,
        (dropoff_datetime <= pickup_datetime) as time_002,
        (duration_seconds > {{ var('duration_quarantine_minutes') }} * 60) as time_003,
        (duration_seconds > {{ var('duration_warning_minutes') }} * 60
            and duration_seconds <= {{ var('duration_quarantine_minutes') }} * 60) as time_004,
        (pickup_datetime is not null
            and to_char(pickup_datetime, 'YYYY-MM') <> source_year_month) as time_005,
        (trip_distance < 0) as dist_001,
        (trip_distance is null or trip_distance = 0) as dist_002,
        (row_speed_mph > {{ var('speed_warning_mph') }}
            and row_speed_mph <= {{ var('speed_quarantine_mph') }}) as speed_001,
        (row_speed_mph > {{ var('speed_quarantine_mph') }}) as speed_002,
        (passenger_count is null or passenger_count < 1
            or passenger_count > {{ var('passenger_kpi_max') }}) as pass_001,
        has_unmapped_zone as loc_001,
        (pu_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
            or do_location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})) as loc_002,
        has_unmapped_code as code_001,
        (source_store_and_fwd_flag is not null and store_and_fwd_flag is null) as code_002,
        (payment_type_id = {{ var('voided_payment_code') }}) as code_003,
        (reported_total_amount is null or reported_total_amount <= 0) as amt_001,
        (fare_amount < 0 or extra < 0 or mta_tax < 0 or tip_amount < 0
            or tolls_amount < 0 or improvement_surcharge < 0
            or congestion_surcharge < 0 or airport_fee < 0
            or cbd_congestion_fee < 0) as amt_002,
        (tip_amount < 0 or tip_amount > reported_total_amount) as amt_003,
        (tip_amount > 0 and payment_type_id is distinct from {{ var('recorded_card_payment_code') }}) as amt_004,
        (component_reconciliation_status = 'ROUNDING_WARNING') as amt_006,
        (component_reconciliation_status = 'REVIEW_WARNING') as amt_007,
        (component_reconciliation_status = 'NOT_RECONCILABLE') as amt_008,
        (airport_fee > 0 and (pu_location_id is null or pu_location_id not in
            ({{ var('airport_pickup_zone_ids') | join(', ') }}))) as fee_001,
        (cbd_congestion_fee > 0 and pickup_datetime < '{{ var('cbd_fee_effective_date') }}'::timestamp) as fee_002,
        (duplicate_occurrence_number > 1) as dup_001,
        has_cross_file_match as dup_002
    from lookups l
), grouped_rules as (
    select
        r.*,
        array_remove(array[
            case when time_001 then 'TIME-001' end,
            case when time_002 then 'TIME-002' end,
            case when time_003 then 'TIME-003' end,
            case when time_004 then 'TIME-004' end,
            case when time_005 then 'TIME-005' end,
            case when dist_001 then 'DIST-001' end,
            case when dist_002 then 'DIST-002' end,
            case when speed_001 then 'SPEED-001' end,
            case when speed_002 then 'SPEED-002' end,
            case when pass_001 then 'PASS-001' end,
            case when loc_001 then 'LOC-001' end,
            case when loc_002 then 'LOC-002' end,
            case when code_001 then 'CODE-001' end,
            case when code_002 then 'CODE-002' end,
            case when code_003 then 'CODE-003' end,
            case when amt_001 then 'AMT-001' end,
            case when amt_002 then 'AMT-002' end,
            case when amt_003 then 'AMT-003' end,
            case when amt_004 then 'AMT-004' end,
            case when amt_006 then 'AMT-006' end,
            case when amt_007 then 'AMT-007' end,
            case when amt_008 then 'AMT-008' end,
            case when fee_001 then 'FEE-001' end,
            case when fee_002 then 'FEE-002' end,
            case when dup_001 then 'DUP-001' end,
            case when dup_002 then 'DUP-002' end
        ], null)::text[] as dq_rule_ids
    from rule_flags r
), policy_applied as (
    select
        g.*,
        coalesce(a.has_duplicate_rule, false) as has_duplicate_rule,
        coalesce(a.has_business_quarantine_rule, false) as has_business_quarantine_rule,
        coalesce(a.has_warning_rule, false) as has_warning_rule,
        coalesce(a.has_unmapped_rule, false) as has_unmapped_rule,
        coalesce(a.max_severity, 'PASS') as max_severity
    from grouped_rules g
    cross join lateral (
        select
            bool_or(p.disposition_action = 'DUPLICATE') as has_duplicate_rule,
            bool_or(p.disposition_action = 'BUSINESS_QUARANTINE') as has_business_quarantine_rule,
            bool_or(p.disposition_action = 'WARNING') as has_warning_rule,
            bool_or(p.rule_id is null) as has_unmapped_rule,
            (array_agg(p.severity order by p.severity_rank desc))[1] as max_severity
        from unnest(g.dq_rule_ids) as rule(rule_id)
        left join {{ ref('dq_rule_policy') }} p on p.rule_id = rule.rule_id
    ) a
), dispositions as (
    select
        g.*,
        case
            when has_duplicate_rule then 'DUPLICATE'
            when has_business_quarantine_rule then 'BUSINESS_QUARANTINE'
            when has_warning_rule then 'WARNING'
            else 'ACCEPTED'
        end::varchar(24) as primary_disposition,
        max_severity as dq_severity
    from policy_applied g
)
select
    d.*,
    coalesce((primary_disposition in ('ACCEPTED', 'WARNING') and reported_total_amount > 0), false) as is_revenue_kpi_eligible,
    coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
        and payment_type_id = {{ var('recorded_card_payment_code') }}
        and fare_amount > 0 and tip_amount >= 0), false) as is_tip_kpi_eligible,
    coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
        and duration_seconds > 0 and duration_seconds <= {{ var('duration_warning_minutes') }} * 60
        and trip_distance > 0 and row_speed_mph <= {{ var('speed_warning_mph') }}), false) as is_speed_kpi_eligible,
    coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
        and duration_seconds > 0 and duration_seconds <= {{ var('duration_warning_minutes') }} * 60), false) as is_duration_kpi_eligible,
    coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
        and passenger_count between 1 and {{ var('passenger_kpi_max') }}), false) as is_passenger_kpi_eligible,
    case when primary_disposition in ('ACCEPTED', 'WARNING') and reported_total_amount > 0
        then reported_total_amount end::numeric(12,2) as revenue_amount,
    coalesce((amt_002 or amt_003), false) as reversal_or_adjustment_candidate
from dispositions d
