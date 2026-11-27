{{ config(tags=['minimal_batch']) }}
-- Configuration is changeable but must remain internally consistent.
select 1
where not (
    '{{ var('cbd_fee_effective_date') }}'::date >= ('{{ var('cbd_schema_start_month') }}-01')::date
    and {{ var('duration_warning_minutes') }} > 0
    and {{ var('duration_quarantine_minutes') }} > {{ var('duration_warning_minutes') }}
    and {{ var('duration_quarantine_minutes') }}::bigint * 60 <= 2147483647
    and {{ var('speed_warning_mph') }} > 0
    and {{ var('speed_quarantine_mph') }} > {{ var('speed_warning_mph') }}
    and {{ var('passenger_kpi_max') }} >= 1
    and {{ var('passenger_group_single_size') }} >= 1
    and {{ var('passenger_group_middle_max') }} > {{ var('passenger_group_single_size') }}
    and {{ var('passenger_group_middle_max') }} < {{ var('passenger_kpi_max') }}
    and {{ var('component_match_tolerance') }} >= 0
    and {{ var('component_rounding_tolerance') }} >= {{ var('component_match_tolerance') }}
    and {{ var('amount_reconciliation_tolerance') }} >= 0
    and {{ var('distance_band_edges_miles')[0] }} > 0
    and {{ var('distance_band_edges_miles')[1] }} > {{ var('distance_band_edges_miles')[0] }}
    and {{ var('distance_band_edges_miles')[2] }} > {{ var('distance_band_edges_miles')[1] }}
    and {{ var('time_band_start_hours')[0] }} >= 0
    and {{ var('time_band_start_hours')[1] }} > {{ var('time_band_start_hours')[0] }}
    and {{ var('time_band_start_hours')[2] }} > {{ var('time_band_start_hours')[1] }}
    and {{ var('time_band_start_hours')[3] }} > {{ var('time_band_start_hours')[2] }}
    and {{ var('time_band_start_hours')[3] }} <= 23
)
