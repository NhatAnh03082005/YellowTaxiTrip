with violations as (
    select 'vendor_expected_codes_repeated' as issue where
        (select count(distinct code) from unnest(array[{{ var('expected_vendor_codes') | join(', ') }}]::smallint[]) as codes(code))
        <> {{ var('expected_vendor_codes') | length }}
    union all
    select 'payment_expected_codes_repeated' where
        (select count(distinct code) from unnest(array[{{ var('expected_payment_type_codes') | join(', ') }}]::smallint[]) as codes(code))
        <> {{ var('expected_payment_type_codes') | length }}
    union all
    select 'rate_expected_codes_repeated' where
        (select count(distinct code) from unnest(array[{{ var('expected_rate_codes') | join(', ') }}]::smallint[]) as codes(code))
        <> {{ var('expected_rate_codes') | length }}
    union all
    select 'vendor_code_count' where (select count(*) from {{ ref('vendor_codes') }}) <> {{ var('expected_vendor_codes') | length }}
    union all
    select 'payment_code_count' where (select count(*) from {{ ref('payment_type_codes') }}) <> {{ var('expected_payment_type_codes') | length }}
    union all
    select 'rate_code_count' where (select count(*) from {{ ref('rate_codes') }}) <> {{ var('expected_rate_codes') | length }}
    union all
    select 'zone_id_count' where (select count(*) from {{ ref('zone_key_map') }}) <> {{ var('zone_lookup_expected_count') }}
    union all
    select 'vendor_code_set' where exists (
        select expected.code from unnest(array[{{ var('expected_vendor_codes') | join(', ') }}]::smallint[]) as expected(code)
        except select vendor_id from {{ ref('vendor_codes') }}
    )
    union all
    select 'payment_code_set' where exists (
        select expected.code from unnest(array[{{ var('expected_payment_type_codes') | join(', ') }}]::smallint[]) as expected(code)
        except select payment_type_id from {{ ref('payment_type_codes') }}
    )
    union all
    select 'rate_code_set' where exists (
        select expected.code from unnest(array[{{ var('expected_rate_codes') | join(', ') }}]::smallint[]) as expected(code)
        except select rate_code_id from {{ ref('rate_codes') }}
    )
    union all
    select 'zone_id_set' where exists (
        select generate_series(1, {{ var('zone_lookup_expected_count') }})
        except select location_id from {{ ref('zone_key_map') }}
    )
    union all
    select 'non_positive_surrogate' where
        exists (select 1 from {{ ref('vendor_codes') }} where vendor_key <= 0)
        or exists (select 1 from {{ ref('payment_type_codes') }} where payment_type_key <= 0)
        or exists (select 1 from {{ ref('rate_codes') }} where rate_code_key <= 0)
        or exists (select 1 from {{ ref('zone_key_map') }} where zone_key <= 0)
    union all
    select 'flex_fare_payment_missing' where not exists
        (select 1 from {{ ref('payment_type_codes') }} where payment_type_id = {{ var('flex_fare_payment_code') }} and payment_type_key > 0)
    union all
    select 'payment_unknown_missing' where not exists
        (select 1 from {{ ref('payment_type_codes') }} where payment_type_id = {{ var('business_unknown_payment_code') }} and payment_type_key > 0)
    union all
    select 'payment_voided_missing' where not exists
        (select 1 from {{ ref('payment_type_codes') }} where payment_type_id = {{ var('voided_payment_code') }} and payment_type_key > 0)
    union all
    select 'rate_unknown_missing' where not exists
        (select 1 from {{ ref('rate_codes') }} where rate_code_id = {{ var('business_unknown_rate_code') }} and rate_code_key > 0)
    union all
    select 'non_geometric_zone_missing' where exists (
        select expected.location_id from unnest(array[{{ var('non_geometric_zone_ids') | join(', ') }}]::smallint[]) as expected(location_id)
        except select location_id from {{ ref('zone_key_map') }} where zone_key > 0
    )
)
select * from violations
