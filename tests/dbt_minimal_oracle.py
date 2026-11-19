"""Independent, bounded specification oracle for T02 fixtures; never runtime code.

No production SQL/Mage imports. Decimal arithmetic and literal expected outputs
are checked separately. This is not a substitute for executing the future dbt SQL.
"""
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib

D = Decimal
COMPONENTS = ('fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
              'improvement_surcharge', 'congestion_surcharge', 'airport_fee')
RAW_FIELDS = [('VendorID', 'int32'), ('tpep_pickup_datetime', 'timestamp[us]'),
              ('tpep_dropoff_datetime', 'timestamp[us]'), ('passenger_count', 'int64'),
              ('trip_distance', 'double'), ('RatecodeID', 'int64'),
              ('store_and_fwd_flag', 'large_string'), ('PULocationID', 'int32'),
              ('DOLocationID', 'int32'), ('payment_type', 'int64'),
              ('fare_amount', 'double'), ('extra', 'double'), ('mta_tax', 'double'),
              ('tip_amount', 'double'), ('tolls_amount', 'double'),
              ('improvement_surcharge', 'double'), ('total_amount', 'double'),
              ('congestion_surcharge', 'double'), ('Airport_fee', 'double')]
HARD = {'TIME-001', 'TIME-002', 'TIME-003', 'DIST-001', 'SPEED-002', 'CODE-003',
        'AMT-001', 'AMT-002', 'AMT-003', 'FEE-002'}


def source_key(sha, number):
    return hashlib.sha256(f'{sha.lower()}:{number}'.encode()).hexdigest()


def digest_fields(values, prefix=''):
    parts = [prefix]
    for ordinal, (value, tag) in enumerate(values, 1):
        token = 'N;' if value is None else f'V{len(value.encode("utf-8"))}:{value};'
        parts.append(f'{ordinal}:{tag}:{token}')
    return hashlib.sha256(''.join(parts).encode()).hexdigest()


def raw_hash(raw, schema_fingerprint, has_cbd):
    fields = RAW_FIELDS + ([('cbd_congestion_fee', 'double')] if has_cbd else [])
    return digest_fields([(raw.get(k, raw.get('airport_fee')) if k == 'Airport_fee'
                           else raw.get(k), t) for k, t in fields], schema_fingerprint)


def numeric(raw, key, places=2):
    value = raw.get(key)
    if value is None:
        return None
    try:
        number = D(value)
        if not number.is_finite():
            return None
        return number.quantize(D(1).scaleb(-places), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def integer(raw, key):
    try:
        value = D(raw[key])
        return int(value) if value.is_finite() and value == int(value) else None
    except (InvalidOperation, ValueError, TypeError, KeyError):
        return None


def timestamp(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def classify_file(file, known_zones):
    """Fixture contract subset, including all legacy business-rule predicates."""
    expected_schema = 'yt_schema_v002' if file['has_cbd'] else 'yt_schema_v001'
    if file['schema_version'] != expected_schema:
        raise ValueError('Unknown/inconsistent fixture schema contract')
    numbers = [r['source_row_number'] for r in file['rows']]
    if len(numbers) != len(set(numbers)) or any(n < 1 for n in numbers):
        raise ValueError('Duplicate/nonpositive source row number')
    for row in file['rows']:
        required = {k for k, _ in RAW_FIELDS} - {'Airport_fee'}
        if (not required <= set(row['raw']) or
                not {'Airport_fee', 'airport_fee'} & set(row['raw']) or
                ('cbd_congestion_fee' in row['raw']) != file['has_cbd']):
            raise ValueError('Missing/inconsistent raw hash input contract')
    result = []
    for item in file['rows']:
        raw = item['raw']; rules = []
        pickup = timestamp(raw.get('tpep_pickup_datetime'))
        dropoff = timestamp(raw.get('tpep_dropoff_datetime'))
        pu = integer(raw, 'PULocationID'); dz = integer(raw, 'DOLocationID')
        normalized = dict(raw, airport_fee=raw.get('Airport_fee', raw.get('airport_fee')))
        money = {k: numeric(normalized, k) for k in (*COMPONENTS, 'total_amount', 'cbd_congestion_fee')}
        if pickup is None or dropoff is None: rules.append('CAST-001')
        if pu is None or dz is None: rules.append('CAST-002')
        if money['total_amount'] is None: rules.append('CAST-003')
        base = {'case_id': item['case_id'], 'file_version_id': file['file_version_id'],
                'source_row_number': item['source_row_number'],
                'source_record_key': source_key(file['sha256'], item['source_row_number']),
                'fare': money['fare_amount'], 'tip': money['tip_amount'],
                'total': money['total_amount'], 'revenue': None,
                'eligible': dict.fromkeys(('revenue', 'tip', 'speed', 'duration', 'passenger'), False),
                'has_in_file_candidate_match': False}
        if rules:
            result.append(dict(base, disposition='TECHNICAL_REJECT', rules=sorted(rules),
                               raw_hash=None, candidate=None, component_status=None))
            continue
        seconds = int((dropoff - pickup).total_seconds())
        distance = numeric(raw, 'trip_distance', 3)
        speed = ((distance * 3600 / seconds).quantize(D('.01'), rounding=ROUND_HALF_UP)
                 if seconds > 0 and distance is not None and distance > 0 else None)
        passenger = integer(raw, 'passenger_count'); vendor = integer(raw, 'VendorID')
        payment = integer(raw, 'payment_type'); rate = integer(raw, 'RatecodeID')
        parts = [money[k] for k in COMPONENTS]
        if file['has_cbd']: parts.append(money['cbd_congestion_fee'])
        calc = sum(parts, D(0)) if all(v is not None for v in parts) else None
        delta = abs(money['total_amount'] - calc) if calc is not None else None
        status = ('NOT_RECONCILABLE' if delta is None else 'MATCHED' if delta <= D('.01')
                  else 'ROUNDING_WARNING' if delta <= D('.05') else 'REVIEW_WARNING')
        flags = {
            'TIME-002': seconds <= 0, 'TIME-003': seconds > 86400,
            'TIME-004': 10800 < seconds <= 86400,
            'TIME-005': pickup.strftime('%Y-%m') != file['source_year_month'],
            'DIST-001': distance is not None and distance < 0,
            'DIST-002': distance is None or distance == 0,
            'SPEED-001': speed is not None and 45 < speed <= 80,
            'SPEED-002': speed is not None and speed > 80,
            'PASS-001': passenger is None or passenger < 1 or passenger > 6,
            'LOC-001': pu not in known_zones or dz not in known_zones,
            'LOC-002': pu in (264, 265) or dz in (264, 265),
            'CODE-001': vendor not in (1, 2, 6, 7) or payment not in range(7) or rate not in (1, 2, 3, 4, 5, 6, 99),
            'CODE-002': raw.get('store_and_fwd_flag') is not None and raw['store_and_fwd_flag'].strip().upper() not in ('Y', 'N'),
            'CODE-003': payment == 6,
            'AMT-001': money['total_amount'] <= 0,
            'AMT-002': any(v is not None and v < 0 for v in [*parts, money['cbd_congestion_fee']]),
            'AMT-003': money['tip_amount'] is not None and (money['tip_amount'] < 0 or money['tip_amount'] > money['total_amount']),
            'AMT-004': money['tip_amount'] is not None and money['tip_amount'] > 0 and payment != 1,
            'AMT-006': status == 'ROUNDING_WARNING', 'AMT-007': status == 'REVIEW_WARNING',
            'AMT-008': status == 'NOT_RECONCILABLE',
            'FEE-001': money['airport_fee'] is not None and money['airport_fee'] > 0 and pu not in (132, 138),
            'FEE-002': money['cbd_congestion_fee'] is not None and money['cbd_congestion_fee'] > 0 and pickup < datetime(2025, 1, 5),
        }
        rules = sorted(k for k,v in flags.items() if v)
        candidate = (vendor, pickup, dropoff, pu, dz, passenger, distance, money['total_amount'])
        result.append(dict(base, rules=rules, raw_hash=raw_hash(raw, file['schema_fingerprint'], file['has_cbd']),
                           candidate=candidate, component_status=status, duration_seconds=seconds,
                           distance=distance, speed=speed, passenger=passenger, payment=payment))
    groups = defaultdict(list); candidates = Counter()
    for row in result:
        if row['raw_hash'] is not None:
            groups[row['raw_hash']].append(row); candidates[row['candidate']] += 1
    for group in groups.values():
        group.sort(key=lambda r: r['source_row_number'])
        for occurrence, row in enumerate(group, 1):
            row['survivor_source_record_key'] = group[0]['source_record_key']
            row['duplicate_occurrence_number'] = occurrence
            if occurrence > 1: row['rules'] = sorted(row['rules'] + ['DUP-001'])
    for row in result:
        if row.get('disposition') == 'TECHNICAL_REJECT': continue
        rules=set(row['rules'])
        disposition = ('DUPLICATE' if 'DUP-001' in rules else 'BUSINESS_QUARANTINE' if rules & HARD
                       else 'WARNING' if rules - {'LOC-002'} else 'ACCEPTED')
        row['disposition'] = disposition
        row['has_in_file_candidate_match'] = candidates[row['candidate']] > 1
        trusted = disposition in ('ACCEPTED', 'WARNING')
        row['eligible'] = {
            'revenue': trusted and row['total'] > 0,
            'tip': trusted and row['payment'] == 1 and row['fare'] is not None and row['fare'] > 0 and row['tip'] is not None and row['tip'] >= 0,
            'speed': trusted and 0 < row['duration_seconds'] <= 10800 and row['distance'] is not None and row['distance'] > 0 and row['speed'] is not None and row['speed'] <= 45,
            'duration': trusted and 0 < row['duration_seconds'] <= 10800,
            'passenger': trusted and row['passenger'] is not None and 1 <= row['passenger'] <= 6,
        }
        row['revenue'] = row['total'] if row['eligible']['revenue'] else None
    return result


def summarize(rows):
    counts = Counter(r['disposition'] for r in rows)
    measures = {}
    for field in ('fare', 'tip', 'total'):
        measures[field] = {}
        for branch in ('ALL', 'TECHNICAL_REJECT', 'DUPLICATE', 'BUSINESS_QUARANTINE', 'TRUSTED'):
            selected = [r for r in rows if branch == 'ALL' or r['disposition'] == branch or
                        (branch == 'TRUSTED' and r['disposition'] in ('ACCEPTED', 'WARNING'))]
            values = [r[field] for r in selected if r[field] is not None]
            measures[field][branch] = {'eligible_count': len(values), 'sum': str(sum(values, D('0.00')))}
    sums = {key: D('0.00') for key in ('revenue', 'tip_numerator', 'tip_denominator', 'speed_distance')}
    sums.update(duration_sum=0, duration_count=0, speed_seconds=0, passenger_sum=0, passenger_count=0)
    for r in rows:
        e=r['eligible']
        if e['revenue']: sums['revenue'] += r['revenue']
        if e['tip']: sums['tip_numerator'] += r['tip']; sums['tip_denominator'] += r['fare']
        if e['speed']: sums['speed_distance'] += r['distance']; sums['speed_seconds'] += r['duration_seconds']
        if e['duration']: sums['duration_sum'] += r['duration_seconds']; sums['duration_count'] += 1
        if e['passenger']: sums['passenger_sum'] += r['passenger']; sums['passenger_count'] += 1
    return {'raw_rows':len(rows), 'canonical_rows':len(rows)-counts['TECHNICAL_REJECT'],
            'fact_rows':counts['ACCEPTED']+counts['WARNING'],
            'technical_rows':counts['TECHNICAL_REJECT'], 'business_rows':counts['BUSINESS_QUARANTINE'],
            'duplicate_rows':counts['DUPLICATE'],
            'candidate_rows':sum(r['has_in_file_candidate_match'] for r in rows),
            'measures':measures, 'kpi':{k:str(v) if isinstance(v,D) else v for k,v in sums.items()}}
