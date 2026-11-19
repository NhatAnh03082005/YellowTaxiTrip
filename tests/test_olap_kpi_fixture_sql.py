"""FIN-12 ratio-of-sums KPI fixture against both changed OLAP models."""
from pathlib import Path
import subprocess
import unittest

import jinja2


class OlapKpiFixtureSQL(unittest.TestCase):
    def test_speed_and_tip_rate_follow_contract(self):
        root = Path(__file__).resolve().parents[1]
        env = jinja2.Environment()
        env.globals.update(config=lambda **kwargs: '', ref=lambda name: 'pg_temp.' + name,
                           var=lambda name, default=None: default)

        def render(name):
            path = root / 'dbt_project/models/marts/olap' / name
            return env.from_string(path.read_text(encoding='utf-8')).render()

        daily = render('olap_daily_kpi.sql')
        monthly = render('olap_monthly_zone_kpi.sql')
        sql = r"""
BEGIN;
CREATE TEMP TABLE stg_yellow_taxi(file_version_id bigint);
CREATE TEMP TABLE fact_taxi_trip(
 file_version_id bigint, pickup_date_key integer, source_year_month text,
 pickup_zone_key integer, vendor_key integer, payment_type_key integer, rate_code_key integer,
 passenger_count integer, revenue_amount numeric, tip_amount numeric, fare_amount numeric,
 trip_distance_miles numeric, duration_seconds numeric,
 is_passenger_kpi_eligible boolean, is_revenue_kpi_eligible boolean,
 is_tip_kpi_eligible boolean, is_speed_kpi_eligible boolean, is_duration_kpi_eligible boolean
);
INSERT INTO stg_yellow_taxi VALUES (1);
INSERT INTO fact_taxi_trip VALUES
 (1,20240501,'2024-05',10,1,1,1,1,12,2,10,10,3600,true,true,true,true,true),
 (1,20240501,'2024-05',10,1,1,1,2,6,1,5,1,60,true,true,true,true,true),
 (2,20240501,'2024-05',10,1,1,1,3,999,99,99,99,1,true,true,true,true,true);
CREATE TEMP VIEW daily AS
__DAILY__;
CREATE TEMP VIEW monthly AS
__MONTHLY__;
DO $$ DECLARE expected_speed numeric := 11 * 3600.0 / 3660; BEGIN
  IF (SELECT trip_count FROM daily) <> 2
     OR abs((SELECT average_speed_mph FROM daily) - expected_speed) > 0.000001
     OR abs((SELECT average_speed_mph FROM monthly) - expected_speed) > 0.000001
     OR (SELECT recorded_card_tip_rate FROM monthly) <> 0.200000
     OR (SELECT total_revenue FROM monthly) <> 18
  THEN RAISE EXCEPTION 'OLAP KPI fixture mismatch'; END IF;
END $$;
ROLLBACK;
""".replace('__DAILY__', daily).replace('__MONTHLY__', monthly)
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
