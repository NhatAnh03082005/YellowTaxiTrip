\set ON_ERROR_STOP on

GRANT USAGE, CREATE ON SCHEMA stage, control, dq, dds, olap TO nyc_taxi;

ALTER DEFAULT PRIVILEGES FOR ROLE nyc_taxi IN SCHEMA stage, control, dq, dds, olap
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO nyc_taxi;

ALTER DEFAULT PRIVILEGES FOR ROLE nyc_taxi IN SCHEMA stage, control, dq, dds, olap
GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO nyc_taxi;
