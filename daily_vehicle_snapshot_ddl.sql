CREATE TABLE dott_vehicle.daily_vehicle_snapshot (
   vehicle_id varchar,
   last_updated_timestamp varchar,
   last_gps_timestamp varchar,
   is_charging varchar,
   battery_level integer,
   battery_dead_ind integer,
   battery_low_ind integer,
   battery_full_ind integer,
   days_since_last_gps_signal bigint,
   lost_vehicle integer,
   num_gps_satellites varchar,
   last_updated_date varchar,
   last_gps_date varchar,
   date_key varchar
)
WITH (
   format = 'RCBINARY',
   partitioned_by = ARRAY['date_key']
);