INSERT INTO
dott_vehicle.daily_vehicle_snapshot
(vehicle_id, last_updated_timestamp, last_gps_timestamp, is_charging, battery_level, battery_dead_ind, battery_low_ind, battery_full_ind, days_since_last_gps_signal, lost_vehicle, num_gps_satellites, last_updated_date, last_gps_date, date_key)
WITH
  curr_base_pre AS (
   SELECT
     vehicle_id
   , time_updated
   , time_gps
   , is_charging
   , battery_level
   , num_gps_satellites
   , updated_date
   , gps_date
   , date_key
   , row_number() OVER (PARTITION BY vehicle_id, gps_date ORDER BY time_gps DESC) rn
   FROM
     (
      SELECT
        vehicle_id
      , time_updated
      , time_gps
      , is_charging
      , CAST(battery_level AS int) battery_level
      , num_gps_satellites
      , date_format(date_parse(substr(time_updated, 1, 19), '%Y-%m-%d %H:%i:%s'), '%Y%m%d') updated_date
      , date_format(date_parse(substr(time_gps, 1, 19), '%Y-%m-%d %H:%i:%s'), '%Y%m%d') gps_date
      , '20210101' date_key
      FROM
        staging.tbl_telemetry
      WHERE (date_format(date_parse(substr(time_gps, 1, 19), '%Y-%m-%d %H:%i:%s'), '%Y%m%d') = {var_d1})
   ) 
) 
SELECT
  vehicle_id
, time_updated last_updated_timestamp
, time_gps last_gps_timestamp
, is_charging
, battery_level
, (CASE WHEN (battery_level = 0) THEN 1 ELSE 0 END) battery_dead_ind
, (CASE WHEN (battery_level <= 20) THEN 1 ELSE 0 END) battery_low_ind
, (CASE WHEN (battery_level = 100) THEN 1 ELSE 0 END) battery_full_ind
, date_diff('day', date_parse(gps_date, '%Y%m%d'), date_parse(date_key, '%Y%m%d')) days_since_last_gps_signal
, (CASE WHEN (date_diff('day', date_parse(gps_date, '%Y%m%d'), date_parse(date_key, '%Y%m%d')) >= 7) THEN 1 ELSE 0 END) lost_vehicle
, num_gps_satellites
, updated_date last_updated_date
, gps_date last_gps_date
, date_key
FROM
  curr_base_pre
WHERE (rn = 1);