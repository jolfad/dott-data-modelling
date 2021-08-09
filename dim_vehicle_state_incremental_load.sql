START TRANSACTION;
DELETE FROM dott_vehicle.dim_vehicle_state 
INSERT INTO 
dott_vehicle.dim_vehicle_state 
(vehicle_id, start_date, end_date, hardware_generation, city_name,
country_name, is_deployed, is_in_warehouse, is_broken, new_deploy_ind, new_broken_ind, new_warehouse_ind, snapshot_timestamp)
WITH
  incremental_base AS (
   SELECT
     vehicle_id
   , time_updated
   , hardware_generation
   , city_name
   , country_name
   , is_deployed
   , is_in_warehouse
   , is_broken
   FROM
     staging.tbl_states
   WHERE (time_updated > (SELECT COALESCE(max(snapshot_timestamp), '0')
FROM
  dott_vehicle.dim_vehicle_state
))
UNION ALL    
  SELECT
     vehicle_id
   , start_date time_updated
   , hardware_generation
   , city_name
   , country_name
   , is_deployed
   , is_in_warehouse
   , is_broken
   FROM
     dott_vehicle.dim_vehicle_state
) 
SELECT
  vehicle_id
, time_updated start_date
, COALESCE(lead(time_updated, 1) OVER (PARTITION BY vehicle_id ORDER BY time_updated), 'N/A') end_date
, hardware_generation
, city_name
, country_name
, is_deployed
, is_in_warehouse
, is_broken
, new_deploy_ind
, new_broken_ind
, new_warehouse_ind
, snapshot_timestamp
FROM
  (
   SELECT
     vehicle_id
   , time_updated
   , hardware_generation
   , city_name
   , country_name
   , is_deployed
   , is_in_warehouse
   , is_broken
   , (CASE WHEN (((previous_deploy_status = 'false') OR (previous_deploy_status IS NULL)) AND (is_deployed = 'true')) THEN 'Y' ELSE 'N' END) new_deploy_ind
   , (CASE WHEN (((previous_broken_status = 'false') OR (previous_broken_status IS NULL)) AND (is_broken = 'true')) THEN 'Y' ELSE 'N' END) new_broken_ind
   , (CASE WHEN (((previous_warehouse_status = 'false') OR (previous_warehouse_status IS NULL)) AND (is_in_warehouse = 'true')) THEN 'Y' ELSE 'N' END) new_warehouse_ind
   , snapshot_timestamp
   FROM
     (
      SELECT
        vehicle_id
      , time_updated
      , hardware_generation
      , city_name
      , country_name
      , lag(is_deployed, 1) OVER (PARTITION BY vehicle_id ORDER BY time_updated) previous_deploy_status
      , is_deployed
      , lag(is_in_warehouse, 1) OVER (PARTITION BY vehicle_id ORDER BY time_updated) previous_warehouse_status
      , is_in_warehouse
      , lag(is_broken, 1) OVER (PARTITION BY vehicle_id ORDER BY time_updated) previous_broken_status
      , is_broken
      , max(time_updated) OVER () snapshot_timestamp
      FROM
        incremental_base
      WHERE (is_deployed <> is_in_warehouse)
   ) 
   WHERE ((previous_deploy_status <> is_deployed) OR (previous_warehouse_status <> is_in_warehouse) OR (previous_broken_status <> is_broken) OR (previous_deploy_status IS NULL))
);
COMMIT;
