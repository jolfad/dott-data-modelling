CREATE TABLE dott_vehicle.dim_vehicle_state (
   vehicle_id varchar,
   start_date varchar,
   end_date varchar,
   hardware_generation varchar,
   city_name varchar,
   country_name varchar,
   is_deployed varchar,
   is_in_warehouse varchar,
   is_broken varchar,
   new_deploy_ind varchar,
   new_broken_ind varchar,
   new_warehouse_ind varchar,
   snapshot_timestamp varchar
)
WITH (
   format = 'RCBINARY'
);