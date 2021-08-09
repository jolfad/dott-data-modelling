# STEPS ON LOADING THE TWO CSV FILES USED FOR THE TEST

# THIS EXERCISE USES HDFS, HIVE AND PRESTO

## LOADING data_modelling_test_tbl_telemetry.csv

### STEP 1 (PUT FILE FROM LOCAL DIRECTORY INTO HDFS)

*hdfs dfs -put -f /home/user/data_modelling_test_tbl_telemetry.csv /user/telemetry/*


### STEP 2 (CREATE AN EXTERNAL TABLE ON HIVE TO POINT TO THE HDFS DIRECTORY IN STEP 1)
#### RUN ON HIVE

*CREATE EXTERNAL TABLE dott_staging.tbl_telemetry_ext (*  
*vehicle_id string*  
*time_updated string ,*  
*time_gps string ,*  
*is_charging string ,*  
*battery_level string ,*  
*num_gps_satellites string*  
*) row format delimited fields terminated by ',' location '/user/telemetry/' tblproperties ("skip.header.line.count"="1") ;* 


### STEP 3 (CREATE A TABLE THAT PUTS THE DATA IN THE EXTERNAL TABLE IN STEP 2 INTO A PROPER TABLE ON PRESTO)
#### RUN ON PRESTO

*CREATE TABLE dott_staging.tbl_telemetry*  
*as*  
*select  
*trim(cast(vehicle_id as varchar)) vehicle_id,*  
*trim(cast(time_updated as varchar)) time_updated,*  
*trim(cast(time_gps as varchar)) time_gps,*  
*trim(cast(is_charging as varchar)) is_charging,*  
*trim(cast(battery_level as varchar)) battery_level,*  
*trim(cast(num_gps_satellites as varchar)) num_gps_satellites*  
*from dott_staging.tbl_telemetry_ext;*         



## LOADING data_modelling_test_tbl_states.csv

### STEP 1 (PUT FILE FROM LOCAL DIRECTORY INTO HDFS)

*hdfs dfs -put -f /home/user/data_modelling_test_tbl_states.csv /user/states/*


### STEP 2 (CREATE AN EXTERNAL TABLE ON HIVE TO POINT TO THE HDFS DIRECTORY IN STEP 1)
#### RUN ON HIVE

*CREATE EXTERNAL TABLE dott_staging.tbl_states_ext (*  
*vehicle_id string,*  
*time_updated string ,*  
*hardware_generation string ,*  
*city_name string ,*  
*country_name string ,*  
*is_deployed string ,*  
*is_in_warehouse string ,*  
*is_broken string*   
*) row format delimited fields terminated by ',' location '/user/states/' tblproperties ("skip.header.line.count"="1");*  


### STEP 3 (CREATE A TABLE THAT PUTS THE DATA IN THE EXTERNAL TABLE IN STEP 2 INTO A PROPER TABLE ON PRESTO)
#### RUN ON PRESTO
*CREATE TABLE dott_staging.tbl_states*  
*as*  
*select*  
*trim(cast(vehicle_id as varchar)) vehicle_id,*  
*trim(cast(time_updated as varchar)) time_updated,*  
*trim(cast(hardware_generation as varchar)) hardware_generation,*  
*trim(cast(city_name as varchar)) city_name,*  
*trim(cast(country_name as varchar)) country_name,*  
*trim(cast(is_deployed as varchar)) is_deployed,*  
*trim(cast(is_in_warehouse as varchar)) is_in_warehouse,*  
*trim(cast(is_broken as varchar)) is_broken*  
*from dott_staging.tbl_states_ext;*  
