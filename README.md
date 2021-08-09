# Dot Data Modelling Task
## Introduction
Two datasets were provided in the form of CSV files and the task is to model the data to help business users answer questions like the below:
- How many vehicles are deployed in each city/ country/ as a whole for a given day/ week?
- How many vehicles are lost in each city/ country/ as a whole for a given day/week?
- What is the 14-day running lost percentage (total loss vehicles in the D-21 and D-7 divided by the average deployed vehicles in the same period) in each city/country as a whole for a given day?
- How many vehicles with battery level are less than 20% in each city ….?

## Dataset
### Telemetry
This dataset was provided in a csv file named ***data_modelling_test_tbl_telemetry.csv***. It contains records of periodic gps signals received by Dott's vehicles; it also contains data on the vehicle's battery level and whether or not the vehicle is being charged at that particular moment. Typically, a vehicle will have several records in an hour (roughly every 15 minutes).

![image](https://user-images.githubusercontent.com/35803494/128660956-978f2b9a-bd33-4b56-b9a6-775962d1dc4e.png)

### State
This dataset was provided in a csv file named ***data_modelling_test_tbl_states.csv***. It contains information about the state of vehicles and what city/country they are being operated out of. There are three possible states a vehicle can have; ***is_deployed***, ***is_broken***, ***is_in_warehouse***. Typically, a vehicle will have several records in an hour (roughly every 15 minutes).

![image](https://user-images.githubusercontent.com/35803494/128661421-db15f2c3-1b98-4a1f-8082-cc370a802581.png)


## Tools used
- HDFS
- HIVE
- PRESTO
 
 ***The steps on how to load the CSV files attached for this test using the tools above is given in loading_csv_steps.md***
 
 ## APPROACH TO THE TASK
 After studying the data and the business questions, I made the below decisions on the data modelling:
 
 ### Telemetry
 I decided to model this as a ***snapshot fact table*** that shows the status of every vehicle at the end of every day. The field that determines the date is the ***time_gps*** field. The reason for this is because a lot of the business questions seemed to focus on ***activities that happened in a given day/week***. As a result, it appears that the business will get the most value from having the data modelled on at least a daily grain. The model is incremental and builds on records from the previous day (Day minus 1). Every vehicle can only have one record for a given day. This model can help business users know the status of every vehicle at the end of a day, as well as how many days have passed since a vehicle last received a gps signal.
 
 ### State
 I decided to model this as a ***Type 2 Slowly Changing Dimension***. This dataset contains data received every 15 minutes and shows the current state of a vehicle as defined by three states; ***is_deployed***, ***is_broken***, ***is_in_warehouse***. However, based on the kind of questions the business is looking to ask of the data, most of this data is redundant since this is essentially a snapshot of vehicle state, roughly every 15 minutes. What will be of value to the business will be to show state changes only. A state change in this model is defined as when any of the three vehicle states changes from **true** to **false** and vice-versa. Modelled as a Type 2 Slowly Changing Dimension, this model will close a record and open a new one when there's a state change in one of the three states; it will also track which of the states had a change so that the business can use this to answer key business questions.
 
 ## Model structure
 
 ### Snapshot Fact Table

 ![image](https://user-images.githubusercontent.com/35803494/128664983-ac6917a5-bfe1-49fa-a70a-b1055a806a7b.png)
 
 ### Slowly Changing Dimension (Type 2)
![image](https://user-images.githubusercontent.com/35803494/128672979-de817e75-a953-4621-a7c6-15a56dae1052.png)


 
 ## Variables used in query
 - **{var_d1}** : Current Day minus 1
 - **{var_d2}** : Current Day minus 2
 
 ## Sample queries
