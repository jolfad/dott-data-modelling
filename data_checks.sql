---QUERY TO TEST THAT ALL VEHICLES FROM PREVIOUS DAY ARE CARRIED OVER TO THE NEXT DAY---
---SHOULD RETURN ZERO---

SELECT count(a.vehicle_id) AS cnt
FROM
  dott_vehicle.daily_vehicle_snapshot a
WHERE (a.date_key = {var_d2}) 
AND (NOT EXISTS (SELECT b.vehicle_id
FROM
  dott_vehicle.daily_vehicle_snapshot b
WHERE ((b.date_key = {var_d1}) AND (a.vehicle_id = b.vehicle_id))
));


---QUERY TO CHECK THAT ALL BATTERY LEVELS ARE WITHIN 0-100---
---SHOULD RETURN ZERO---

SELECT count(1) AS cnt
FROM
  dott_vehicle.daily_vehicle_snapshot
WHERE (date_key = YYYYMMDD) AND (battery_level < 0) AND (battery_level > 100);


---QUERY TO CHECK THAT THERE ARE NO DUPLICATE RECORDS AND VEHICLE_ID IS UNIQUE---
---SHOULD RETURN ZERO---

SELECT
  vehicle_id
, count(1) AS cnt
FROM
  dott_vehicle.daily_vehicle_snapshot
WHERE (date_key = YYYYMMDD)
GROUP BY vehicle_id
HAVING (count(1) > 1);


---QUERY TO CHECK THAT EVERY VEHICLE_ID HAS ONLY ONE CURRENT RECORD---
---SHOULD RETURN ZERO---

SELECT
  vehicle_id
, count(1) cnt
FROM
  dott_vehicle.dim_vehicle_state
WHERE (end_date = 'N/A')
GROUP BY vehicle_id
HAVING (count(1) > 1);


---QUERY TO CHECK THAT THERE ARE NO GAPS IN THE RECORDS IN THE DIMENSION FOR EVERY VEHICLE---
---SHOULD RETURN ZERO---

SELECT count(1) cnt
FROM
  (
   SELECT
     vehicle_id
   , start_date
   , end_date
   , lead(start_date, 1) OVER (PARTITION BY vehicle_id ORDER BY start_date) next_start_date
   FROM
     dott_vehicle.dim_vehicle_state
) 
WHERE ((vehicle_id <> 'N/A') AND (end_date <> next_start_date));