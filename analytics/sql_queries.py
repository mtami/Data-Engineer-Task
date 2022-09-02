device_agg_hourly_query = """
WITH devices_cte AS
  (SELECT device_id,
          temperature,
          time,
          date_trunc('hour', TIMESTAMP 'epoch' + (time::int) * INTERVAL '1 second') AS date_hour,
          LOCATION,
          lag(LOCATION) OVER (PARTITION BY device_id,
                                           date_trunc('hour', TIMESTAMP 'epoch' + (time::int) * INTERVAL '1 second')
                              ORDER BY time) AS lag_location
   FROM devices),
     device_distance AS
  (SELECT *,
          CASE
              WHEN lag_location IS NULL THEN 0
              ELSE 1.6 * SQRT(POW(69.1 * ((location::json ->> 'latitude')::float - (lag_location::json ->> 'longitude')::float), 2) + POW(69.1 * ((lag_location::json ->> 'longitude')::float - (location::json ->> 'longitude')::float) * COS((location::json ->> 'longitude')::float / 57.3), 2))
          END AS sub_distance_km
   FROM devices_cte)
SELECT device_id,
       date_hour ,
       max(temperature) as max_temperature,
       count(*) as total_measurements,
       sum(sub_distance_km) as distance_km
from device_distance
{condition} 
group by device_id,
         date_hour
"""