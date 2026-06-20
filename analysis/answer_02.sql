SELECT
    pickup_hour,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM yellow_taxi_trips
WHERE trip_year = 2023
  AND trip_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour;