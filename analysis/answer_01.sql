SELECT
    trip_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM yellow_taxi_trips
WHERE trip_year = 2023
  AND trip_month BETWEEN 1 AND 5
GROUP BY trip_month
ORDER BY trip_month;