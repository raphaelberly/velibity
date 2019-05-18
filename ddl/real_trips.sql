CREATE OR REPLACE VIEW velibity.real_trips AS (

WITH
trips AS (
  SELECT
    t.id,
    t.username,
    t.distance_km,
    t.duration_s,
    t.is_elec,
    t.start_datetime,
    t.start_datetime + t.duration_s * interval '1 second' AS end_datetime,
    lag(t.start_datetime + t.duration_s * interval '1 second')
      OVER (PARTITION BY t.username ORDER BY t.start_datetime) AS last_end_datetime
  FROM velibity.trips t
  WHERE distance_km > 0
),
trips_with_new_id AS (
  SELECT
    t.*,
    coalesce((t.start_datetime - t.last_end_datetime >= INTERVAL '5 minutes'), TRUE) AS is_main,
    sum((t.start_datetime - t.last_end_datetime >= INTERVAL '5 minutes')::INT * t.id)
      OVER (PARTITION BY t.username ORDER BY t.start_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS new_id
  FROM trips t
),
trips_clean AS (
  SELECT
    md5(concat(t.username, t.new_id::TEXT)) AS id,
    t.username,
    min(t.start_datetime)::TIMESTAMP(0)     AS start_datetime,
    sum(t.distance_km)::NUMERIC(3,1)        AS distance_km,
    sum(t.duration_s)                       AS duration_s,
    max(t.is_elec::INT)::BOOL               AS is_elec
  FROM trips_with_new_id t
  GROUP BY 1,2
)
SELECT
  t.*
FROM trips_clean t
ORDER BY t.start_datetime DESC

);
