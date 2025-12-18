WITH bounds AS (
  SELECT
    to_timestamp(%(snap)s, 'YYMMDDHH24') AS t0,
    to_timestamp(%(snap)s, 'YYMMDDHH24') + INTERVAL '1 hour' AS t1
)
SELECT
  COUNT(DISTINCT s.stop_id) AS cancelled_stops
FROM public.stops s
CROSS JOIN bounds b
WHERE
  (s.arrival_cs = 'c'
   AND s.cancelled_arrival IS NOT NULL
   AND s.cancelled_arrival >= b.t0
   AND s.cancelled_arrival <  b.t1)
  OR
  (s.departure_cs = 'c'
   AND s.cancelled_departure IS NOT NULL
   AND s.cancelled_departure >= b.t0
   AND s.cancelled_departure <  b.t1);
