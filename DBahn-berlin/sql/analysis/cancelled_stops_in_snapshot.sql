WITH bounds AS (
  SELECT
    to_timestamp(%(snap)s, 'YYMMDDHH24') AS t0,
    to_timestamp(%(snap)s, 'YYMMDDHH24') + INTERVAL '1 hour' AS t1
)
SELECT
  COUNT(DISTINCT s.stop_id) AS cancelled_stops
FROM public.stops s
LEFT JOIN public.dim_time ta_clt ON ta_clt.time_id = s.arrival_clt_id
LEFT JOIN public.dim_time td_clt ON td_clt.time_id = s.departure_clt_id
CROSS JOIN bounds b
WHERE
  (
    s.arrival_cs = 'c'
    AND ta_clt.ts IS NOT NULL
    AND ta_clt.ts >= b.t0 AND ta_clt.ts < b.t1
  )
  OR
  (
    s.departure_cs = 'c'
    AND td_clt.ts IS NOT NULL
    AND td_clt.ts >= b.t0 AND td_clt.ts < b.t1
  );