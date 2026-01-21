SELECT
  COUNT(*) AS n,
  AVG(EXTRACT(EPOCH FROM (t_ct.ts - t_pt.ts))) AS avg_delay_seconds
FROM public.stops s
JOIN public.dim_time t_pt
  ON t_pt.time_id = s.departure_pt_id
JOIN public.dim_time t_ct
  ON t_ct.time_id = s.departure_ct_id;
