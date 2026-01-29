SELECT
  s.stop_id,
  s.eva,
  t_ar.ts AS arrival_pt,
  t_dp.ts AS departure_pt,
  t_ar_ct.ts AS arrival_ct,
  t_dp_ct.ts AS departure_ct,
  s.arrival_cs,
  s.departure_cs
FROM public.stops s
LEFT JOIN public.dim_time t_ar ON t_ar.time_id = s.arrival_pt_id
LEFT JOIN public.dim_time t_dp ON t_dp.time_id = s.departure_pt_id
LEFT JOIN public.dim_time t_ar_ct ON t_ar_ct.time_id = s.arrival_ct_id
LEFT JOIN public.dim_time t_dp_ct ON t_dp_ct.time_id = s.departure_ct_id
ORDER BY random()
LIMIT 1;