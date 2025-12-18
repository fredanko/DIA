SELECT
    st.eva,
    st.name AS station_name,
    COUNT(*) AS n,
    AVG(EXTRACT(EPOCH FROM (s.actual_departure - s.dp_ts))) AS avg_delay_seconds
FROM public.stops s
JOIN public.stationen st
  ON st.eva = s.eva
WHERE s.dp_ts IS NOT NULL
  AND s.actual_departure IS NOT NULL
  AND st.name ILIKE %(station_name)s
GROUP BY st.eva, st.name;
