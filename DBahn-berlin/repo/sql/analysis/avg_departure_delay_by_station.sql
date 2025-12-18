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
        GROUP BY st.eva, st.name
        HAVING COUNT(*) > 0
        ORDER BY avg_delay_seconds DESC;
