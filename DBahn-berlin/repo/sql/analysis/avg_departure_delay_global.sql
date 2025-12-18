SELECT
            AVG(EXTRACT(EPOCH FROM (actual_departure - dp_ts))) AS avg_delay_seconds,
            COUNT(*) AS n
        FROM public.stops
        WHERE dp_ts IS NOT NULL
          AND actual_departure IS NOT NULL;
