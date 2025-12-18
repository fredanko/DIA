SELECT stop_id, eva, ar_ts, dp_ts
        FROM stops
        ORDER BY random()
        LIMIT 1;
