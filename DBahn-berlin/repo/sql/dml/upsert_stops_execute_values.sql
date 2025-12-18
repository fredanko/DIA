INSERT INTO stops (stop_id, eva, ar_ts, dp_ts)
VALUES %s
ON CONFLICT (stop_id) DO UPDATE
SET eva   = EXCLUDED.eva,
    ar_ts = EXCLUDED.ar_ts,
    dp_ts = EXCLUDED.dp_ts;
