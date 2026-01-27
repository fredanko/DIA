WITH base AS (
    SELECT
        s.stop_id,
        s.train_id,
        t.ts AS planned_ts,
        DATE(t.ts) AS service_date,
        LAG(t.ts) OVER (
            PARTITION BY s.train_id, DATE(t.ts)
            ORDER BY t.ts
        ) AS prev_ts
    FROM stops s
    JOIN dim_time t
      ON t.time_id = COALESCE(s.arrival_pt_id, s.departure_pt_id)
),
runs AS (
    SELECT
        stop_id,
        train_id,
        planned_ts,
        service_date,
        SUM(
            CASE
                WHEN prev_ts IS NULL
                  OR planned_ts - prev_ts > INTERVAL '4 hours'
                THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY train_id, service_date
            ORDER BY planned_ts
        ) AS run_no
    FROM base
),
sequenced AS (
    SELECT
        stop_id,
        ROW_NUMBER() OVER (
            PARTITION BY train_id, service_date, run_no
            ORDER BY planned_ts
        ) - 1 AS seq
    FROM runs
)
UPDATE stops s
SET stop_sequence_index = q.seq
FROM sequenced q
WHERE s.stop_id = q.stop_id;
