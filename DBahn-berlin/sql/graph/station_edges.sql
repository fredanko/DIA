WITH base AS (
    SELECT
        s.stop_id,
        s.train_id,
        s.eva,
        t.ts AS planned_ts,
        DATE(t.ts) AS service_date,
        LAG(t.ts) OVER (
            PARTITION BY s.train_id, DATE(t.ts)
            ORDER BY t.ts
        ) AS prev_ts
    FROM stops s
    JOIN dim_time t
      ON t.time_id = COALESCE(s.arrival_pt_id, s.departure_pt_id)
    --- filter out busses here
    JOIN dim_train dt
      ON dt.train_id = s.train_id
    WHERE dt.category != 'Bus'
),
runs AS (
    SELECT
        stop_id,
        train_id,
        eva,
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
        train_id,
        eva,
        service_date,
        run_no,
        ROW_NUMBER() OVER (
            PARTITION BY train_id, service_date, run_no
            ORDER BY planned_ts
        ) - 1 AS seq
    FROM runs
),
pairs AS (
    SELECT
        a.eva AS eva_u,
        b.eva AS eva_v
    FROM sequenced a
    JOIN sequenced b
      ON a.train_id = b.train_id
     AND a.service_date = b.service_date
     AND a.run_no = b.run_no
     AND b.seq = a.seq + 1
)
SELECT DISTINCT
    LEAST(eva_u, eva_v)     AS eva_u,
    GREATEST(eva_u, eva_v)  AS eva_v
FROM pairs
WHERE eva_u <> eva_v;
