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
    JOIN dim_train dt
      ON dt.train_id = s.train_id
    WHERE dt.category != 'Bus'
),
runs AS (
    SELECT *,
        SUM(
            CASE
                WHEN prev_ts IS NULL
                  OR planned_ts - prev_ts > INTERVAL '4 hours'
                THEN 1 ELSE 0
            END
        ) OVER (
            PARTITION BY train_id, service_date
            ORDER BY planned_ts
        ) AS run_no
    FROM base
),
sequenced AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY train_id, service_date, run_no
            ORDER BY planned_ts
        ) AS seq
    FROM runs
)

SELECT
    a.train_id,
    dt.category,
    dt.number        AS train_number,
    dt.line          AS train_line,

    a.eva            AS eva_from,
    b.eva            AS eva_to,

    t_dep.ts         AS dep_ts,
    t_arr.ts         AS arr_ts
FROM sequenced a
JOIN sequenced b
  ON a.train_id = b.train_id
 AND a.service_date = b.service_date
 AND a.run_no = b.run_no
 AND b.seq = a.seq + 1
JOIN dim_train dt
  ON dt.train_id = a.train_id
JOIN dim_time t_dep
  ON t_dep.time_id = a.departure_pt_id
JOIN dim_time t_arr
  ON t_arr.time_id = b.arrival_pt_id
WHERE dt.category != 'Bus';

