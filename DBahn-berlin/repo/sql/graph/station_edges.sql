WITH consecutive_stops AS (
    SELECT
        s1.eva AS eva_from,
        s2.eva AS eva_to
    FROM stops s1
    JOIN stops s2
      ON s1.train_id = s2.train_id
     AND s2.stop_sequence_index = s1.stop_sequence_index + 1

    JOIN dim_time t1
      ON t1.time_id = COALESCE(s1.arrival_pt_id, s1.departure_pt_id)
    JOIN dim_time t2
      ON t2.time_id = COALESCE(s2.arrival_pt_id, s2.departure_pt_id)

    WHERE DATE(t1.ts) = DATE(t2.ts)
),
undirected_edges AS (
    SELECT DISTINCT
        LEAST(eva_from, eva_to) AS eva_u,
        GREATEST(eva_from, eva_to) AS eva_v
    FROM consecutive_stops
    WHERE eva_from <> eva_to
)
SELECT
    eva_u,
    eva_v
FROM undirected_edges
ORDER BY eva_u, eva_v;
