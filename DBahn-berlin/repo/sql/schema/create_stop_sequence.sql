WITH ordered_stops AS (
    SELECT
        stop_id,
        ROW_NUMBER() OVER (
            PARTITION BY train_id
            ORDER BY COALESCE(arrival_pt_id, departure_pt_id)
        ) - 1 AS seq
    FROM stops
)
UPDATE stops s
SET stop_sequence_index = o.seq
FROM ordered_stops o
WHERE s.stop_id = o.stop_id;
