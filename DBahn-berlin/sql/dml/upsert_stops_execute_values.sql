INSERT INTO stops (
  stop_id, eva, train_id,
  arrival_pt_id, departure_pt_id,
  arrival_pp, departure_pp
)
VALUES %s
ON CONFLICT (stop_id) DO UPDATE
SET
  eva = EXCLUDED.eva,
  train_id = EXCLUDED.train_id,
  arrival_pt_id = EXCLUDED.arrival_pt_id,
  departure_pt_id = EXCLUDED.departure_pt_id,
  arrival_pp = EXCLUDED.arrival_pp,
  departure_pp = EXCLUDED.departure_pp;
