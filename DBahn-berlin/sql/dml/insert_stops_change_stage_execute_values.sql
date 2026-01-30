INSERT INTO _stops_change_stage (
  stop_id, snapshot_ts,
  arrival_ct_id, departure_ct_id,
  arrival_clt_id, departure_clt_id,
  arrival_cs, departure_cs,
  arrival_cp, departure_cp
)
VALUES %s
ON CONFLICT (stop_id) DO UPDATE
SET
  snapshot_ts = EXCLUDED.snapshot_ts,
  arrival_ct_id = EXCLUDED.arrival_ct_id,
  departure_ct_id = EXCLUDED.departure_ct_id,
  arrival_clt_id = EXCLUDED.arrival_clt_id,
  departure_clt_id = EXCLUDED.departure_clt_id,
  arrival_cs = EXCLUDED.arrival_cs,
  departure_cs = EXCLUDED.departure_cs,
  arrival_cp = EXCLUDED.arrival_cp,
  departure_cp = EXCLUDED.departure_cp;
