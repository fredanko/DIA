UPDATE public.stops s
SET
  arrival_ct_id = st.arrival_ct_id,
  departure_ct_id = st.departure_ct_id,
  arrival_clt_id = st.arrival_clt_id,
  departure_clt_id = st.departure_clt_id,
  arrival_cs = st.arrival_cs,
  departure_cs = st.departure_cs,
  arrival_cp = st.arrival_cp,
  departure_cp = st.departure_cp
FROM _stops_change_stage st
JOIN _stops_latest_snapshot ls
  ON ls.stop_id = st.stop_id
WHERE s.stop_id = st.stop_id
  AND st.snapshot_ts = ls.snapshot_ts;
