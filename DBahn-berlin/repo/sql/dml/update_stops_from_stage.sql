UPDATE public.stops s
            SET
                actual_arrival      = COALESCE(st.actual_arrival,      s.actual_arrival),
                actual_departure    = COALESCE(st.actual_departure,    s.actual_departure),
                cancelled_arrival   = COALESCE(st.cancelled_arrival,   s.cancelled_arrival),
                cancelled_departure = COALESCE(st.cancelled_departure, s.cancelled_departure),
                arrival_cs          = COALESCE(st.arrival_cs,          s.arrival_cs),
                departure_cs        = COALESCE(st.departure_cs,        s.departure_cs)
            FROM _stops_change_stage st
            JOIN _stops_latest_snapshot ls
              ON ls.stop_id = st.stop_id
            WHERE s.stop_id = st.stop_id
              AND COALESCE(st.snapshot_ts, '-infinity'::timestamptz) = ls.snapshot_ts;
