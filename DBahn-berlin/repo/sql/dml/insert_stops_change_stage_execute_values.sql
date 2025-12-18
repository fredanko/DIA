INSERT INTO _stops_change_stage (
                    stop_id, snapshot_ts,
                    actual_arrival, actual_departure,
                    cancelled_arrival, cancelled_departure,
                    arrival_cs, departure_cs
                )
                VALUES %s
