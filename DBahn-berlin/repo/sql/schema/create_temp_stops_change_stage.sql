CREATE TEMP TABLE IF NOT EXISTS _stops_change_stage (
                stop_id             text PRIMARY KEY,
                snapshot_ts         timestamptz NULL,
                actual_arrival      timestamptz NULL,
                actual_departure    timestamptz NULL,
                cancelled_arrival   timestamptz NULL,
                cancelled_departure timestamptz NULL,
                arrival_cs          text NULL,
                departure_cs        text NULL
            );
