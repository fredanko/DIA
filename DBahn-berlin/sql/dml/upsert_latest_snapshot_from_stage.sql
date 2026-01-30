INSERT INTO _stops_latest_snapshot (stop_id, snapshot_ts)
            SELECT
                stop_id,
                COALESCE(snapshot_ts, '-infinity'::timestamptz)
            FROM _stops_change_stage
            ON CONFLICT (stop_id) DO UPDATE
            SET snapshot_ts = GREATEST(_stops_latest_snapshot.snapshot_ts, EXCLUDED.snapshot_ts);
