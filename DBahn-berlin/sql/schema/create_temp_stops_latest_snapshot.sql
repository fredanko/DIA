CREATE TEMP TABLE IF NOT EXISTS _stops_latest_snapshot (
                stop_id     text PRIMARY KEY,
                snapshot_ts timestamptz NOT NULL
            );
