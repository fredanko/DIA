CREATE TABLE IF NOT EXISTS stops (
        stop_id TEXT PRIMARY KEY,

        eva   BIGINT NOT NULL REFERENCES stationen(eva),
        ar_ts TIMESTAMPTZ,
        dp_ts TIMESTAMPTZ,

        CHECK (ar_ts IS NULL OR dp_ts IS NULL OR dp_ts >= ar_ts)
    );

    CREATE INDEX IF NOT EXISTS stops_eva_ar_idx
      ON stops (eva, ar_ts);
