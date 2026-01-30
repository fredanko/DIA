CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT PRIMARY KEY,

    eva BIGINT NOT NULL REFERENCES stationen(eva),
    train_id BIGINT NOT NULL REFERENCES dim_train(train_id),

    -- planned time
    arrival_pt_id   BIGINT NULL REFERENCES dim_time(time_id),
    departure_pt_id BIGINT NULL REFERENCES dim_time(time_id),

    -- changed/actual time (DB "ct")
    arrival_ct_id   BIGINT NULL REFERENCES dim_time(time_id),
    departure_ct_id BIGINT NULL REFERENCES dim_time(time_id),

    -- cancellation creation time (DB "clt")
    arrival_clt_id   BIGINT NULL REFERENCES dim_time(time_id),
    departure_clt_id BIGINT NULL REFERENCES dim_time(time_id),

    -- status (DB "cs")
    arrival_cs   TEXT NULL,
    departure_cs TEXT NULL,

    -- platforms (optional but useful)
    arrival_pp   TEXT NULL,  -- planned platform ar@pp
    departure_pp TEXT NULL,  -- planned platform dp@pp
    arrival_cp   TEXT NULL,  -- changed platform ar@cp
    --departure_cp TEXT NULL,   -- changed platform dp@cp
    departure_cp TEXT NULL   -- changed platform dp@cp

     -- stop order within this train run
    -- stop_sequence_index INTEGER
);

CREATE INDEX IF NOT EXISTS stops_eva_idx ON stops (eva);
CREATE INDEX IF NOT EXISTS stops_departure_pt_idx ON stops (departure_pt_id);
CREATE INDEX IF NOT EXISTS stops_arrival_pt_idx ON stops (arrival_pt_id);
