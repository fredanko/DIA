CREATE TEMP TABLE IF NOT EXISTS _stops_change_stage (
    stop_id        TEXT PRIMARY KEY,
    snapshot_ts    TIMESTAMPTZ NULL,

    arrival_ct_id     BIGINT NULL,
    departure_ct_id   BIGINT NULL,
    arrival_clt_id    BIGINT NULL,
    departure_clt_id  BIGINT NULL,

    arrival_cs      TEXT NULL,
    departure_cs    TEXT NULL,

    arrival_cp      TEXT NULL,
    departure_cp    TEXT NULL
);
