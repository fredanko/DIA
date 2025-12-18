-- =========================
-- DDL: Star(-ish) Schema
-- =========================

BEGIN;

CREATE SCHEMA IF NOT EXISTS dw;

-- Für Entity-Matching (Stationsnamen):
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Für "closest station" ohne PostGIS (Haversine/earthdistance):
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- -------------------------
-- Helper: Normalisierung
-- -------------------------
CREATE OR REPLACE FUNCTION dw.norm_text(txt text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT regexp_replace(
           replace(replace(replace(replace(lower(coalesce(txt,'')),
             'ß','ss'),'ä','ae'),'ö','oe'),'ü','ue'),
           '[^a-z0-9]+', '', 'g'
         );
$$;

-- -------------------------
-- Dimension: Station
-- -------------------------
CREATE TABLE IF NOT EXISTS dw.dim_station (
  station_key   BIGSERIAL PRIMARY KEY,
  name_canonical TEXT NOT NULL,
  name_norm     TEXT GENERATED ALWAYS AS (dw.norm_text(name_canonical)) STORED,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (name_canonical)
);

CREATE INDEX IF NOT EXISTS idx_dim_station_name_norm_trgm
  ON dw.dim_station USING gin (name_norm gin_trgm_ops);

-- EVA-Nummern (business keys) + (optional) coords pro EVA
CREATE TABLE IF NOT EXISTS dw.dim_station_eva (
  eva         BIGINT PRIMARY KEY,
  station_key BIGINT NOT NULL REFERENCES dw.dim_station(station_key),
  is_main     BOOLEAN NOT NULL DEFAULT FALSE,
  lat         DOUBLE PRECISION,
  lon         DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_dim_station_eva_station
  ON dw.dim_station_eva(station_key);

-- Alias-Tabelle für Mismatches (Mapping-Cache + Audit)
CREATE TABLE IF NOT EXISTS dw.dim_station_alias (
  alias        TEXT PRIMARY KEY,
  alias_norm   TEXT GENERATED ALWAYS AS (dw.norm_text(alias)) STORED,
  station_key  BIGINT NOT NULL REFERENCES dw.dim_station(station_key),
  source       TEXT,
  confidence   REAL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_station_alias_norm
  ON dw.dim_station_alias(alias_norm);

CREATE INDEX IF NOT EXISTS idx_dim_station_alias_norm_trgm
  ON dw.dim_station_alias USING gin (alias_norm gin_trgm_ops);

-- Unresolved-Log (wenn fuzzy nicht sicher genug ist)
CREATE TABLE IF NOT EXISTS dw.stg_unresolved_station (
  unresolved_key BIGSERIAL PRIMARY KEY,
  station_name   TEXT NOT NULL,
  station_norm   TEXT NOT NULL,
  context_file   TEXT,
  best_match     TEXT,
  best_similarity REAL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------
-- Dimension: Time (hour)
-- -------------------------
CREATE TABLE IF NOT EXISTS dw.dim_time_hour (
  hour_ts  TIMESTAMP PRIMARY KEY,
  "date"   DATE NOT NULL,
  "year"   INT NOT NULL,
  "month"  INT NOT NULL,
  "day"    INT NOT NULL,
  "hour"   INT NOT NULL
);

-- -------------------------
-- Dimension: Trip
-- -------------------------
-- Minimal: genug für Gruppierung + später Graph/Edges
CREATE TABLE IF NOT EXISTS dw.dim_trip (
  trip_key      BIGSERIAL PRIMARY KEY,
  service_date  DATE NOT NULL,
  category      TEXT NOT NULL,  -- z.B. "S", "ICE", ...
  train_no      TEXT NOT NULL,  -- z.B. "41108"
  owner         TEXT,           -- z.B. "08"
  trip_type     TEXT,           -- z.B. "p"
  line          TEXT,           -- z.B. "41"
  UNIQUE (service_date, category, train_no, owner, trip_type, line)
);

-- -------------------------
-- Fact: Stop Event
-- -------------------------
CREATE TABLE IF NOT EXISTS dw.fact_stop_event (
  stop_event_key BIGSERIAL PRIMARY KEY,

  station_key    BIGINT NOT NULL REFERENCES dw.dim_station(station_key),
  trip_key       BIGINT REFERENCES dw.dim_trip(trip_key),

  -- Natural stop id aus XML: <s id="...">
  stop_id        TEXT NOT NULL,

  -- Planned (timetable)
  planned_arrival_ts    TIMESTAMP,
  planned_departure_ts  TIMESTAMP,
  planned_platform      TEXT,
  planned_path          TEXT,      -- ppth aus XML (pipe-separated)

  -- Actual/Changed (changes)
  actual_arrival_ts     TIMESTAMP,
  actual_departure_ts   TIMESTAMP,
  actual_platform       TEXT,

  -- Status (cs: p/a/c) wenn vorhanden
  arrival_status        CHAR(1),
  departure_status      CHAR(1),

  is_canceled           BOOLEAN NOT NULL DEFAULT FALSE,

  -- Convenience: event times/hours (für snapshot queries)
  planned_event_ts      TIMESTAMP GENERATED ALWAYS AS (COALESCE(planned_departure_ts, planned_arrival_ts)) STORED,
  planned_hour_ts       TIMESTAMP GENERATED ALWAYS AS (date_trunc('hour', COALESCE(planned_departure_ts, planned_arrival_ts))) STORED,
  actual_event_ts       TIMESTAMP GENERATED ALWAYS AS (COALESCE(actual_departure_ts, actual_arrival_ts)) STORED,
  actual_hour_ts        TIMESTAMP GENERATED ALWAYS AS (date_trunc('hour', COALESCE(actual_departure_ts, actual_arrival_ts))) STORED,

  -- Delay (Minuten)
  delay_arrival_min     INT GENERATED ALWAYS AS (
    CASE
      WHEN actual_arrival_ts IS NOT NULL AND planned_arrival_ts IS NOT NULL
      THEN (EXTRACT(EPOCH FROM (actual_arrival_ts - planned_arrival_ts))/60)::INT
      ELSE NULL
    END
  ) STORED,
  delay_departure_min   INT GENERATED ALWAYS AS (
    CASE
      WHEN actual_departure_ts IS NOT NULL AND planned_departure_ts IS NOT NULL
      THEN (EXTRACT(EPOCH FROM (actual_departure_ts - planned_departure_ts))/60)::INT
      ELSE NULL
    END
  ) STORED,
  effective_delay_min   INT GENERATED ALWAYS AS (COALESCE(delay_departure_min, delay_arrival_min)) STORED,

  load_source   TEXT,
  load_file     TEXT,
  load_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (station_key, stop_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_station ON dw.fact_stop_event(station_key);
CREATE INDEX IF NOT EXISTS idx_fact_planned_hour_canceled ON dw.fact_stop_event(planned_hour_ts, is_canceled);
CREATE INDEX IF NOT EXISTS idx_fact_actual_hour_canceled  ON dw.fact_stop_event(actual_hour_ts, is_canceled);

-- -------------------------
-- Optional: Edges für Netzwerk
-- -------------------------
CREATE TABLE IF NOT EXISTS dw.fact_route_edge (
  from_station_key BIGINT NOT NULL REFERENCES dw.dim_station(station_key),
  to_station_key   BIGINT NOT NULL REFERENCES dw.dim_station(station_key),
  category         TEXT,
  line             TEXT,
  weight           INT NOT NULL DEFAULT 1,
  first_seen       TIMESTAMP,
  last_seen        TIMESTAMP,
  PRIMARY KEY (from_station_key, to_station_key, category, line)
);

COMMIT;
