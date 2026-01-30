CREATE TABLE IF NOT EXISTS stationen (
    eva        BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    latitude   DOUBLE PRECISION,
    longitude  DOUBLE PRECISION
);
