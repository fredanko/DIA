CREATE TABLE IF NOT EXISTS dim_train (
    train_id     BIGSERIAL PRIMARY KEY,

    owner        TEXT NOT NULL,  -- tl@o
    category     TEXT NOT NULL,  -- tl@c
    number       TEXT NOT NULL,  -- tl@n

    trip_type    TEXT NULL,      -- tl@t
    filter_flags TEXT NULL,      -- tl@f
    line         TEXT NULL,      -- ar/dp@l (line)

    UNIQUE (owner, category, number, trip_type, filter_flags, line)
);
