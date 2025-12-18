ALTER TABLE public.stops
                ADD COLUMN IF NOT EXISTS actual_arrival   timestamptz,
                ADD COLUMN IF NOT EXISTS actual_departure timestamptz,
                ADD COLUMN IF NOT EXISTS cancelled_arrival   timestamptz,
                ADD COLUMN IF NOT EXISTS cancelled_departure timestamptz,
                ADD COLUMN IF NOT EXISTS arrival_cs   text,
                ADD COLUMN IF NOT EXISTS departure_cs text;
