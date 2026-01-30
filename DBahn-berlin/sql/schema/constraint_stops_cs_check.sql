DO $$
BEGIN
    ALTER TABLE public.stops
        DROP CONSTRAINT IF EXISTS stops_cs_check;

    ALTER TABLE public.stops
        ADD CONSTRAINT stops_cs_check
        CHECK (
            (arrival_cs   IS NULL OR arrival_cs   IN ('a','p','c')) AND
            (departure_cs IS NULL OR departure_cs IN ('a','p','c'))
        );
END $$;
