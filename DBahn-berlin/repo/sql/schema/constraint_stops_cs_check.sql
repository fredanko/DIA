DO $$
            BEGIN
                ALTER TABLE public.stops
                    ADD CONSTRAINT stops_cs_check
                    CHECK (
                        (arrival_cs   IS NULL OR arrival_cs   IN ('a','p','c')) AND
                        (departure_cs IS NULL OR departure_cs IN ('a','p','c'))
                    );
            EXCEPTION
                WHEN duplicate_object THEN
                    NULL;
            END $$;
