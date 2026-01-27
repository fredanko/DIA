ALTER TABLE public.stops
ADD COLUMN IF NOT EXISTS stop_sequence_index INTEGER;
