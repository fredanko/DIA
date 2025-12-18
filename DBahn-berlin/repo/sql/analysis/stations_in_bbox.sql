SELECT eva, name, latitude, longitude
FROM public.stationen
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
