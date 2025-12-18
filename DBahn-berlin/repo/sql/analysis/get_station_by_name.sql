SELECT eva, latitude, longitude
FROM public.stationen
WHERE name ILIKE %(name)s
LIMIT 1;
