SELECT
  eva,
  name,
  latitude,
  longitude,
  sqrt(
    power(%(lat)s - latitude, 2) +
    power(%(lon)s - longitude, 2)
  ) AS distance_deg
FROM public.stationen
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
ORDER BY distance_deg
LIMIT 1;