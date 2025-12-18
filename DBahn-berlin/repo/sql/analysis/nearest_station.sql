SELECT
  eva,
  name,
  latitude,
  longitude,
  6371000 * 2 * asin(
    sqrt(
      power(sin(radians(%(lat)s - latitude) / 2), 2) +
      cos(radians(latitude)) * cos(radians(%(lat)s)) *
      power(sin(radians(%(lon)s - longitude) / 2), 2)
    )
  ) AS distance_m
FROM public.stationen
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
ORDER BY distance_m
LIMIT 1;
