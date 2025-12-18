INSERT INTO stationen (eva, name, latitude, longitude)
VALUES (%s, %s, %s, %s)
ON CONFLICT (eva) DO UPDATE
SET name = EXCLUDED.name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
