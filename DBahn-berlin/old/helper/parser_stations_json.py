import json

def parse(conn, json_path, CREATE_STATIONEN, INSERT_STATIONEN):

    raw = json.loads(json_path.read_text(encoding="utf-8"))
    stations = raw["result"] if isinstance(raw, dict) and "result" in raw else raw
    print("Stations in file:", len(stations))

    rows = []
    for st in stations:
        name = st.get("name")
        for eva in (st.get("evaNumbers") or []):
            if eva.get("isMain") is True:
                eva_no = eva.get("number")

                coords = (eva.get("geographicCoordinates") or {}).get("coordinates")
                lon = lat = None
                if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                    lon, lat = float(coords[0]), float(coords[1])

                if eva_no is not None:
                    rows.append(
                        {
                            "eva": int(eva_no),
                            "name": name,
                            "lat": lat,
                            "lon": lon,
                        }
                    )

    with conn.cursor() as cur:
        cur.execute(CREATE_STATIONEN)
    conn.commit()

    payload = [(r["eva"], r["name"], r["lat"], r["lon"]) for r in rows]

    with conn.cursor() as cur:
        cur.executemany(INSERT_STATIONEN, payload)
    conn.commit()

    print("Upserted rows:", len(payload))
    conn.close()