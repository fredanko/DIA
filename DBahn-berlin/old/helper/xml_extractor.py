import xml.etree.ElementTree as ET

def extract_s_id_and_pts(root: ET.Element):
    station_name = root.get("station", None)
    if not station_name:
        return
    rows = []
    for s in root.findall("s"):
        s_id = s.get("id")

        ar = s.find("ar")
        dp = s.find("dp")

        ar_pt = ar.get("pt") if ar is not None else None
        dp_pt = dp.get("pt") if dp is not None else None

        rows.append({"id": s_id, "ar_pt": ar_pt, "dp_pt": dp_pt})
    return station_name, rows