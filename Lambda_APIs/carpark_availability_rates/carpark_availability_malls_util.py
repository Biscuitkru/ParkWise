import os, json, time, requests
import sqlite3
from pyproj import Transformer
WGS84_TO_SVY21 = Transformer.from_crs("EPSG:4326", "EPSG:3414", always_xy=True)

from update_carpark_availability import (
    get_db,
    rebuild_join_snapshot,
    dump_csv_to_s3_from_db
)

#############################################################
# Config
#############################################################
API_URL   = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
API_KEY   = os.getenv("API_KEY")
SAVE_CSV  = os.getenv("SAVE_CSV", "true").lower() == "true"
MALL_PREFIX = "MALL"                                  # => carpark_number = f"MALL{CarParkID}"

#############################################################
# Network
#############################################################
def _headers():
    if not API_KEY:
        raise RuntimeError("Missing API_KEY env var for LTA DataMall")
    return {"AccountKey": API_KEY, "accept": "application/json"}

def fetch_all_lta_malls():
    """Fetch all LTA rows (500/page), keep only Agency == 'LTA'."""
    rows, skip = [], 0
    while True:
        url = API_URL if skip == 0 else f"{API_URL}?$skip={skip}"
        r = requests.get(url, headers=_headers(), timeout=30)
        r.raise_for_status()
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < 500:
            break
        skip += 500
    return [r for r in rows if (r.get("Agency") or "").upper() == "LTA"]

#############################################################
# Transform helpers
#############################################################
def _split_location(loc):
    if not loc:
        return None, None
    try:
        lat_str, lon_str = loc.strip().split()
        return float(lat_str), float(lon_str)
    except Exception:
        return None, None

def _cp_number_from_lta(carpark_id: str) -> str:
    cid = (carpark_id or "").strip()
    return f"{MALL_PREFIX}{cid}" if cid else ""

#############################################################
# DB upsert
#############################################################
def upsert_mall_rows(rows):
    """
    Upsert LTA mall data into:
      - carpark_availability (per lot_type)
      - carpark_info (minimal row; address + coords)
    Mirror-delete ONLY mall-prefixed rows not seen in this run.
    """
    conn = get_db()
    cur  = conn.cursor()
    retrieved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    cur.execute("BEGIN IMMEDIATE;")
    try:
        for r in rows:
            cp_no_raw = _cp_number_from_lta(r.get("CarParkID"))
            if not cp_no_raw:
                continue
            cp_no = cp_no_raw.upper()  # keep consistent with your joins

            lot_type = (r.get("LotType") or "").strip().upper()
            if not lot_type:
                continue

            lots_avail = int(r.get("AvailableLots") or 0)
            update_dt  = retrieved_at
            total_lots = None

            # availability upsert
            cur.execute("""
                INSERT INTO carpark_availability(
                  carpark_number, lot_type, lots_available, total_lots, update_datetime, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(carpark_number, lot_type) DO UPDATE SET
                  lots_available = excluded.lots_available,
                  total_lots     = COALESCE(carpark_availability.total_lots, excluded.total_lots),
                  update_datetime= excluded.update_datetime,
                  last_seen_at   = excluded.last_seen_at
            """, (cp_no, lot_type, lots_avail, total_lots, update_dt, retrieved_at))
            cur.execute("""
                INSERT OR IGNORE INTO carpark_availability_history(
                  carpark_number, lot_type, lots_available, update_datetime, retrieved_at
                ) VALUES (?, ?, ?, ?, ?)
            """, (cp_no, lot_type, lots_avail, update_dt, retrieved_at))

            # ensure minimal info row (so join has something to attach)
            dev  = (r.get("Development") or "").strip()
            area = (r.get("Area") or "").strip()
            # lat/lon from LTA (or parse "Location")
            lat, lon = r.get("Latitude"), r.get("Longitude")
            if lat is None or lon is None:
                lat, lon = _split_location(r.get("Location"))

            # Convert WGS84 -> SVY21
            if lat is not None and lon is not None:
                easting, northing = WGS84_TO_SVY21.transform(float(lon), float(lat))
                x_txt, y_txt = f"{easting:.3f}", f"{northing:.3f}"
            else:
                x_txt, y_txt = None, None

            address = f"{dev} ({area})" if dev and area else (dev or area or None)

            cur.execute("""
                INSERT INTO carpark_info(
                    carpark_number, address, x_coord, y_coord, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(carpark_number) DO UPDATE SET
                    address    = COALESCE(excluded.address, carpark_info.address),
                    x_coord    = COALESCE(excluded.x_coord, carpark_info.x_coord),
                    y_coord    = COALESCE(excluded.y_coord, carpark_info.y_coord),
                    updated_at = excluded.updated_at
            """, (cp_no, address, x_txt, y_txt, retrieved_at))

        # mirror-delete ONLY stale mall rows (safe for HDB/URA)
        cur.execute("""
            DELETE FROM carpark_availability
            WHERE carpark_number LIKE ? AND last_seen_at < ?
        """, (f"{MALL_PREFIX}%", retrieved_at))

        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise