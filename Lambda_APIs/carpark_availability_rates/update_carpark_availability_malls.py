import json, time

from carpark_avail_rates_util import (
    get_db,
    rebuild_join_snapshot,
    dump_csv_to_s3_from_db,
)

from carpark_availability_malls_util import (
    fetch_all_lta_malls,
    upsert_mall_rows,
    _cp_number_from_lta,
    SAVE_CSV, 
)

def lambda_handler(event, context):
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    rows = fetch_all_lta_malls()
    upsert_mall_rows(rows)

    conn = get_db()
    rebuild_join_snapshot(conn)

    csv_key = dump_csv_to_s3_from_db(ts) if SAVE_CSV else None

    sample = [{
        "carpark_number": _cp_number_from_lta(r.get("CarParkID")).upper(),
        "lot_type": (r.get("LotType") or "").strip().upper(),
        "lots_available": int(r.get("AvailableLots") or 0),
        "address": (r.get("Development") or "").strip()
    } for r in rows[:3]]

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mall_rows_seen": len(rows),
            "sample_first_3": sample,
            "csv_key": csv_key
        })
    }