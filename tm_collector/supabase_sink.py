"""Envío de indicadores agregados a Supabase (API REST, sin librerías extra)."""
import logging
from collections import defaultdict
from datetime import datetime, timezone

import requests

from . import config

log = logging.getLogger("supabase")


def _iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def agg_positions(rows):
    """Por ventana y ruta: lecturas, buses distintos y velocidad promedio calculada."""
    step = config.AGG_MINUTES * 60
    acc = defaultdict(lambda: {"readings": 0, "vehicles": set(), "s": 0.0, "n": 0})
    for r in rows:
        if r["vehicle_ts"] < r["poll_ts"] - 600:  # ignora buses con lecturas viejas ("fantasma")
            continue
        a = acc[((r["vehicle_ts"] // step) * step, r["route_id"] or "SIN_RUTA")]
        a["readings"] += 1
        a["vehicles"].add(r["vehicle_id"])
        if r["speed_calc_ms"] is not None:
            a["s"] += r["speed_calc_ms"]
            a["n"] += 1
    return [{
        "bucket_start": _iso(b), "agg_minutes": config.AGG_MINUTES, "route_id": route,
        "readings": a["readings"], "vehicles": len(a["vehicles"]),
        "avg_speed_ms": round(a["s"] / a["n"], 3) if a["n"] else None,
    } for (b, route), a in acc.items()]


def agg_trips(rows):
    """Por ventana y ruta, con el último estado de cada viaje visto en la hora."""
    step = config.AGG_MINUTES * 60
    last = {}
    for r in rows:
        last[r["trip_id"]] = r
    acc = defaultdict(lambda: {"trips": 0, "canceled": 0, "added": 0, "d": 0, "dn": 0})
    for r in last.values():
        a = acc[((r["poll_ts"] // step) * step, r["route_id"] or "SIN_RUTA")]
        a["trips"] += 1
        a["canceled"] += r["schedule_relationship"] == 3
        a["added"] += r["schedule_relationship"] == 1
        if r["delay_s"] is not None:
            a["d"] += r["delay_s"]
            a["dn"] += 1
    return [{
        "bucket_start": _iso(b), "agg_minutes": config.AGG_MINUTES, "route_id": route,
        "trips": a["trips"], "canceled": a["canceled"], "added": a["added"],
        "avg_delay_s": round(a["d"] / a["dn"], 1) if a["dn"] else None,
    } for (b, route), a in acc.items()]


def _post(table, payload, on_conflict=None):
    url = f"{config.SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    headers = {
        "apikey": config.SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {config.SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    r = requests.post(url, json=payload, headers=headers, timeout=60)
    r.raise_for_status()


def _push(table, data, conflict):
    for i in range(0, len(data), 1000):
        _post(table, data[i:i + 1000], on_conflict=conflict)
    log.info("Supabase %s: %d filas", table, len(data))


def push_aggregates(prefix, rows):
    key = "bucket_start,agg_minutes,route_id"
    if prefix == "vp":
        _push("vp_agg", agg_positions(rows), key)
    elif prefix == "tu":
        _push("tu_agg", agg_trips(rows), key)
    elif prefix == "al":
        data = [{
            "alert_id": r["alert_id"], "content_hash": r["content_hash"], "is_deleted": r["is_deleted"],
            "seen_at": _iso(r["poll_ts"]), "header_text": r["header_text"],
            "description_text": r["description_text"], "route_ids": r["route_ids"],
            "active_start": _iso(r["active_start"]) if r["active_start"] else None,
            "active_end": _iso(r["active_end"]) if r["active_end"] else None,
        } for r in rows]
        _push("alerts_log", data, "alert_id,content_hash,is_deleted")


def push_file_log(name, rows, size_bytes, drive_id):
    _post("collector_files", [{
        "file_name": name, "rows": rows, "size_bytes": size_bytes, "drive_file_id": drive_id,
    }], on_conflict="file_name")
