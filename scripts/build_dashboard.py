import json, os

def row(id, title, y):
    return {"id": id, "title": title, "type": "row", "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}}

def stat(id, title, query, x, y, w, h, color="blue", desc="", mappings=None):
    p = {
        "id": id, "title": title, "type": "stat", "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": {"type": "redis-datasource", "uid": "redis"},
        "targets": [{"refId": "A", "type": "command", "query": query}],
        "options": {"reduceOptions": {"calcs": ["last"]},
                    "colorMode": "background", "textMode": "value", "graphMode": "none"},
        "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": color}, "noValue": "n/a"}}
    }
    if mappings:
        p["fieldConfig"]["defaults"]["mappings"] = mappings
    return p

def tbl(id, title, query, x, y, w, h, desc="", transforms=None):
    p = {
        "id": id, "title": title, "type": "table", "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": {"type": "redis-datasource", "uid": "redis"},
        "targets": [{"refId": "A", "type": "command", "query": query}],
        "options": {"showHeader": True},
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "minWidth": 100}}}
    }
    if transforms:
        p["transformations"] = transforms
    return p

def extract(excludes, renames):
    return [
        {"id": "extractFields", "options": {"format": "json", "source": "Value"}},
        {"id": "organize", "options": {
            "excludeByName": {k: True for k in ["Value", "Time"] + excludes},
            "renameByName": renames
        }}
    ]

sunlight_map = [
    {"type": "value", "options": {"True":  {"text": "In Sunlight",  "color": "yellow", "index": 0}}},
    {"type": "value", "options": {"False": {"text": "In Shadow",    "color": "dark-blue", "index": 1}}},
]

panels = [
    row(100, "Live Satellite Tracking", 0),
    stat(1,  "ISS (25544)",         "HGET sat:meta:25544 name",            0,  1, 5, 4, "purple",
         "International Space Station — primary tracked object"),
    stat(30, "Hubble (20580)",      "HGET sat:meta:20580 name",            5,  1, 5, 4, "blue",
         "Hubble Space Telescope"),
    stat(31, "NOAA-20 (43013)",     "HGET sat:meta:43013 name",            10, 1, 5, 4, "green",
         "NOAA-20 weather/imagery satellite"),
    stat(7,  "ISS Orbit Class",     "HGET sat:pos:flat:25544 orbit_type",  15, 1, 3, 4, "purple",
         "LEO = Low Earth Orbit (<2000 km). ISS orbits at ~408 km."),
    stat(17, "ISS Sunlight",        "HGET sat:pos:flat:25544 in_sunlight", 18, 1, 3, 4, "orange",
         "Whether ISS is currently in sunlight or Earth shadow", mappings=sunlight_map),
    stat(8,  "ISS Over",            "HGET sat:pos:flat:25544 country",     21, 1, 3, 4, "teal",
         "Country or ocean the ISS is currently flying over"),

    row(101, "ISS Live Position  (updates every ~15 s via Kafka -> Spark -> Redis)", 5),
    stat(20, "Latitude (deg)",      "HGET sat:pos:flat:25544 latitude",    0,  6, 6, 5, "blue",
         "Current sub-satellite latitude. ISS range: -51.6 to +51.6 deg (orbital inclination)"),
    stat(21, "Longitude (deg)",     "HGET sat:pos:flat:25544 longitude",   6,  6, 6, 5, "blue",
         "Current sub-satellite longitude (-180 to +180 deg)"),
    stat(22, "Altitude (km)",       "HGET sat:pos:flat:25544 altitude_km", 12, 6, 6, 5, "teal",
         "Orbital altitude above Earth surface. ISS nominal range: 400-420 km"),
    stat(23, "Velocity (km/s)",     "HGET sat:pos:flat:25544 velocity_km_s", 18, 6, 6, 5, "teal",
         "Orbital velocity. ISS typical: ~7.66 km/s = ~27,600 km/h"),

    row(102, "Anomaly Detection  (Speed Layer: Spark Structured Streaming -> Redis)", 11),
    tbl(3, "Recent Anomaly Alerts", "LRANGE alerts:recent 0 14", 0, 12, 24, 8,
        "Real-time alerts from the Spark anomaly_detection job. "
        "VELOCITY_ANOMALY: speed deviated >2 sigma. "
        "ALTITUDE_ANOMALY: >5 km change within 60 s window. "
        "SPACE_WEATHER_CORRELATION: satellite crossed an active solar event zone.",
        transforms=extract(
            excludes=["alert_id"],
            renames={"alert_type": "Alert Type", "severity": "Severity",
                     "satellite_name": "Satellite", "satellite_id": "NORAD ID",
                     "detected_at": "Detected At", "source": "Source"}
        )),

    row(103, "Space Weather Events  (NASA DONKI producer -> Kafka -> Redis)", 20),
    tbl(4, "Recent Space Weather Events", "LRANGE events:recent 0 14", 0, 21, 24, 8,
        "Solar events ingested from the NASA DONKI API. "
        "CME = Coronal Mass Ejection (plasma cloud). "
        "HSS = High Speed Stream (fast solar wind). FLR = Solar Flare. "
        "When a tracked satellite enters the affected zone, a SPACE_WEATHER_CORRELATION alert is raised.",
        transforms=extract(
            excludes=["event_id"],
            renames={"event_type": "Type", "start_time": "Start Time",
                     "end_time": "End Time", "speed_km_s": "Speed (km/s)",
                     "source_location": "Source Location"}
        )),

    row(104, "System Health", 29),
    stat(6, "Redis Keys",           "DBSIZE",              0,  30, 6, 4, "blue",
         "Total keys in Redis. Reflects the volume of live data held by the speed layer."),
    stat(9, "Active Weather Events","SCARD events:active", 6,  30, 6, 4, "red",
         "Number of distinct active space weather events currently tracked."),
    tbl(88, "ISS Last Seen",        "HGET sat:meta:25544 last_seen", 12, 30, 12, 4,
        "Timestamp of the last enriched position message received for the ISS from the bridge."),

    row(105, "Batch Analytics  (Batch Layer: Airflow + Spark + MapReduce -> Redis)", 34),
    tbl(10, "Daily Aggregation - Per-Satellite Summary", "LRANGE batch:daily:list 0 -1",
        0, 35, 12, 8,
        "Output of the Spark daily_aggregation job orchestrated by Airflow (satellite_daily_pipeline DAG). "
        "Reads the full day of position data from HDFS and aggregates statistics per satellite.",
        transforms=extract(
            excludes=[],
            renames={"satellite_id": "NORAD ID", "satellite_name": "Satellite",
                     "avg_altitude_km": "Avg Alt (km)", "total_positions": "Positions",
                     "orbit_type": "Orbit", "date": "Date",
                     "countries_overflown": "Countries", "time_in_sunlight_pct": "Sunlight %"}
        )),
    tbl(11, "Weekly TLE Drift - Per-Satellite Analysis", "LRANGE batch:weekly:list 0 -1",
        12, 35, 12, 8,
        "Output of the Hadoop MapReduce TLE drift analysis (satellite_weekly_pipeline DAG). "
        "Compares Two-Line Element orbital parameters across the week to detect orbital decay or manoeuvres. "
        "anomaly_detected = true means measurable drift was observed.",
        transforms=extract(
            excludes=[],
            renames={"satellite_id": "NORAD ID", "anomaly_detected": "Anomaly?",
                     "tle_count": "TLE Snapshots", "analysis_period": "Period"}
        )),
]

dashboard = {
    "__inputs": [], "__requires": [],
    "title": "Satellite Tracker", "uid": "satellite-tracker-v1",
    "version": 3, "schemaVersion": 36,
    "refresh": "10s", "time": {"from": "now-1h", "to": "now"},
    "panels": panels
}

out = os.path.join(os.path.dirname(__file__), "..", "config", "grafana", "provisioning", "dashboards", "satellite_tracker.json")
with open(out, "w", encoding="utf-8") as f:
    json.dump(dashboard, f, indent=2, ensure_ascii=False)
print(f"Written {len(panels)} panels to {out}")
