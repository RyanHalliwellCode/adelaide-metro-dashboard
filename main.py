"""
Client for the Adelaide Metro GTFS-realtime feed.

The debug endpoint gives us plain text instead of binary protobuf, so regex
parsing is enough and we don't need a protobuf library.

Run it directly for a quick check of what the feed is reporting.
"""

import re
import urllib.request
from collections import Counter

import db

DEBUG_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions/debug"
# Where the operator says each trip will actually reach each stop. Better than
# anything we can work out from a position and a timetable.
TRIP_UPDATES_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/trip_updates/debug"
# Disruptions: stop relocations, detours, works. Note the path is
# service_alerts - plain /alerts returns 403.
SERVICE_ALERTS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/service_alerts/debug"

# Format: each entry has two "vehicle {" blocks, so we split on "entity {".
LAT_RE = re.compile(r"latitude:\s*(-?\d+\.?\d*)")
LON_RE = re.compile(r"longitude:\s*(-?\d+\.?\d*)")
ROUTE_ID_RE = re.compile(r'route_id:\s*"([^"]*)"')
VEHICLE_ID_RE = re.compile(r'vehicle\s*\{\s*id:\s*"([^"]*)"')
# trip_id is the good one - it joins straight to the timetable in gtfs.db.
TRIP_ID_RE = re.compile(r'trip_id:\s*"([^"]*)"')
BEARING_RE = re.compile(r"bearing:\s*(-?\d+\.?\d*)")
SPEED_RE = re.compile(r"speed:\s*(-?\d+\.?\d*)")
HEADER_TIMESTAMP_RE = re.compile(r"timestamp:\s*(\d+)")

# Fields the feed has always sent that we used to drop on the floor. The
# accessibility and air conditioning ones live in an operator extension block,
# but there's one vehicle per entity so a plain search finds the right one.
LABEL_RE = re.compile(r'label:\s*"([^"]*)"')
OCCUPANCY_RE = re.compile(r"occupancy_status:\s*(\w+)")
WHEELCHAIR_RE = re.compile(r"wheelchair_accessible:\s*(\d+)")
AIR_CON_RE = re.compile(r"air_conditioned:\s*(\w+)")
SCHEDULE_REL_RE = re.compile(r"schedule_relationship:\s*(\w+)")

# GTFS-realtime spells these in caps; nobody wants to read that on a map.
OCCUPANCY_TEXT = {
    "EMPTY": "empty",
    "MANY_SEATS_AVAILABLE": "plenty of seats",
    "FEW_SEATS_AVAILABLE": "a few seats",
    "STANDING_ROOM_ONLY": "standing room only",
    "CRUSHED_STANDING_ROOM_ONLY": "very full",
    "FULL": "full",
    "NOT_ACCEPTING_PASSENGERS": "not taking passengers",
}


def fetch_debug_text(url: str = DEBUG_URL) -> str:
    """Download the raw text body of the GTFS-realtime debug feed."""
    # CloudFront serves stale copies without this, which freezes the live map.
    request = urllib.request.Request(
        url,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
    )
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


def parse_feed_timestamp(text: str) -> int | None:
    # Slice off the entities first or we'd match a vehicle's own timestamp.
    match = HEADER_TIMESTAMP_RE.search(text.split("entity {", 1)[0])
    return int(match.group(1)) if match else None


def parse_vehicles(text: str, route_types: dict[str, str] | None = None) -> list[dict]:
    """
    Pull the fields we care about out of each entity block. route_types comes
    from the timetable, so nothing here has to know which routes are trains.
    """
    lookup = route_types or {}
    vehicles = []

    for block in text.split("entity {")[1:]:  # skip the header before the first entry
        lat_match = LAT_RE.search(block)
        lon_match = LON_RE.search(block)
        if not lat_match or not lon_match:
            continue  # no coordinates, no marker

        route_match = ROUTE_ID_RE.search(block)
        vehicle_id_match = VEHICLE_ID_RE.search(block)
        trip_match = TRIP_ID_RE.search(block)
        bearing_match = BEARING_RE.search(block)
        speed_match = SPEED_RE.search(block)
        label_match = LABEL_RE.search(block)
        occupancy_match = OCCUPANCY_RE.search(block)
        wheelchair_match = WHEELCHAIR_RE.search(block)
        air_con_match = AIR_CON_RE.search(block)
        relationship_match = SCHEDULE_REL_RE.search(block)
        route_id = route_match.group(1) if route_match else None
        occupancy = occupancy_match.group(1) if occupancy_match else None

        vehicles.append({
            "latitude": float(lat_match.group(1)),
            "longitude": float(lon_match.group(1)),
            "route_id": route_id,
            "vehicle_id": vehicle_id_match.group(1) if vehicle_id_match else None,
            "trip_id": trip_match.group(1) if trip_match else None,
            "bearing": float(bearing_match.group(1)) if bearing_match else None,
            "speed": float(speed_match.group(1)) if speed_match else None,
            "label": label_match.group(1) if label_match else None,
            "occupancy": occupancy,
            "occupancy_text": OCCUPANCY_TEXT.get(occupancy),
            # 1 means accessible, 0 means not. Absent means the operator didn't say.
            "wheelchair": wheelchair_match.group(1) == "1" if wheelchair_match else None,
            "air_conditioned": air_con_match.group(1) == "true" if air_con_match else None,
            "schedule_relationship": relationship_match.group(1) if relationship_match else None,
            "type": lookup.get(route_id, "unknown"),
        })

    return vehicles


# One entity per trip, each holding a run of stop_time_update blocks. The four
# leading spaces before the closing brace anchor to the right nesting level.
STOP_TIME_UPDATE_RE = re.compile(r"stop_time_update \{(.*?)\n    \}", re.S)
STOP_ID_RE = re.compile(r'stop_id:\s*"([^"]*)"')
EPOCH_RE = re.compile(r"time:\s*(\d+)")


# Anchored to the start of a line on purpose: "informed_entity {" also contains
# "entity {", and splitting naively turns 60 alerts into 402 fragments.
ENTITY_SPLIT_RE = re.compile(r"^entity \{", re.M)
ALERT_ID_RE = re.compile(r'id:\s*"([^"]*)"')
ALERT_ROUTE_RE = re.compile(r'informed_entity \{\s*route_id:\s*"([^"]*)"')
ALERT_STOP_RE = re.compile(r'informed_entity \{[^}]*stop_id:\s*"([^"]*)"')
ACTIVE_START_RE = re.compile(r"active_period \{\s*(?:start:\s*(\d+))?")
ACTIVE_END_RE = re.compile(r"active_period \{[^}]*?end:\s*(\d+)", re.S)
CAUSE_RE = re.compile(r"cause:\s*(\w+)")
EFFECT_RE = re.compile(r"effect:\s*(\w+)")
# Each text block is a translation wrapper around the string we actually want.
HEADER_TEXT_RE = re.compile(r'header_text \{\s*translation \{\s*text:\s*"((?:[^"\\]|\\.)*)"', re.S)
DESCRIPTION_RE = re.compile(r'description_text \{\s*translation \{\s*text:\s*"((?:[^"\\]|\\.)*)"', re.S)
URL_TEXT_RE = re.compile(r'url \{\s*translation \{\s*text:\s*"((?:[^"\\]|\\.)*)"', re.S)
TAG_RE = re.compile(r"<[^>]+>")


def clean_alert_text(raw: str) -> str:
    """Descriptions arrive as escaped HTML; the panel wants readable text."""
    text = raw.replace('\\"', '"').replace("\\n", " ").replace("\\/", "/")
    text = TAG_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&#39;", "'")
    return " ".join(text.split())


def parse_alerts(text: str) -> list[dict]:
    """Live disruptions, each tagged with the routes it affects."""
    alerts = []

    for block in ENTITY_SPLIT_RE.split(text)[1:]:
        header = HEADER_TEXT_RE.search(block)
        if not header:
            continue
        description = DESCRIPTION_RE.search(block)
        url = URL_TEXT_RE.search(block)
        alert_id = ALERT_ID_RE.search(block)
        start = ACTIVE_START_RE.search(block)
        end = ACTIVE_END_RE.search(block)
        cause = CAUSE_RE.search(block)
        effect = EFFECT_RE.search(block)

        alerts.append({
            "id": alert_id.group(1) if alert_id else None,
            "header": clean_alert_text(header.group(1)),
            "description": clean_alert_text(description.group(1)) if description else None,
            "url": url.group(1).replace("\\/", "/") if url else None,
            "routes": sorted(set(ALERT_ROUTE_RE.findall(block))),
            "stops": sorted(set(ALERT_STOP_RE.findall(block))),
            "starts": int(start.group(1)) if start and start.group(1) else None,
            "ends": int(end.group(1)) if end else None,
            "cause": cause.group(1) if cause else None,
            "effect": effect.group(1) if effect else None,
        })

    return alerts


def parse_trip_updates(text: str) -> dict[str, dict[str, int]]:
    """trip_id -> stop_id -> predicted arrival, as a unix timestamp."""
    predictions: dict[str, dict[str, int]] = {}

    for block in text.split("entity {")[1:]:
        trip_match = TRIP_ID_RE.search(block)
        if not trip_match:
            continue
        per_stop = {}
        for chunk in STOP_TIME_UPDATE_RE.findall(block):
            stop_match = STOP_ID_RE.search(chunk)
            epoch_match = EPOCH_RE.search(chunk)
            if stop_match and epoch_match:
                per_stop[stop_match.group(1)] = int(epoch_match.group(1))
        if per_stop:
            predictions[trip_match.group(1)] = per_stop

    return predictions


def main():
    text = fetch_debug_text()
    vehicles = parse_vehicles(text, db.route_types())

    print(f"{len(vehicles)} vehicles reported at feed timestamp {parse_feed_timestamp(text)}")
    # Hundreds of trains would mean the route lookup is broken.
    for vehicle_type, count in Counter(v["type"] for v in vehicles).most_common():
        print(f"  {vehicle_type}: {count}")


if __name__ == "__main__":
    main()
