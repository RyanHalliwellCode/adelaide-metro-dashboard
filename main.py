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
        route_id = route_match.group(1) if route_match else None

        vehicles.append({
            "latitude": float(lat_match.group(1)),
            "longitude": float(lon_match.group(1)),
            "route_id": route_id,
            "vehicle_id": vehicle_id_match.group(1) if vehicle_id_match else None,
            "trip_id": trip_match.group(1) if trip_match else None,
            "bearing": float(bearing_match.group(1)) if bearing_match else None,
            "speed": float(speed_match.group(1)) if speed_match else None,
            "type": lookup.get(route_id, "unknown"),
        })

    return vehicles


def main():
    text = fetch_debug_text()
    vehicles = parse_vehicles(text, db.route_types())

    print(f"{len(vehicles)} vehicles reported at feed timestamp {parse_feed_timestamp(text)}")
    # Hundreds of trains would mean the route lookup is broken.
    for vehicle_type, count in Counter(v["type"] for v in vehicles).most_common():
        print(f"  {vehicle_type}: {count}")


if __name__ == "__main__":
    main()
