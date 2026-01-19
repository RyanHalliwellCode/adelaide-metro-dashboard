"""
Grabs live vehicle positions from the Adelaide Metro GTFS realtime feed.

The debug endpoint gives us plain text instead of binary protobuf, so regex
parsing is enough and we don't need a protobuf library.
"""

import json
import re
import urllib.request
from collections import Counter

DEBUG_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions/debug"
OUTPUT_PATH = "vehicles.json"

# Format: each entry has two "vehicle {" blocks, so we split on "entity {".
LAT_RE = re.compile(r"latitude:\s*(-?\d+\.?\d*)")
LON_RE = re.compile(r"longitude:\s*(-?\d+\.?\d*)")
ROUTE_ID_RE = re.compile(r'route_id:\s*"([^"]*)"')
VEHICLE_ID_RE = re.compile(r'vehicle\s*\{\s*id:\s*"([^"]*)"')


def fetch_debug_text(url: str) -> str:
    """Download the raw text body of the GTFS-realtime debug feed."""
    # CloudFront serves stale copies without this, which freezes the live map.
    request = urllib.request.Request(
        url,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
    )
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


# Taken from route_type in routes.txt, hardcoded to avoid a 17 MB download.
# Don't guess from the ID shape - G10, H33 and J1 are all buses, not trains.
RAIL_ROUTES = frozenset({
    "BEL", "FLNDRS", "GAW", "GAWC", "GRNG", "NOAR",
    "OSBORN", "OUTHA", "PTDOCK", "SALIS", "SEAFRD",
})
TRAM_ROUTES = frozenset({"BTANIC", "FESTVL", "GLNELG"})


def classify_vehicle(route_id: str) -> str:
    """Map a GTFS route_id to a broad vehicle type for map display."""
    if not route_id:
        return "unknown"
    if route_id in RAIL_ROUTES:
        return "train"
    if route_id in TRAM_ROUTES:
        return "tram"
    return "bus"


def parse_vehicles(text: str) -> list[dict]:
    """Pull the fields we care about out of each entity block."""
    vehicles = []

    blocks = text.split("entity {")[1:]  # skip the header before the first entry

    for block in blocks:
        lat_match = LAT_RE.search(block)
        lon_match = LON_RE.search(block)
        route_match = ROUTE_ID_RE.search(block)
        vehicle_id_match = VEHICLE_ID_RE.search(block)
        route_id = route_match.group(1) if route_match else None

        vehicles.append({
            "latitude": float(lat_match.group(1)) if lat_match else None,
            "longitude": float(lon_match.group(1)) if lon_match else None,
            "route_id": route_id,
            "vehicle_id": vehicle_id_match.group(1) if vehicle_id_match else None,
            "type": classify_vehicle(route_id),
        })

    # No coordinates, no marker.
    return [v for v in vehicles if v["latitude"] is not None and v["longitude"] is not None]


def main():
    text = fetch_debug_text(DEBUG_URL)
    vehicles = parse_vehicles(text)

    for v in vehicles:
        print(v)

    print(f"\nTotal vehicles found: {len(vehicles)}")

    # Sanity check - hundreds of trains would mean classification is broken.
    counts = Counter(v["type"] for v in vehicles)
    for vehicle_type, count in counts.most_common():
        print(f"  {vehicle_type}: {count}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(vehicles, f, indent=2)
    print(f"Wrote static snapshot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
