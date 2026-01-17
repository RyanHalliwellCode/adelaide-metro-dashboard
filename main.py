"""
Fetches live vehicle position data from the Adelaide Metro GTFS realtime
debug endpoint and counts how many vehicles are currently reported.

The debug endpoint returns the GTFS-realtime feed in its human-readable
text/protobuf-debug-string format rather than binary protobuf, so we can
parse it with simple regular expressions instead of a protobuf library.
"""

import json
import re
import urllib.request
from collections import Counter

DEBUG_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions/debug"
OUTPUT_PATH = "vehicles.json"

# Each vehicle entry in the debug text looks roughly like:
#
#   entity {
#     id: "V11455881003"
#     vehicle {
#       trip {
#         route_id: "190"
#       }
#       position {
#         latitude: -34.92748
#         longitude: 138.60025
#       }
#       vehicle {
#         id: "1003"
#       }
#     }
#   }
#
# Note there are TWO "vehicle {" blocks per entry (the outer one and a
# nested vehicle descriptor), so we split on "entity {" instead to get
# exactly one chunk per vehicle, then search within each chunk for the
# individual "key: value" lines we care about.
LAT_RE = re.compile(r"latitude:\s*(-?\d+\.?\d*)")
LON_RE = re.compile(r"longitude:\s*(-?\d+\.?\d*)")
ROUTE_ID_RE = re.compile(r'route_id:\s*"([^"]*)"')
VEHICLE_ID_RE = re.compile(r'vehicle\s*\{\s*id:\s*"([^"]*)"')


def fetch_debug_text(url: str) -> str:
    """Download the raw text body of the GTFS-realtime debug feed."""
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


# Authoritative route classification, taken from the `route_type` column of
# routes.txt in the GTFS static feed:
#   https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip
# route_type 2 = Rail, 0 = Tram/Light rail; everything else in the Adelaide
# feed is a bus variant (3 = Bus, 701 = Regional bus, 712 = School bus).
#
# These sets are hardcoded because the rail/tram networks are small and stable
# (11 rail + 3 tram route IDs) while the static feed is a ~17 MB download.
# Regenerate by reading route_type from routes.txt if the network changes.
#
# NOTE: do NOT infer type from the shape of the ID. Adelaide uses letter
# prefixes for *bus* corridors (G10, H33, J1, M44, W90, X30), so a rule like
# "letter-prefixed means train" misclassifies hundreds of buses as trains.
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
    """
    Split the feed on each "entity {" block and pull out the fields we
    care about from within that block.
    """
    vehicles = []

    # Split the raw text into one chunk per top-level entity (= one vehicle).
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

    # Markers need coordinates, so drop any entries missing lat/lon.
    return [v for v in vehicles if v["latitude"] is not None and v["longitude"] is not None]


def main():
    text = fetch_debug_text(DEBUG_URL)
    vehicles = parse_vehicles(text)

    for v in vehicles:
        print(v)

    print(f"\nTotal vehicles found: {len(vehicles)}")

    # Print a per-type breakdown; an implausible count here (e.g. hundreds of
    # trains on an 11-line network) is a quick signal that classification broke.
    counts = Counter(v["type"] for v in vehicles)
    for vehicle_type, count in counts.most_common():
        print(f"  {vehicle_type}: {count}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(vehicles, f, indent=2)
    print(f"Wrote static snapshot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
