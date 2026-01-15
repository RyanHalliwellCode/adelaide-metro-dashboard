"""
Fetches live vehicle position data from the Adelaide Metro GTFS realtime
debug endpoint and counts how many vehicles are currently reported.

The debug endpoint returns the GTFS-realtime feed in its human-readable
text/protobuf-debug-string format rather than binary protobuf, so we can
parse it with simple regular expressions instead of a protobuf library.
"""

import re
import urllib.request

DEBUG_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions/debug"

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

        vehicles.append({
            "latitude": float(lat_match.group(1)) if lat_match else None,
            "longitude": float(lon_match.group(1)) if lon_match else None,
            "route_id": route_match.group(1) if route_match else None,
            "vehicle_id": vehicle_id_match.group(1) if vehicle_id_match else None,
        })

    return vehicles


def main():
    text = fetch_debug_text(DEBUG_URL)
    vehicles = parse_vehicles(text)

    for v in vehicles:
        print(v)

    print(f"\nTotal vehicles found: {len(vehicles)}")


if __name__ == "__main__":
    main()
