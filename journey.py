# Point-to-point journey planning: from a pair of coordinates, work out which
# services get you there and when.
#
# Walking is part of the answer, not a detail. Every stop within range of the
# start is considered, each carrying the time it takes to reach on foot, so a
# stop five minutes further away wins if the service from it is fifteen minutes
# better. That falls out of ranking on arrival time rather than on which stop
# happens to be closest.

from datetime import date, datetime

import db

# How far people will walk at either end. Larger than a transfer radius: you
# will walk ten minutes to start a trip, but not to change mid-journey.
ORIGIN_RADIUS_METRES = 900

MIN_CHANGE_SECONDS = 150     # off one vehicle and onto the next
MAX_CHANGE_SECONDS = 2400    # past 40 minutes of waiting it isn't a connection
SEARCH_WINDOW_SECONDS = 7200  # only look two hours ahead

# Ask for far more rows than we show. A busy corridor returns dozens of
# near-identical trips on one route, and deduplicating happens after the query -
# with a small limit the whole page fills with one bus and the tram never
# appears at all.
CANDIDATE_ROWS = 400


def to_seconds(value: str | None) -> int | None:
    if not value:
        return None
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def to_clock(total: int) -> str:
    return f"{(total // 3600) % 24:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def with_platforms(conn, stops: list[dict]) -> list[dict]:
    """
    Add each station's platforms to the list.

    Stations hold no stop_times - only their platforms do - so searching from
    the station record alone finds no trains or trams at all. The platform
    inherits the station's walking time; they are metres apart.
    """
    if not stops:
        return stops
    marks = ",".join("?" * len(stops))
    children: dict[str, list] = {}
    for parent, stop_id, name in conn.execute(
        f"""SELECT parent_station, stop_id, stop_name FROM stops
            WHERE parent_station IN ({marks})""",
        [s["stop_id"] for s in stops],
    ):
        children.setdefault(parent, []).append((stop_id, name))

    expanded = []
    for stop in stops:
        expanded.append(stop)
        for stop_id, name in children.get(stop["stop_id"], ()):
            expanded.append({**stop, "stop_id": stop_id, "name": name})
    return expanded


def direct_journeys(conn, origins, destinations, services, after, limit):
    """One vehicle all the way: a trip calling at both stops, in order."""
    by_origin = {s["stop_id"]: s for s in origins}
    by_destination = {s["stop_id"]: s for s in destinations}
    rows = conn.execute(
        f"""SELECT a.stop_id, b.stop_id, a.departure_time, b.arrival_time,
                   r.route_short_name, r.route_id, r.route_type, t.trip_headsign, t.trip_id
            FROM stop_times a
            JOIN stop_times b ON b.trip_id = a.trip_id
            JOIN trips t ON t.trip_id = a.trip_id
            JOIN routes r ON r.route_id = t.route_id
            WHERE a.stop_id IN ({','.join('?' * len(by_origin))})
              AND b.stop_id IN ({','.join('?' * len(by_destination))})
              AND CAST(b.stop_sequence AS INTEGER) > CAST(a.stop_sequence AS INTEGER)
              AND t.service_id IN ({','.join('?' * len(services))})
              AND a.departure_time >= ? AND a.departure_time <= ?
            ORDER BY b.arrival_time LIMIT ?""",
        (*by_origin, *by_destination, *services, to_clock(after),
         to_clock(after + SEARCH_WINDOW_SECONDS), CANDIDATE_ROWS),
    ).fetchall()

    journeys = []
    for from_stop, to_stop, dep, arr, short, route_id, route_type, headsign, trip_id in rows:
        board, alight = by_origin[from_stop], by_destination[to_stop]
        depart = to_seconds(dep)
        # No good if the service leaves before you could walk there.
        if depart < after + board["walk_seconds"]:
            continue
        journeys.append({
            "legs": [leg(board, alight, depart, to_seconds(arr), short or route_id,
                         route_id, route_type, headsign, trip_id)],
            "walk_start": board,
            "walk_end": alight,
            "depart": depart,
            "arrive": to_seconds(arr) + alight["walk_seconds"],
            "changes": 0,
        })
    return journeys


def leg(board, alight, depart, arrive, short, route_id, route_type, headsign, trip_id):
    return {
        "route": short,
        "route_id": route_id,
        "type": db.gtfs_type_name(route_type),
        "headsign": headsign,
        "trip_id": trip_id,
        "board": board["name"],
        "board_stop": board["stop_id"],
        "alight": alight["name"],
        "alight_stop": alight["stop_id"],
        "depart": to_clock(depart),
        "arrive": to_clock(arrive),
    }


def one_change_journeys(conn, origins, destinations, services, after, limit):
    """
    Two legs. Take everything reachable from the origin and everything that
    reaches the destination, then join them wherever a change fits - including
    across a short walk, since a bus and a train never share a stop.
    """
    marks = ",".join("?" * len(services))
    by_origin = {s["stop_id"]: s for s in origins}
    by_destination = {s["stop_id"]: s for s in destinations}

    outbound = conn.execute(
        f"""SELECT a.stop_id, b.stop_id, a.departure_time, b.arrival_time,
                   r.route_short_name, r.route_id, r.route_type, t.trip_headsign, t.trip_id,
                   sb.stop_name
            FROM stop_times a
            JOIN stop_times b ON b.trip_id = a.trip_id
            JOIN stops sb ON sb.stop_id = b.stop_id
            JOIN trips t ON t.trip_id = a.trip_id
            JOIN routes r ON r.route_id = t.route_id
            WHERE a.stop_id IN ({','.join('?' * len(by_origin))})
              AND CAST(b.stop_sequence AS INTEGER) > CAST(a.stop_sequence AS INTEGER)
              AND t.service_id IN ({marks})
              AND a.departure_time >= ? AND a.departure_time <= ?""",
        (*by_origin, *services, to_clock(after), to_clock(after + SEARCH_WINDOW_SECONDS)),
    ).fetchall()
    if not outbound:
        return []

    inbound = conn.execute(
        f"""SELECT c.stop_id, d.stop_id, c.departure_time, d.arrival_time,
                   r.route_short_name, r.route_id, r.route_type, t.trip_headsign, t.trip_id,
                   sc.stop_name
            FROM stop_times c
            JOIN stop_times d ON d.trip_id = c.trip_id
            JOIN stops sc ON sc.stop_id = c.stop_id
            JOIN trips t ON t.trip_id = c.trip_id
            JOIN routes r ON r.route_id = t.route_id
            WHERE d.stop_id IN ({','.join('?' * len(by_destination))})
              AND CAST(d.stop_sequence AS INTEGER) > CAST(c.stop_sequence AS INTEGER)
              AND t.service_id IN ({marks})
              AND c.departure_time >= ? AND c.departure_time <= ?""",
        (*by_destination, *services, to_clock(after),
         to_clock(after + SEARCH_WINDOW_SECONDS)),
    ).fetchall()
    if not inbound:
        return []

    second_leg = {}
    for row in inbound:
        second_leg.setdefault(row[0], []).append(row)

    # Where you can walk to from each stop the first leg reaches.
    interchange_stops = {row[1] for row in outbound}
    walks = {}
    if interchange_stops:
        ids = list(interchange_stops)
        for start, end, metres in conn.execute(
            f"""SELECT from_stop, to_stop, metres FROM walk_transfers
                WHERE from_stop IN ({','.join('?' * len(ids))})""",
            ids,
        ):
            walks.setdefault(start, []).append((end, metres))

    journeys = []
    for from_stop, change_stop, dep1, arr1, short1, rid1, rtype1, head1, trip1, change_name in outbound:
        board = by_origin[from_stop]
        depart = to_seconds(dep1)
        if depart < after + board["walk_seconds"]:
            continue
        landed = to_seconds(arr1)

        options = [(change_stop, 0.0, change_name)] + [
            (nearby, metres, None) for nearby, metres in walks.get(change_stop, ())
        ]
        for next_stop, metres, name in options:
            walk_seconds = int(metres / db.WALK_SPEED_MPS)
            for c_stop, d_stop, dep2, arr2, short2, rid2, rtype2, head2, trip2, board2_name in second_leg.get(next_stop, ()):
                wait = to_seconds(dep2) - landed - walk_seconds
                if not (MIN_CHANGE_SECONDS <= wait <= MAX_CHANGE_SECONDS) or rid1 == rid2:
                    continue
                alight = by_destination[d_stop]
                journeys.append({
                    "legs": [
                        leg({"name": board["name"], "stop_id": from_stop},
                            {"name": change_name, "stop_id": change_stop},
                            depart, landed, short1 or rid1, rid1, rtype1, head1, trip1),
                        leg({"name": board2_name, "stop_id": c_stop},
                            {"name": alight["name"], "stop_id": d_stop},
                            to_seconds(dep2), to_seconds(arr2), short2 or rid2, rid2, rtype2, head2, trip2),
                    ],
                    "walk_start": board,
                    "walk_end": alight,
                    "change": {
                        "from": change_name,
                        "to": board2_name,
                        "metres": round(metres),
                        "wait_minutes": wait // 60,
                    },
                    "depart": depart,
                    "arrive": to_seconds(arr2) + alight["walk_seconds"],
                    "changes": 1,
                })
    return journeys


def plan(origin: tuple, destination: tuple, after: str | None = None, limit: int = 4) -> dict:
    conn = db.connect()
    try:
        services = db.services_on(conn, date.today())
        start_seconds = to_seconds(after or datetime.now().strftime("%H:%M:%S"))

        near_origin = db.stops_near(conn, origin[0], origin[1], ORIGIN_RADIUS_METRES)
        near_destination = db.stops_near(conn, destination[0], destination[1], ORIGIN_RADIUS_METRES)
        origins = with_platforms(conn, near_origin)
        destinations = with_platforms(conn, near_destination)
        if not origins or not destinations:
            return {
                "journeys": [],
                "origin_stops": near_origin,
                "destination_stops": near_destination,
                "message": "No stops within walking distance of "
                           + ("the start." if not origins else "the destination."),
            }

        found = direct_journeys(conn, origins, destinations, services, start_seconds, limit)
        # Only look for a change if there is no decent direct option.
        if len(found) < 2:
            found += one_change_journeys(conn, origins, destinations, services, start_seconds, limit)

        # Earliest arrival wins, then fewer changes, then less walking.
        found.sort(key=lambda j: (j["arrive"], j["changes"],
                                  j["walk_start"]["metres"] + j["walk_end"]["metres"]))

        # One per route combination, so the list isn't the same idea five times.
        seen, best = set(), []
        for journey in found:
            key = tuple(l["route"] for l in journey["legs"])
            if key in seen:
                continue
            seen.add(key)
            journey["depart_clock"] = to_clock(journey["depart"])
            journey["arrive_clock"] = to_clock(journey["arrive"])
            journey["total_minutes"] = (journey["arrive"] - start_seconds) // 60
            best.append(journey)
            if len(best) == limit:
                break

        return {
            "journeys": best,
            "origin_stops": near_origin[:6],
            "destination_stops": near_destination[:6],
            "searched_from": to_clock(start_seconds),
        }
    finally:
        conn.close()
