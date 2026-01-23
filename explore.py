# Poke around gtfs.db from the terminal.
#   python explore.py            what's in the database
#   python explore.py lines      every train and tram line
#   python explore.py stops BEL  stops on a line, in order
#   python explore.py next BEL   next few departures from the feed's timetable

import sqlite3
import sys
from datetime import datetime

DB = "gtfs.db"


def connect():
    try:
        return sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        sys.exit(f"No {DB} yet - run 'python gtfs_static.py' first.")


def services_today(conn):
    # Without this you get every timetable at once - weekday, Saturday, Sunday
    # and school services all stacked on the same line.
    today = datetime.now()
    weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][today.weekday()]
    stamp = today.strftime("%Y%m%d")

    active = {
        row[0]
        for row in conn.execute(
            f"SELECT service_id FROM calendar WHERE {weekday}='1' AND start_date<=? AND end_date>=?",
            (stamp, stamp),
        )
    }
    # calendar_dates overrides the weekly pattern: 1 adds a day, 2 removes it.
    for service_id, exception in conn.execute(
        "SELECT service_id, exception_type FROM calendar_dates WHERE date=?", (stamp,)
    ):
        active.add(service_id) if exception == "1" else active.discard(service_id)
    return active


def summary(conn):
    print("Tables:")
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        print(f"  {name:18s} {count:>9,} rows")

    start, end, version = conn.execute(
        "SELECT feed_start_date, feed_end_date, feed_version FROM feed_info"
    ).fetchone()
    print(f"\nFeed {version}, valid {start} to {end}")

    print("\nRoutes by type:")
    rows = conn.execute("SELECT route_type, COUNT(*) FROM routes GROUP BY route_type ORDER BY 2 DESC")
    names = {"0": "tram", "2": "train", "3": "bus", "4": "ferry", "700": "bus", "701": "bus", "712": "bus"}
    for rtype, count in rows:
        print(f"  {names.get(rtype, '?'):6s} (type {rtype:3s}) {count:>4}")


def lines(conn):
    # Only trains, trams and the ferry - 700-odd bus routes isn't a useful list.
    query = """SELECT r.route_type, r.route_id, r.route_long_name, COUNT(t.trip_id)
               FROM routes r LEFT JOIN trips t ON t.route_id = r.route_id
               WHERE r.route_type IN ('0','2','4')
               GROUP BY r.route_id ORDER BY r.route_type, r.route_id"""
    names = {"0": "tram", "2": "train", "4": "ferry"}
    for rtype, rid, long_name, trips in conn.execute(query):
        print(f"  {names[rtype]:5s} {rid:8s} {long_name[:44]:46s} {trips:>5} trips")


def stops(conn, route_id):
    # Longest trip on the line, so we get the full run rather than a short one.
    # Ignore trips starting at 24:00:00+ (GTFS's way of writing after-midnight)
    # or the first stop comes out looking like "24:21:00".
    trip = conn.execute(
        """SELECT st.trip_id, COUNT(*) n FROM stop_times st
           JOIN trips t ON t.trip_id = st.trip_id
           WHERE t.route_id = ?
           GROUP BY st.trip_id HAVING MIN(st.departure_time) < '24:00:00'
           ORDER BY n DESC LIMIT 1""",
        (route_id,),
    ).fetchone()
    if not trip:
        sys.exit(f"No trips found for '{route_id}'. Try: python explore.py lines")

    query = """SELECT st.departure_time, s.stop_name FROM stop_times st
               JOIN stops s ON s.stop_id = st.stop_id
               WHERE st.trip_id = ? ORDER BY CAST(st.stop_sequence AS INTEGER)"""
    print(f"{route_id} - {trip[1]} stops:")
    for dep, name in conn.execute(query, (trip[0],)):
        print(f"  {dep}  {name}")


def next_departures(conn, route_id):
    now = datetime.now().strftime("%H:%M:%S")
    active = services_today(conn)
    if not active:
        sys.exit("No services running today - the feed may have expired.")

    # One row per trip, timed from where it starts. Listing every stop event
    # instead just gives you the same departure repeated down the line.
    placeholders = ",".join("?" * len(active))
    query = f"""SELECT MIN(st.departure_time) dep, t.trip_id, t.trip_headsign
                FROM trips t JOIN stop_times st ON st.trip_id = t.trip_id
                WHERE t.route_id = ? AND t.service_id IN ({placeholders})
                GROUP BY t.trip_id HAVING dep > ?
                ORDER BY dep LIMIT 10"""
    rows = list(conn.execute(query, (route_id, *active, now)))
    if not rows:
        sys.exit(f"Nothing scheduled after {now} today for '{route_id}'.")

    print(f"{route_id} - next departures after {now} today:")
    for dep, trip_id, headsign in rows:
        origin = conn.execute(
            """SELECT s.stop_name FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
               WHERE st.trip_id = ? ORDER BY CAST(st.stop_sequence AS INTEGER) LIMIT 1""",
            (trip_id,),
        ).fetchone()
        print(f"  {dep}  from {origin[0][:34]:36s} -> {headsign or ''}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "summary"
    route = sys.argv[2] if len(sys.argv) > 2 else None
    conn = connect()

    if command == "lines":
        lines(conn)
    elif command == "stops" and route:
        stops(conn, route)
    elif command == "next" and route:
        next_departures(conn, route)
    elif command == "summary":
        summary(conn)
    else:
        sys.exit("Usage: python explore.py [summary | lines | stops ROUTE | next ROUTE]")
