import os
import sqlite3
import secrets
import urllib.request
import json
from math import ceil, trunc
from functools import wraps
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, g, jsonify
)
from werkzeug.security import generate_password_hash, check_password_hash

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

DATABASE = os.path.join(os.path.dirname(__file__), "f1fantasy.db")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")
F1_API_BASE = "https://api.jolpi.ca/ergast/f1"
CURRENT_SEASON = 2026
TURBO_SALARY_CAP = 18.0  # Only drivers under this price can be turbo (GridRivals rule)

TEAM_COLORS = {
    "Red Bull": "#3671C6",
    "Ferrari": "#E8002D",
    "McLaren": "#FF8000",
    "Mercedes": "#27F4D2",
    "Aston Martin": "#229971",
    "Alpine": "#FF87BC",
    "Racing Bulls": "#6692FF",
    "Audi": "#ff0000",
    "Williams": "#64C4FF",
    "Haas": "#B6BABD",
    "Cadillac": "#c0a44d",
}


def driver_slug(name):
    return name.lower().replace(" ", "-").replace(".", "")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    db.executescript(SCHEMA)
    # Run migrations for columns that may not exist yet
    migrations = [
        ("drivers", "portrait", "TEXT"),
        ("races", "race_datetime", "TEXT NOT NULL DEFAULT ''"),
        ("races", "quali_datetime", "TEXT NOT NULL DEFAULT ''"),
        ("races", "quali_locked", "INTEGER NOT NULL DEFAULT 0"),
        ("race_results", "quali_pos", "INTEGER"),
        ("race_results", "laps", "INTEGER NOT NULL DEFAULT 0"),
        ("race_results", "total_laps", "INTEGER NOT NULL DEFAULT 0"),
        ("user_teams", "lock_duration", "INTEGER NOT NULL DEFAULT 1"),
        ("user_teams", "lock_remaining", "INTEGER NOT NULL DEFAULT 1"),
        ("user_teams", "on_cooldown", "INTEGER NOT NULL DEFAULT 0"),
    ]
    for table, col, col_type in migrations:
        try:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # column already exists
    db.commit()


def migrate_2026_calendar(db):
    """One-time migration to fix 2026 race calendar dates and names."""
    # Check if migration is needed by looking at round 7 name
    r7 = db.execute("SELECT name FROM races WHERE round = 7").fetchone()
    if not r7 or r7["name"] == "Canadian Grand Prix":
        return  # already correct or no races
    updates = [
        (1,  "Australian Grand Prix",          "AUS", "2026-03-08T04:00", "2026-03-07T05:00"),
        (2,  "Chinese Grand Prix",             "CHN", "2026-03-15T07:00", "2026-03-14T07:00"),
        (3,  "Japanese Grand Prix",            "JPN", "2026-03-29T05:00", "2026-03-28T06:00"),
        (4,  "Bahrain Grand Prix",             "BHR", "2026-04-12T15:00", "2026-04-11T16:00"),
        (5,  "Saudi Arabian Grand Prix",       "KSA", "2026-04-19T17:00", "2026-04-18T17:00"),
        (6,  "Miami Grand Prix",               "USA", "2026-05-03T20:00", "2026-05-02T20:00"),
        (7,  "Canadian Grand Prix",            "CAN", "2026-05-24T20:00", "2026-05-23T20:00"),
        (8,  "Monaco Grand Prix",              "MON", "2026-06-07T13:00", "2026-06-06T14:00"),
        (9,  "Barcelona-Catalunya Grand Prix", "ESP", "2026-06-14T13:00", "2026-06-13T14:00"),
        (10, "Austrian Grand Prix",            "AUT", "2026-06-28T13:00", "2026-06-27T14:00"),
        (11, "British Grand Prix",             "GBR", "2026-07-05T14:00", "2026-07-04T15:00"),
        (12, "Belgian Grand Prix",             "BEL", "2026-07-19T13:00", "2026-07-18T14:00"),
        (13, "Hungarian Grand Prix",           "HUN", "2026-07-26T13:00", "2026-07-25T14:00"),
        (14, "Dutch Grand Prix",               "NED", "2026-08-23T13:00", "2026-08-22T14:00"),
        (15, "Italian Grand Prix",             "ITA", "2026-09-06T13:00", "2026-09-05T14:00"),
        (16, "Spanish Grand Prix",             "ESP", "2026-09-13T13:00", "2026-09-12T14:00"),
        (17, "Azerbaijan Grand Prix",          "AZE", "2026-09-26T11:00", "2026-09-25T12:00"),
        (18, "Singapore Grand Prix",           "SGP", "2026-10-11T12:00", "2026-10-10T13:00"),
        (19, "United States Grand Prix",       "USA", "2026-10-25T20:00", "2026-10-24T21:00"),
        (20, "Mexico City Grand Prix",         "MEX", "2026-11-01T20:00", "2026-10-31T21:00"),
        (21, "São Paulo Grand Prix",           "BRA", "2026-11-08T17:00", "2026-11-07T18:00"),
        (22, "Las Vegas Grand Prix",           "USA", "2026-11-22T04:00", "2026-11-21T04:00"),
        (23, "Qatar Grand Prix",               "QAT", "2026-11-29T16:00", "2026-11-28T18:00"),
        (24, "Abu Dhabi Grand Prix",           "ARE", "2026-12-06T13:00", "2026-12-05T14:00"),
    ]
    for rnd, name, country, race_dt, quali_dt in updates:
        db.execute("""
            UPDATE races SET name = ?, country = ?, race_datetime = ?, quali_datetime = ?
            WHERE round = ?
        """, (name, country, race_dt, quali_dt, rnd))

    # Sprint weekends: lock at sprint qualifying start (Friday) instead of regular quali
    sprint_lock_times = [
        (2,  "2026-03-13T07:30"),  # Chinese GP
        (6,  "2026-05-01T20:30"),  # Miami GP
        (7,  "2026-05-22T20:30"),  # Canadian GP
        (11, "2026-07-03T15:30"),  # British GP
        (14, "2026-08-21T14:30"),  # Dutch GP
        (18, "2026-10-09T12:30"),  # Singapore GP
    ]
    for rnd, sq_dt in sprint_lock_times:
        db.execute("UPDATE races SET quali_datetime = ? WHERE round = ?", (sq_dt, rnd))

    db.commit()


def migrate_sprint_lock_times(db):
    """Set quali_datetime to sprint qualifying time for sprint weekends."""
    # Check if already applied by looking at R2's quali_datetime
    r2 = db.execute("SELECT quali_datetime FROM races WHERE round = 2").fetchone()
    if not r2 or r2["quali_datetime"] == "2026-03-13T07:30":
        return  # already correct or no races
    sprint_lock_times = [
        (2,  "2026-03-13T07:30"),  # Chinese GP
        (6,  "2026-05-01T20:30"),  # Miami GP
        (7,  "2026-05-22T20:30"),  # Canadian GP
        (11, "2026-07-03T15:30"),  # British GP
        (14, "2026-08-21T14:30"),  # Dutch GP
        (18, "2026-10-09T12:30"),  # Singapore GP
    ]
    for rnd, sq_dt in sprint_lock_times:
        db.execute("UPDATE races SET quali_datetime = ? WHERE round = ?", (sq_dt, rnd))
    db.commit()


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    username    TEXT UNIQUE NOT NULL,
    password    TEXT NOT NULL,
    budget      REAL NOT NULL DEFAULT 100.0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS drivers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    team            TEXT NOT NULL,
    price           REAL NOT NULL,
    price_change    REAL NOT NULL DEFAULT 0,
    points          INTEGER NOT NULL DEFAULT 0,
    number          INTEGER,
    country         TEXT,
    portrait        TEXT
);

CREATE TABLE IF NOT EXISTS constructors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    price           REAL NOT NULL,
    price_change    REAL NOT NULL DEFAULT 0,
    points          INTEGER NOT NULL DEFAULT 0,
    color           TEXT DEFAULT '#ffffff'
);

CREATE TABLE IF NOT EXISTS races (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    country         TEXT NOT NULL,
    round           INTEGER NOT NULL,
    race_datetime   TEXT NOT NULL,
    quali_datetime  TEXT NOT NULL,
    completed       INTEGER NOT NULL DEFAULT 0,
    quali_locked    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS race_results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id     INTEGER NOT NULL REFERENCES races(id),
    driver_id   INTEGER NOT NULL REFERENCES drivers(id),
    position    INTEGER,
    grid        INTEGER,
    quali_pos   INTEGER,
    fastest_lap INTEGER NOT NULL DEFAULT 0,
    status      TEXT,
    dnf         INTEGER NOT NULL DEFAULT 0,
    laps        INTEGER NOT NULL DEFAULT 0,
    total_laps  INTEGER NOT NULL DEFAULT 0,
    sprint_pos  INTEGER
);

CREATE TABLE IF NOT EXISTS preseason_results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id     INTEGER NOT NULL DEFAULT 0,
    driver_id   INTEGER NOT NULL REFERENCES drivers(id),
    position    INTEGER
);

CREATE TABLE IF NOT EXISTS user_teams (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    driver_id       INTEGER REFERENCES drivers(id),
    constructor_id  INTEGER REFERENCES constructors(id),
    is_turbo        INTEGER NOT NULL DEFAULT 0,
    slot            INTEGER NOT NULL,
    lock_duration   INTEGER NOT NULL DEFAULT 1,
    lock_remaining  INTEGER NOT NULL DEFAULT 1,
    on_cooldown     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS race_picks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    race_id         INTEGER NOT NULL REFERENCES races(id),
    driver_id       INTEGER REFERENCES drivers(id),
    constructor_id  INTEGER REFERENCES constructors(id),
    is_turbo        INTEGER NOT NULL DEFAULT 0,
    slot            INTEGER NOT NULL,
    UNIQUE(user_id, race_id, slot)
);

CREATE TABLE IF NOT EXISTS user_scores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    race_id     INTEGER NOT NULL REFERENCES races(id),
    points      INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, race_id)
);

CREATE TABLE IF NOT EXISTS pick_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    race_id         INTEGER NOT NULL REFERENCES races(id),
    driver_id       INTEGER REFERENCES drivers(id),
    points          INTEGER NOT NULL DEFAULT 0,
    is_turbo        INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, race_id, driver_id)
);

CREATE TABLE IF NOT EXISTS pick_constructor_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    race_id         INTEGER NOT NULL REFERENCES races(id),
    constructor_id  INTEGER REFERENCES constructors(id),
    points          INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, race_id, constructor_id)
);
"""

# ---------------------------------------------------------------------------
# Scoring – GridRivals style (from official notebook)
# ---------------------------------------------------------------------------

# Race finish: P1=100, -3 per pos (extended to P22)
RACE_FINISH_PTS = {i: 100 - (i - 1) * 3 for i in range(1, 23)}

# Qualifying: P1=50, -2 per pos (extended to P22)
QUALI_PTS = {i: 50 - (i - 1) * 2 for i in range(1, 23)}

# Sprint race: P1=20, -1 per pos down to P20=1 (P21/P22 = 0)
SPRINT_PTS = {i: max(0, 21 - i) for i in range(1, 23)}

# Overtake points: 3 per net position gained (quali pos vs race finish)
OVERTAKE_PTS_PER_POS = 3

# Beating teammate: keyed by gap (teammate_pos - driver_pos)
TEAMMATE_PTS = {}
for _gap in range(1, 4):
    TEAMMATE_PTS[_gap] = 2
for _gap in range(4, 8):
    TEAMMATE_PTS[_gap] = 5
for _gap in range(8, 13):
    TEAMMATE_PTS[_gap] = 8
for _gap in range(13, 23):
    TEAMMATE_PTS[_gap] = 12

# PI (Performance Index): 8-race rolling avg vs current finish (2026 values)
PI_PTS = {0: 0, 1: 0, 2: 2, 3: 4, 4: 6, 5: 9, 6: 12, 7: 16, 8: 20, 9: 25}
for _pi in range(10, 23):
    PI_PTS[_pi] = 30  # capped at 30

# Constructor/Team scoring: per-driver position mapped, then summed
CONSTRUCTOR_RACE_PTS = {i: 60 - (i - 1) * 2 for i in range(1, 23)}
CONSTRUCTOR_RACE_PTS[0] = 0
CONSTRUCTOR_QUALI_PTS = {0: 0}
for i in range(1, 23):
    CONSTRUCTOR_QUALI_PTS[i] = 30 - (i - 1)

def calc_completion_pts(laps, total_laps):
    """Points for completing race distance milestones. Uses (laps+1)/maxlaps."""
    if not total_laps or total_laps == 0:
        return 0
    pct = (laps + 1) / total_laps
    if pct >= 0.9:
        return 12
    elif pct >= 0.75:
        return 9
    elif pct >= 0.5:
        return 6
    elif pct >= 0.25:
        return 3
    return 0


def calc_teammate_pts(driver_pos, teammate_pos):
    """Points for beating teammate. Only the higher-finishing driver gets points."""
    if not driver_pos or not teammate_pos or driver_pos >= teammate_pos:
        return 0
    gap = teammate_pos - driver_pos
    return TEAMMATE_PTS.get(gap, 12)  # 12 for any gap >= 13


def calc_pi_pts(db, race_id, driver_id, current_finish):
    """Performance Index points: 8-race rolling avg finish vs current finish."""
    if not current_finish:
        return 0
    total = 0
    for i in range(race_id - 8, race_id):
        if i > 1:
            #use actual race results
            total += db.execute(f"SELECT position FROM race_results WHERE driver_id = {driver_id} AND race_id = {i}").fetchall()[0][0]
        else:
            #use initial ranking
            total += db.execute(f"SELECT position FROM preseason_results WHERE driver_id = {driver_id} AND race_id = 0").fetchall()[0][0]

    avg = ceil(total/8)
    pi_finish = max(0, avg - current_finish)
    return PI_PTS.get(pi_finish, 0)


def calc_driver_race_points(result, teammate_finish=None, db=None):
    """Calculate total GridRivals-style points for a driver in a race."""
    pts = 0
    pos = result["position"]
    quali_pos = result["quali_pos"]
    grid_pos = result["grid"]

    # Race finish points. DNS or DSQ = 0 points
    if pos and result["status"] not in ['Did not start', 'Disqualified']:
        pts += RACE_FINISH_PTS.get(pos, max(0, 100 - (pos - 1) * 3))

    # Qualifying points
    if quali_pos:
        pts += QUALI_PTS.get(quali_pos, max(0, 50 - (quali_pos - 1) * 2))

    # Overtake points (starting pos vs race finish, only gains)
    if pos and grid_pos:
        gained = grid_pos - pos
        if gained > 0:
            pts += gained * OVERTAKE_PTS_PER_POS

    # Sprint race
    if result["sprint_pos"]:
        sp = result["sprint_pos"]
        pts += SPRINT_PTS.get(sp, 0)

    # Completion points
    pts += calc_completion_pts(result["laps"], result["total_laps"])

    # Beating teammate
    if teammate_finish:
        pts += calc_teammate_pts(pos, teammate_finish)

    # PI points (requires db for historical lookup)
    if db and pos:
        pts += calc_pi_pts(db, result["race_id"], result["driver_id"], pos)

    return pts


def calc_constructor_race_points(driver_positions, driver_quali_positions):
    """Constructor points: map each driver's position individually, then sum."""
    pts = 0
    for p in driver_positions:
        if p:
            pts += CONSTRUCTOR_RACE_PTS.get(p, max(0, 60 - (p - 1) * 2))
    for p in driver_quali_positions:
        if p:
            pts += CONSTRUCTOR_QUALI_PTS.get(p, max(0, 30 - (p - 1)))
    return pts


def update_driver_prices(db, race_id):
    """Adjust driver prices based on race performance."""

    base_salaries = {
        1: 34,
        2: 32.4,
        3: 30.8,
        4: 29.2,
        5: 27.6,
        6: 26,
        7: 24.4,
        8: 22.8,
        9: 21.2,
        10: 19.6,
        11: 18,
        12: 16.4,
        13: 14.8,
        14: 13.2,
        15: 11.6,
        16: 10,
        17: 8.4,
        18: 6.8,
        19: 5.2,
        20: 3.60,
        21: 2,
        22: 0.4
    }

    #caluclate change in driver salary based on performance
    results = db.execute("SELECT id, price, price_change, points FROM drivers").fetchall()
    sorted_results = sorted(results, key=lambda results: results['points'], reverse=True)
    for i in range(0, len(sorted_results)):
        #previous salary - base salary for position
        value = (base_salaries[i+1] - sorted_results[i][1])/4
        rounded_value = trunc(value * 10) / 10
        if rounded_value > 2:
            rounded_value = 2
        elif rounded_value < -2:
            rounded_value = -2
        #update database
        db.execute("UPDATE drivers SET price = price + ?, price_change = ? WHERE id = ?",
                        (rounded_value, rounded_value, sorted_results[i]["id"])
         )
    db.commit()

    base_constructiors = {
    1: 30,
    2: 27.4,
    3: 24.8,
    4: 22.2,
    5: 19.6,
    6: 17,
    7: 14.4,
    8: 11.8,
    9: 9.2,
    10: 6.6,
    11: 4
    }

    #calculate change in driver salry based on performance
    results = db.execute("SELECT id, price, points FROM constructors").fetchall()
    sorted_results = sorted(results, key=lambda results: results['points'], reverse=True)
    for i in range(0, len(sorted_results)):
        #previous salary - base salary for position
        value = (base_constructiors[i+1] - sorted_results[i][1])/4
        rounded_value = trunc(value * 10) / 10
        if rounded_value > 3:
            rounded_value = 3
        elif rounded_value < -3:
            rounded_value = -3
        #updata database
        db.execute("UPDATE constructors SET price = price + ?, price_change = ? WHERE id = ?",
                        (rounded_value, rounded_value, sorted_results[i]["id"])
        )
    db.commit()

    #update driver cost for everyone who has them
    results = db.execute("SELECT * FROM drivers").fetchall()
    for r in results:
        pick_users = db.execute(
                    "SELECT DISTINCT user_id FROM race_picks WHERE race_id = ? AND driver_id = ?",
                    (race_id, r["id"])
        ).fetchall()
        if not pick_users:
            pick_users = db.execute(
                "SELECT DISTINCT user_id FROM user_teams WHERE driver_id = ? AND on_cooldown = 0",
                    (r["id"],)
        ).fetchall()
        
        for u in pick_users:
            db.execute(
                "UPDATE users SET budget = budget + ? WHERE id = ?",
                    (r["price_change"], u["user_id"])
        )

    #update constructor cost for everyone who has them
    results = db.execute("SELECT * FROM constructors").fetchall()
    for r in results:
        pick_users = db.execute(
                    "SELECT DISTINCT user_id FROM race_picks WHERE race_id = ? AND constructor_id = ?",
                    (race_id, r["id"])
        ).fetchall()
        if not pick_users:
            pick_users = db.execute(
                "SELECT DISTINCT user_id FROM user_teams WHERE constructor_id = ? AND on_cooldown = 0",
                    (r["id"],)
        ).fetchall()
        for u in pick_users:
            db.execute(
                "UPDATE users SET budget = budget + ? WHERE id = ?",
                    (r["price_change"], u["user_id"])
        )
            
        


# ---------------------------------------------------------------------------
# F1 API – auto-fetch results
# ---------------------------------------------------------------------------

def _api_fetch(url):
    """Helper to fetch JSON from the F1 API."""
    req = urllib.request.Request(url, headers={"User-Agent": "F1Fantasy/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_race_results(round_num):
    """Fetch race results from the Jolpica F1 API."""
    url = f"{F1_API_BASE}/{CURRENT_SEASON}/{round_num}/results.json"
    try:
        data = _api_fetch(url)
    except Exception as e:
        return None, str(e)

    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        return None, "No results available yet for this round."

    race_data = races[0]
    results = race_data.get("Results", [])
    if not results:
        return None, "No results available yet for this round."

    # Determine total laps from the winner
    total_laps = int(results[0].get("laps", 0)) if results else 0

    parsed = []
    for r in results:
        driver_name = f"{r['Driver']['givenName']} {r['Driver']['familyName']}"
        status = r.get("status", "")
        is_dnf = status not in ("Finished", "") and "Lap" not in status
        fastest_lap_rank = None
        fl = r.get("FastestLap")
        if fl:
            fastest_lap_rank = int(fl.get("rank", 0))

        parsed.append({
            "driver_name": driver_name,
            "driver_number": int(r.get("number", 0)),
            "position": int(r.get("position", 0)),
            "grid": int(r.get("grid", 0)),
            "laps": int(r.get("laps", 0)),
            "total_laps": total_laps,
            "status": status,
            "dnf": is_dnf,
            "fastest_lap": fastest_lap_rank == 1,
        })

    return parsed, None


def fetch_qualifying_results(round_num):
    """Fetch qualifying results. Returns {driver_number: quali_position}."""
    url = f"{F1_API_BASE}/{CURRENT_SEASON}/{round_num}/qualifying.json"
    try:
        data = _api_fetch(url)
    except Exception:
        return {}

    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        return {}

    quali_map = {}
    for r in races[0].get("QualifyingResults", []):
        num = int(r.get("number", 0))
        pos = int(r.get("position", 0))
        quali_map[num] = pos
    return quali_map


def fetch_sprint_results(round_num):
    """Fetch sprint results. Returns {driver_name: sprint_position}."""
    url = f"{F1_API_BASE}/{CURRENT_SEASON}/{round_num}/sprint.json"
    try:
        data = _api_fetch(url)
    except Exception:
        return {}

    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        return {}

    sprint_map = {}
    for r in races[0].get("SprintResults", []):
        name = f"{r['Driver']['givenName']} {r['Driver']['familyName']}"
        pos = int(r.get("position", 0))
        sprint_map[name] = pos
    return sprint_map


def match_driver_by_number(db, number):
    """Match an API driver to our DB by car number."""
    row = db.execute("SELECT id FROM drivers WHERE number = ?", (number,)).fetchone()
    return row["id"] if row else None


def fetch_official_driver_standings():
    """Fetch official F1 driver standings from Jolpica API."""
    url = f"{F1_API_BASE}/{CURRENT_SEASON}/driverStandings.json"
    try:
        data = _api_fetch(url)
        standings_list = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not standings_list:
            return []
        entries = standings_list[0]["DriverStandings"]
        result = []
        for i, e in enumerate(entries):
            pos = e.get("position", e.get("positionText", str(i + 1)))
            try:
                pos = int(pos)
            except (ValueError, TypeError):
                pos = i + 1
            result.append({
                "position": pos,
                "name": f"{e['Driver']['givenName']} {e['Driver']['familyName']}",
                "team": e["Constructors"][0]["name"] if e.get("Constructors") else "",
                "points": float(e["points"]),
            })
        return result
    except Exception as e:
        print(f"[standings] Driver standings error: {e}")
        return []


def fetch_official_constructor_standings():
    """Fetch official F1 constructor standings from Jolpica API."""
    url = f"{F1_API_BASE}/{CURRENT_SEASON}/constructorStandings.json"
    try:
        data = _api_fetch(url)
        standings_list = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not standings_list:
            return []
        entries = standings_list[0]["ConstructorStandings"]
        result = []
        for i, e in enumerate(entries):
            pos = e.get("position", e.get("positionText", str(i + 1)))
            try:
                pos = int(pos)
            except (ValueError, TypeError):
                pos = i + 1
            result.append({
                "position": pos,
                "name": e["Constructor"]["name"],
                "points": float(e["points"]),
            })
        return result
    except Exception as e:
        print(f"[standings] Constructor standings error: {e}")
        return []


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

def get_locked_out_drivers(db, user_id):
    rows = db.execute(
        "SELECT driver_id FROM user_teams WHERE user_id = ? AND on_cooldown = 1 AND driver_id IS NOT NULL",
        (user_id,)
    ).fetchall()
    return {r["driver_id"] for r in rows}


def get_locked_out_constructors(db, user_id):
    rows = db.execute(
        "SELECT constructor_id FROM user_teams WHERE user_id = ? AND on_cooldown = 1 AND constructor_id IS NOT NULL",
        (user_id,)
    ).fetchall()
    return {r["constructor_id"] for r in rows}


def is_lineup_locked(db):
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    # Locked if quali time has passed and not manually unlocked (quali_locked = -1)
    # quali_locked: 0 = default (auto), 1 = manually locked, -1 = manually unlocked
    row = db.execute(
        "SELECT COUNT(*) as c FROM races WHERE completed = 0 AND quali_locked != -1 AND quali_datetime <= ?",
        (now_utc,)
    ).fetchone()
    return row["c"] > 0


def process_lock_decrements(db, race_id):
    # Delete cooldown rows (drivers and constructors)
    cooldown_rows = db.execute(
        "SELECT id FROM user_teams WHERE on_cooldown = 1"
    ).fetchall()
    for row in cooldown_rows:
        db.execute("DELETE FROM user_teams WHERE id = ?", (row["id"],))

    # Decrement lock_remaining for all active contracts
    db.execute("""
        UPDATE user_teams SET lock_remaining = lock_remaining - 1
        WHERE on_cooldown = 0 AND lock_remaining > 0
    """)

    # Move expired contracts to cooldown
    db.execute("""
        UPDATE user_teams SET on_cooldown = 1
        WHERE lock_remaining <= 0 AND on_cooldown = 0
    """)


# ---------------------------------------------------------------------------
# Seed data – 2026 season
# ---------------------------------------------------------------------------

def seed_data():
    db = get_db()

    if db.execute("SELECT COUNT(*) FROM drivers").fetchone()[0] > 0:
        return

    # 2026 grid – 11 teams, 22 drivers
    drivers = [
        # (name, team, price, number, country)
        ("Max Verstappen",      "Red Bull",      30.0,  3, "NED"),
        ("Isack Hadjar",        "Red Bull",      19.6,  6, "FRA"),
        ("Lewis Hamilton",      "Ferrari",       20.9, 44, "GBR"),
        ("Charles Leclerc",     "Ferrari",       23.5, 16, "MON"),
        ("Lando Norris",        "McLaren",       27.4,  1, "GBR"),
        ("Oscar Piastri",       "McLaren",       26.1, 81, "AUS"),
        ("George Russell",      "Mercedes",      28.7, 63, "GBR"),
        ("Kimi Antonelli",      "Mercedes",      24.8, 12, "ITA"),
        ("Fernando Alonso",     "Aston Martin",  22.2, 14, "ESP"),
        ("Lance Stroll",        "Aston Martin",   17.0, 18, "CAN"),
        ("Pierre Gasly",        "Alpine",        18.3, 10, "FRA"),
        ("Franco Colapinto",    "Alpine",         4.7, 43, "ARG"),
        ("Liam Lawson",         "Racing Bulls",  14.4, 30, "NZL"),
        ("Arvid Lindblad",      "Racing Bulls",   4.7, 41, "GBR"),
        ("Nico Hulkenberg",     "Audi",          11.8, 27, "GER"),
        ("Gabriel Bortoleto",   "Audi",           10.5,  5, "BRA"),
        ("Alexander Albon",     "Williams",      13.1, 23, "THA"),
        ("Carlos Sainz",        "Williams",      15.7, 55, "ESP"),
        ("Oliver Bearman",      "Haas",           9.2, 87, "GBR"),
        ("Esteban Ocon",        "Haas",          7.9, 31, "FRA"),
        ("Sergio Perez",        "Cadillac",       4.7, 11, "MEX"),
        ("Valtteri Bottas",     "Cadillac",       4.7, 77, "FIN"),
    ]

    constructors_data = [
        ("Red Bull",      25.0, "#3671C6"),
        ("Ferrari",       22.5, "#E8002D"),
        ("McLaren",       28.5, "#FF8000"),
        ("Mercedes",      28.5, "#27F4D2"),
        ("Aston Martin",  20.0, "#229971"),
        ("Alpine",        12.5, "#FF87BC"),
        ("Racing Bulls",  15.0, "#6692FF"),
        ("Audi",           10.0, "#ff0000"),
        ("Williams",      17.5, "#64C4FF"),
        ("Haas",           5.0, "#B6BABD"),
        ("Cadillac",       7.5, "#c0a44d"),
    ]

    # 2026 race calendar with race and qualifying datetimes (UTC)
    races_data = [
        # (name, country, round, race_datetime, quali_datetime)
        ("Australian Grand Prix",           "AUS", 1,  "2026-03-08T04:00", "2026-03-07T05:00"),
        ("Chinese Grand Prix",              "CHN", 2,  "2026-03-15T07:00", "2026-03-13T07:30"),  # Sprint: lock at SQ
        ("Japanese Grand Prix",             "JPN", 3,  "2026-03-29T05:00", "2026-03-28T06:00"),
        ("Bahrain Grand Prix",              "BHR", 4,  "2026-04-12T15:00", "2026-04-11T16:00"),
        ("Saudi Arabian Grand Prix",        "KSA", 5,  "2026-04-19T17:00", "2026-04-18T17:00"),
        ("Miami Grand Prix",                "USA", 6,  "2026-05-03T20:00", "2026-05-01T20:30"),  # Sprint: lock at SQ
        ("Canadian Grand Prix",             "CAN", 7,  "2026-05-24T20:00", "2026-05-22T20:30"),  # Sprint: lock at SQ
        ("Monaco Grand Prix",               "MON", 8,  "2026-06-07T13:00", "2026-06-06T14:00"),
        ("Barcelona-Catalunya Grand Prix",  "ESP", 9,  "2026-06-14T13:00", "2026-06-13T14:00"),
        ("Austrian Grand Prix",             "AUT", 10, "2026-06-28T13:00", "2026-06-27T14:00"),
        ("British Grand Prix",              "GBR", 11, "2026-07-05T14:00", "2026-07-03T15:30"),  # Sprint: lock at SQ
        ("Belgian Grand Prix",              "BEL", 12, "2026-07-19T13:00", "2026-07-18T14:00"),
        ("Hungarian Grand Prix",            "HUN", 13, "2026-07-26T13:00", "2026-07-25T14:00"),
        ("Dutch Grand Prix",                "NED", 14, "2026-08-23T13:00", "2026-08-21T14:30"),  # Sprint: lock at SQ
        ("Italian Grand Prix",              "ITA", 15, "2026-09-06T13:00", "2026-09-05T14:00"),
        ("Spanish Grand Prix",              "ESP", 16, "2026-09-13T13:00", "2026-09-12T14:00"),
        ("Azerbaijan Grand Prix",           "AZE", 17, "2026-09-26T11:00", "2026-09-25T12:00"),
        ("Singapore Grand Prix",            "SGP", 18, "2026-10-11T12:00", "2026-10-09T12:30"),  # Sprint: lock at SQ
        ("United States Grand Prix",        "USA", 19, "2026-10-25T20:00", "2026-10-24T21:00"),
        ("Mexico City Grand Prix",          "MEX", 20, "2026-11-01T20:00", "2026-10-31T21:00"),
        ("São Paulo Grand Prix",            "BRA", 21, "2026-11-08T17:00", "2026-11-07T18:00"),
        ("Las Vegas Grand Prix",            "USA", 22, "2026-11-22T04:00", "2026-11-21T04:00"),
        ("Qatar Grand Prix",                "QAT", 23, "2026-11-29T16:00", "2026-11-28T18:00"),
        ("Abu Dhabi Grand Prix",            "ARE", 24, "2026-12-06T13:00", "2026-12-05T14:00"),
    ]

    #Season starting ranking for drivers
    preseason_data = [
        (0, 1, 1), #Verstappen
        (0, 2, 9), #Hadjar
        (0, 3, 8), #Hamilton
        (0, 4, 6), #Leclerc
        (0, 5, 3), #Norris
        (0, 6, 4), #Piastri
        (0, 7, 2), #Russell
        (0, 8, 5), #Antonelli
        (0, 9, 7), #Alonso
        (0, 10, 11), #Stroll
        (0, 11, 10), #Gasly
        (0, 12, 19), #Colapinto may be lower
        (0, 13, 13), #Lawson
        (0, 14, 19), #Lindblad may be lower
        (0, 15, 15), #Hulkenberg
        (0, 16, 16), #Bortoleto
        (0, 17, 14), #Albon
        (0, 18, 12), #Sainz
        (0, 19, 17), #Bearman
        (0, 20, 18), #Ocon
        (0, 21, 19), #Perez may be lower
        (0, 22, 19)  #Bottas may be lower
    ]

    for d in drivers:
        portrait = driver_slug(d[0]) + ".svg"
        db.execute(
            "INSERT INTO drivers (name, team, price, number, country, portrait) VALUES (?,?,?,?,?,?)",
            (*d, portrait)
        )
    for c in constructors_data:
        db.execute("INSERT INTO constructors (name, price, color) VALUES (?,?,?)", c)
    for r in races_data:
        db.execute(
            "INSERT INTO races (name, country, round, race_datetime, quali_datetime) VALUES (?,?,?,?,?)", r
        )
    for p in preseason_data:
        db.execute("INSERT INTO preseason_results (race_id, driver_id, position) VALUES (?,?,?)", p)
    db.commit()


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

@app.context_processor
def utility_processor():
    from datetime import datetime, timezone
    return {
        "team_colors": TEAM_COLORS,
        "now_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M"),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.before_request
def before_request():
    init_db()
    seed_data()
    db = get_db()
    migrate_2026_calendar(db)
    migrate_sprint_lock_times(db)


@app.route("/")
def index():
    if "user_id" in session:
        return redirect(url_for("home"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    db = get_db()
    users = db.execute("SELECT id, username FROM users ORDER BY username").fetchall()

    if request.method == "POST":
        user_id = request.form.get("user_id")
        password = request.form.get("password")

        if not user_id or not password:
            flash("Please select a user and enter a password.", "error")
            return render_template("login.html", users=users)

        user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if user and check_password_hash(user["password"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            return redirect(url_for("home"))
        else:
            flash("Invalid password.", "error")

    return render_template("login.html", users=users)


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("register.html")
        if len(password) < 4:
            flash("Password must be at least 4 characters.", "error")
            return render_template("register.html")

        db = get_db()
        if db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone():
            flash("Username already taken.", "error")
            return render_template("register.html")

        db.execute(
            "INSERT INTO users (username, password) VALUES (?, ?)",
            (username, generate_password_hash(password)),
        )
        db.commit()
        flash("Account created! You can now log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    db = get_db()
    user_id = session["user_id"]

    # Find last completed race for "last race points" column
    last_race = db.execute(
        "SELECT id FROM races WHERE completed = 1 ORDER BY round DESC LIMIT 1"
    ).fetchone()
    last_race_id = last_race["id"] if last_race else None

    leaderboard = db.execute("""
        SELECT u.id, u.username, u.budget,
               COALESCE(SUM(us.points), 0) as total_points,
               COALESCE((SELECT points FROM user_scores WHERE user_id = u.id AND race_id = ?), 0) as last_race_pts
        FROM users u
        LEFT JOIN user_scores us ON u.id = us.user_id
        GROUP BY u.id
        ORDER BY total_points DESC
    """, (last_race_id,)).fetchall()

    # Calculate position changes: compare current ranking to ranking before the last race
    prev_positions = {}
    if last_race_id:
        prev_leaderboard = db.execute("""
            SELECT u.id, COALESCE(SUM(us.points), 0) as total_points
            FROM users u
            LEFT JOIN user_scores us ON u.id = us.user_id AND us.race_id != ?
            GROUP BY u.id
            ORDER BY total_points DESC
        """, (last_race_id,)).fetchall()
        for i, row in enumerate(prev_leaderboard):
            prev_positions[row["id"]] = i + 1

    position_changes = {}
    for i, row in enumerate(leaderboard):
        current_pos = i + 1
        prev_pos = prev_positions.get(row["id"], current_pos)
        position_changes[row["id"]] = prev_pos - current_pos  # positive = moved up

    team_drivers = db.execute("""
        SELECT d.*, ut.is_turbo, ut.slot, ut.lock_duration, ut.lock_remaining, ut.on_cooldown
        FROM user_teams ut
        JOIN drivers d ON ut.driver_id = d.id
        WHERE ut.user_id = ? AND ut.driver_id IS NOT NULL
        ORDER BY ut.slot
    """, (user_id,)).fetchall()

    team_constructors = db.execute("""
        SELECT c.*, ut.slot
        FROM user_teams ut
        JOIN constructors c ON ut.constructor_id = c.id
        WHERE ut.user_id = ? AND ut.constructor_id IS NOT NULL
        ORDER BY ut.slot
    """, (user_id,)).fetchall()

    user = db.execute("SELECT budget FROM users WHERE id = ?", (user_id,)).fetchone()
    spent = sum(d["price"] for d in team_drivers if not d["on_cooldown"]) + sum(c["price"] for c in team_constructors)
    remaining_budget = user["budget"] - spent

    races = db.execute("SELECT * FROM races ORDER BY round").fetchall()
    completed_races = [r for r in races if r["completed"]]
    # Next unscored race (for admin attention)
    next_unscored = next((r for r in races if not r["completed"]), None)
    # Next upcoming race with a future datetime (for countdowns)
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    next_race = next((r for r in races if not r["completed"] and r["race_datetime"] > now_utc), None)
    # Fall back to unscored if no future race
    if not next_race:
        next_race = next_unscored
    lineup_locked = is_lineup_locked(db)

    user_total = db.execute(
        "SELECT COALESCE(SUM(points), 0) as total FROM user_scores WHERE user_id = ?",
        (user_id,)
    ).fetchone()["total"]

    driver_standings = fetch_official_driver_standings()
    constructor_standings = fetch_official_constructor_standings()

    user_rank = 1
    for i, row in enumerate(leaderboard):
        if row["id"] == user_id:
            user_rank = i + 1
            break

    # Last race summary: each user's picks and per-pick points
    last_race_summary = []
    last_race_info = None
    if last_race_id:
        last_race_info = db.execute("SELECT * FROM races WHERE id = ?", (last_race_id,)).fetchone()
        users_list = db.execute("SELECT id, username FROM users ORDER BY id").fetchall()
        for u in users_list:
            score_row = db.execute(
                "SELECT points FROM user_scores WHERE user_id = ? AND race_id = ?",
                (u["id"], last_race_id)
            ).fetchone()
            total_pts = score_row["points"] if score_row else 0
            driver_picks = db.execute("""
                SELECT d.name, ps.points, ps.is_turbo
                FROM pick_scores ps
                JOIN drivers d ON ps.driver_id = d.id
                WHERE ps.user_id = ? AND ps.race_id = ? AND ps.driver_id IS NOT NULL
                ORDER BY ps.points DESC
            """, (u["id"], last_race_id)).fetchall()
            constructor_picks = db.execute("""
                SELECT c.name, ps.points
                FROM pick_constructor_scores ps
                JOIN constructors c ON ps.constructor_id = c.id
                WHERE ps.user_id = ? AND ps.race_id = ? AND ps.constructor_id IS NOT NULL
            """, (u["id"], last_race_id)).fetchall()
            last_race_summary.append({
                "username": u["username"],
                "total_points": total_pts,
                "drivers": [{"name": p["name"], "points": p["points"], "is_turbo": p["is_turbo"]} for p in driver_picks],
                "constructors": [{"name": p["name"], "points": p["points"]} for p in constructor_picks],
            })
        last_race_summary.sort(key=lambda x: x["total_points"], reverse=True)

    # Past races that user hasn't submitted picks for
    past_races = [r for r in races if r["race_datetime"][:10] < now_utc[:10]]
    needs_picks = []
    for r in past_races:
        has_picks = db.execute(
            "SELECT COUNT(*) as c FROM race_picks WHERE user_id = ? AND race_id = ?",
            (user_id, r["id"])
        ).fetchone()["c"]
        if not has_picks:
            needs_picks.append(r)

    return render_template(
        "home.html",
        leaderboard=leaderboard,
        team_drivers=team_drivers,
        team_constructors=team_constructors,
        remaining_budget=remaining_budget,
        total_budget=user["budget"],
        races=races,
        completed_races=completed_races,
        next_race=next_race,
        lineup_locked=lineup_locked,
        user_total=user_total,
        user_rank=user_rank,
        driver_standings=driver_standings,
        constructor_standings=constructor_standings,
        needs_picks=needs_picks,
        last_race_summary=last_race_summary,
        last_race_info=last_race_info,
        position_changes=position_changes,
    )


@app.route("/team", methods=["GET", "POST"])
@login_required
def team():
    db = get_db()
    user_id = session["user_id"]

    lineup_locked = is_lineup_locked(db)

    locked_out = get_locked_out_drivers(db, user_id)
    locked_out_constructors = get_locked_out_constructors(db, user_id)

    if request.method == "POST":
        action = request.form.get("action")

        # --- Early Release (driver or constructor) ---
        if action == "release_constructor":
            if lineup_locked:
                flash("Cannot release constructors while lineups are locked.", "error")
                return redirect(url_for("team"))

            release_id = request.form.get("constructor_id")
            if not release_id:
                flash("No constructor specified.", "error")
                return redirect(url_for("team"))

            contract = db.execute(
                "SELECT id, lock_remaining, lock_duration FROM user_teams WHERE user_id = ? AND constructor_id = ? AND on_cooldown = 0",
                (user_id, release_id)
            ).fetchone()
            if not contract or contract["lock_remaining"] <= 0:
                flash("Constructor is not under contract.", "error")
                return redirect(url_for("team"))

            constructor = db.execute("SELECT name, price FROM constructors WHERE id = ?", (release_id,)).fetchone()

            if contract["lock_remaining"] < contract["lock_duration"]:
                penalty = round(constructor["price"] * 0.03, 1)
                db.execute("UPDATE users SET budget = budget - ? WHERE id = ?", (penalty, user_id))
                penalty_msg = f" Penalty: ${penalty:.1f}M deducted from budget."
                db.execute(
                    "UPDATE user_teams SET on_cooldown = 1, lock_remaining = 0 WHERE id = ?",
                    (contract["id"],)
                )
            else:
                penalty_msg = " No penalty (contract hadn't started yet)."
                db.execute("DELETE FROM user_teams WHERE id = ?", (contract["id"],))

            db.commit()
            flash(f"Released {constructor['name']}.{penalty_msg}", "success")
            return redirect(url_for("team"))

        if action == "release_driver":
            if lineup_locked:
                flash("Cannot release drivers while lineups are locked.", "error")
                return redirect(url_for("team"))

            release_id = request.form.get("driver_id")
            if not release_id:
                flash("No driver specified.", "error")
                return redirect(url_for("team"))

            # Verify user has this driver under contract (locked, not on cooldown)
            contract = db.execute(
                "SELECT id, lock_remaining, lock_duration FROM user_teams WHERE user_id = ? AND driver_id = ? AND on_cooldown = 0",
                (user_id, release_id)
            ).fetchone()
            if not contract or contract["lock_remaining"] <= 0:
                flash("Driver is not under contract.", "error")
                return redirect(url_for("team"))

            driver = db.execute("SELECT name, price FROM drivers WHERE id = ?", (release_id,)).fetchone()

            # Contract hasn't started yet (no race scored since picking) = free release
            # Contract has started (lock_remaining < lock_duration) = 3% penalty
            if contract["lock_remaining"] < contract["lock_duration"]:
                penalty = round(driver["price"] * 0.03, 1)
                db.execute("UPDATE users SET budget = budget - ? WHERE id = ?", (penalty, user_id))
                penalty_msg = f" Penalty: ${penalty:.1f}M deducted from budget."
            else:
                penalty_msg = " No penalty (contract hadn't started yet)."

            # Remove the driver from the team (no cooldown needed if contract never started)
            if contract["lock_remaining"] < contract["lock_duration"]:
                # Put driver on cooldown (will be cleared after next race scoring)
                db.execute(
                    "UPDATE user_teams SET on_cooldown = 1, lock_remaining = 0 WHERE id = ?",
                    (contract["id"],)
                )
            else:
                db.execute("DELETE FROM user_teams WHERE id = ?", (contract["id"],))

            db.commit()
            flash(f"Released {driver['name']}.{penalty_msg}", "success")
            return redirect(url_for("team"))

        if lineup_locked:
            flash("Lineups are locked! Qualifying has started. Wait until the race is scored.", "error")
            return redirect(url_for("home"))

        driver_ids = request.form.getlist("drivers")
        constructor_ids = request.form.getlist("constructors")
        turbo_driver = request.form.get("turbo_driver")
        lock_durations = {}
        for did in driver_ids:
            dur = request.form.get(f"lock_{did}", "1")
            lock_durations[did] = max(1, min(5, int(dur)))
        constructor_lock_durations = {}
        for cid in constructor_ids:
            dur = request.form.get(f"lock_c_{cid}", "1")
            constructor_lock_durations[cid] = max(1, min(5, int(dur)))

        if len(driver_ids) != 5:
            flash("You must select exactly 5 drivers.", "error")
            return redirect(url_for("team"))
        if len(constructor_ids) != 1:
            flash("You must select exactly 1 constructor.", "error")
            return redirect(url_for("team"))

        for did in driver_ids:
            if int(did) in locked_out:
                dname = db.execute("SELECT name FROM drivers WHERE id = ?", (did,)).fetchone()
                flash(f"{dname['name']} is on cooldown and cannot be selected this race.", "error")
                return redirect(url_for("team"))

        for cid in constructor_ids:
            if int(cid) in locked_out_constructors:
                cname = db.execute("SELECT name FROM constructors WHERE id = ?", (cid,)).fetchone()
                flash(f"{cname['name']} is on cooldown and cannot be selected this race.", "error")
                return redirect(url_for("team"))

        drivers_picked = db.execute(
            f"SELECT * FROM drivers WHERE id IN ({','.join('?' * len(driver_ids))})",
            driver_ids
        ).fetchall()
        constructors_picked = db.execute(
            f"SELECT * FROM constructors WHERE id IN ({','.join('?' * len(constructor_ids))})",
            constructor_ids
        ).fetchall()

        total_cost = sum(d["price"] for d in drivers_picked) + sum(c["price"] for c in constructors_picked)
        user = db.execute("SELECT budget FROM users WHERE id = ?", (user_id,)).fetchone()

        if total_cost > user["budget"]:
            flash(f"Over budget! Cost: ${total_cost:.1f}M / Budget: ${user['budget']:.1f}M", "error")
            return redirect(url_for("team"))

        # Turbo salary cap validation
        if turbo_driver:
            turbo_d = next((d for d in drivers_picked if str(d["id"]) == turbo_driver), None)
            if turbo_d and turbo_d["price"] >= TURBO_SALARY_CAP:
                flash(f"Turbo driver must cost less than ${TURBO_SALARY_CAP:.1f}M. {turbo_d['name']} costs ${turbo_d['price']:.1f}M.", "error")
                return redirect(url_for("team"))

        # Snapshot existing active contracts (lock already started) so we don't reset them
        existing_driver_locks = {}
        existing_constructor_locks = {}
        for row in db.execute(
            "SELECT driver_id, constructor_id, lock_duration, lock_remaining FROM user_teams WHERE user_id = ? AND on_cooldown = 0",
            (user_id,)
        ).fetchall():
            if row["driver_id"] and row["lock_remaining"] < row["lock_duration"]:
                existing_driver_locks[str(row["driver_id"])] = (row["lock_duration"], row["lock_remaining"])
            if row["constructor_id"] and row["lock_remaining"] < row["lock_duration"]:
                existing_constructor_locks[str(row["constructor_id"])] = (row["lock_duration"], row["lock_remaining"])

        db.execute("DELETE FROM user_teams WHERE user_id = ? AND on_cooldown = 0", (user_id,))
        for i, did in enumerate(driver_ids):
            is_turbo = 1 if did == turbo_driver else 0
            if did in existing_driver_locks:
                dur, rem = existing_driver_locks[did]
            else:
                dur = lock_durations.get(did, 1)
                rem = dur
            db.execute(
                "INSERT INTO user_teams (user_id, driver_id, is_turbo, slot, lock_duration, lock_remaining, on_cooldown) VALUES (?,?,?,?,?,?,0)",
                (user_id, did, is_turbo, i + 1, dur, rem)
            )
        for i, cid in enumerate(constructor_ids):
            if cid in existing_constructor_locks:
                cdur, crem = existing_constructor_locks[cid]
            else:
                cdur = constructor_lock_durations.get(cid, 1)
                crem = cdur
            db.execute(
                "INSERT INTO user_teams (user_id, constructor_id, is_turbo, slot, lock_duration, lock_remaining, on_cooldown) VALUES (?,?,?,?,?,?,0)",
                (user_id, cid, 0, i + 6, cdur, crem)
            )
        db.commit()
        flash("Team saved!", "success")
        return redirect(url_for("home"))

    if lineup_locked:
        flash("Lineups are locked! Qualifying has started. Wait until the race is scored.", "error")
        return redirect(url_for("home"))

    drivers = db.execute("SELECT * FROM drivers ORDER BY price DESC").fetchall()
    constructors = db.execute("SELECT * FROM constructors ORDER BY price DESC").fetchall()
    user = db.execute("SELECT budget FROM users WHERE id = ?", (user_id,)).fetchone()

    current_drivers = db.execute(
        "SELECT driver_id, is_turbo, lock_duration, lock_remaining FROM user_teams WHERE user_id = ? AND driver_id IS NOT NULL AND on_cooldown = 0",
        (user_id,)
    ).fetchall()
    current_constructors = db.execute(
        "SELECT constructor_id, lock_duration, lock_remaining FROM user_teams WHERE user_id = ? AND constructor_id IS NOT NULL AND on_cooldown = 0",
        (user_id,)
    ).fetchall()

    selected_driver_ids = [str(d["driver_id"]) for d in current_drivers]
    turbo_id = next((str(d["driver_id"]) for d in current_drivers if d["is_turbo"]), None)
    selected_constructor_ids = [str(c["constructor_id"]) for c in current_constructors]
    driver_locks = {str(d["driver_id"]): d["lock_duration"] for d in current_drivers}
    constructor_locks = {str(c["constructor_id"]): c["lock_duration"] for c in current_constructors}
    driver_remaining = {str(d["driver_id"]): d["lock_remaining"] for d in current_drivers}
    constructor_remaining = {str(c["constructor_id"]): c["lock_remaining"] for c in current_constructors}
    # Drivers/constructors with active contracts (lock already started, can't change duration)
    active_driver_contracts = {str(d["driver_id"]) for d in current_drivers if d["lock_remaining"] < d["lock_duration"]}
    active_constructor_contracts = {str(c["constructor_id"]) for c in current_constructors if c["lock_remaining"] < c["lock_duration"]}

    # Get locked drivers (under contract, not on cooldown) for early release UI
    locked_drivers = db.execute("""
        SELECT ut.id as ut_id, ut.driver_id, ut.lock_remaining, ut.lock_duration, d.name, d.price
        FROM user_teams ut
        JOIN drivers d ON ut.driver_id = d.id
        WHERE ut.user_id = ? AND ut.driver_id IS NOT NULL AND ut.on_cooldown = 0 AND ut.lock_remaining > 0
    """, (user_id,)).fetchall()

    # Get locked constructors for early release UI
    locked_constructors = db.execute("""
        SELECT ut.id as ut_id, ut.constructor_id, ut.lock_remaining, ut.lock_duration, c.name, c.price
        FROM user_teams ut
        JOIN constructors c ON ut.constructor_id = c.id
        WHERE ut.user_id = ? AND ut.constructor_id IS NOT NULL AND ut.on_cooldown = 0 AND ut.lock_remaining > 0
    """, (user_id,)).fetchall()

    return render_template(
        "team.html",
        drivers=drivers,
        constructors=constructors,
        budget=user["budget"],
        selected_driver_ids=selected_driver_ids,
        selected_constructor_ids=selected_constructor_ids,
        turbo_id=turbo_id,
        locked_out=locked_out,
        locked_out_constructors=locked_out_constructors,
        driver_locks=driver_locks,
        constructor_locks=constructor_locks,
        turbo_salary_cap=TURBO_SALARY_CAP,
        locked_drivers=locked_drivers,
        locked_constructors=locked_constructors,
        lineup_locked=lineup_locked,
        active_driver_contracts=active_driver_contracts,
        active_constructor_contracts=active_constructor_contracts,
        driver_remaining=driver_remaining,
        constructor_remaining=constructor_remaining,
    )


@app.route("/picks/<int:race_id>", methods=["GET", "POST"])
@login_required
def race_picks_page(race_id):
    db = get_db()
    user_id = session["user_id"]

    race = db.execute("SELECT * FROM races WHERE id = ?", (race_id,)).fetchone()
    if not race:
        flash("Race not found.", "error")
        return redirect(url_for("home"))
    if race["completed"]:
        flash("This race has already been scored.", "error")
        return redirect(url_for("home"))

    if request.method == "POST":
        driver_ids = request.form.getlist("drivers")
        constructor_ids = request.form.getlist("constructors")
        turbo_driver = request.form.get("turbo_driver")
        lock_durations = {}
        for did in driver_ids:
            dur = request.form.get(f"lock_{did}", "1")
            lock_durations[did] = max(1, min(5, int(dur)))
        constructor_lock_durations = {}
        for cid in constructor_ids:
            dur = request.form.get(f"lock_c_{cid}", "1")
            constructor_lock_durations[cid] = max(1, min(5, int(dur)))

        if len(driver_ids) != 5:
            flash("You must select exactly 5 drivers.", "error")
            return redirect(url_for("race_picks_page", race_id=race_id))
        if len(constructor_ids) != 1:
            flash("You must select exactly 1 constructor.", "error")
            return redirect(url_for("race_picks_page", race_id=race_id))

        # Budget check
        drivers_picked = db.execute(
            f"SELECT * FROM drivers WHERE id IN ({','.join('?' * len(driver_ids))})", driver_ids
        ).fetchall()
        constructors_picked = db.execute(
            f"SELECT * FROM constructors WHERE id IN ({','.join('?' * len(constructor_ids))})", constructor_ids
        ).fetchall()
        user = db.execute("SELECT budget FROM users WHERE id = ?", (user_id,)).fetchone()
        total_cost = sum(d["price"] for d in drivers_picked) + sum(c["price"] for c in constructors_picked)
        if total_cost > user["budget"]:
            flash(f"Over budget! Cost: ${total_cost:.1f}M / Budget: ${user['budget']:.1f}M", "error")
            return redirect(url_for("race_picks_page", race_id=race_id))

        # Turbo salary cap validation
        if turbo_driver:
            turbo_d = next((d for d in drivers_picked if str(d["id"]) == turbo_driver), None)
            if turbo_d and turbo_d["price"] >= TURBO_SALARY_CAP:
                flash(f"Turbo driver must cost less than ${TURBO_SALARY_CAP:.1f}M. {turbo_d['name']} costs ${turbo_d['price']:.1f}M.", "error")
                return redirect(url_for("race_picks_page", race_id=race_id))

        # Clear old picks for this race
        db.execute("DELETE FROM race_picks WHERE user_id = ? AND race_id = ?", (user_id, race_id))
        for i, did in enumerate(driver_ids):
            is_turbo = 1 if did == turbo_driver else 0
            db.execute(
                "INSERT INTO race_picks (user_id, race_id, driver_id, is_turbo, slot) VALUES (?,?,?,?,?)",
                (user_id, race_id, did, is_turbo, i + 1)
            )
        for i, cid in enumerate(constructor_ids):
            db.execute(
                "INSERT INTO race_picks (user_id, race_id, constructor_id, is_turbo, slot) VALUES (?,?,?,0,?)",
                (user_id, race_id, cid, i + 6)
            )

        # Also save as user_teams (with lock durations) if user has no current team
        existing_team = db.execute(
            "SELECT COUNT(*) as c FROM user_teams WHERE user_id = ? AND on_cooldown = 0",
            (user_id,)
        ).fetchone()["c"]
        if existing_team == 0:
            for i, did in enumerate(driver_ids):
                is_turbo = 1 if did == turbo_driver else 0
                dur = lock_durations.get(did, 1)
                db.execute(
                    "INSERT INTO user_teams (user_id, driver_id, is_turbo, slot, lock_duration, lock_remaining, on_cooldown) VALUES (?,?,?,?,?,?,0)",
                    (user_id, did, is_turbo, i + 1, dur, dur)
                )
            for i, cid in enumerate(constructor_ids):
                cdur = constructor_lock_durations.get(cid, 1)
                db.execute(
                    "INSERT INTO user_teams (user_id, constructor_id, is_turbo, slot, lock_duration, lock_remaining, on_cooldown) VALUES (?,?,?,?,?,?,0)",
                    (user_id, cid, 0, i + 6, cdur, cdur)
                )

        db.commit()
        flash(f"Picks saved for {race['name']}!", "success")
        return redirect(url_for("home"))

    drivers = db.execute("SELECT * FROM drivers ORDER BY price DESC").fetchall()
    constructors = db.execute("SELECT * FROM constructors ORDER BY price DESC").fetchall()
    user = db.execute("SELECT budget FROM users WHERE id = ?", (user_id,)).fetchone()

    # Load existing picks for this race
    existing_drivers = db.execute(
        "SELECT driver_id, is_turbo FROM race_picks WHERE user_id = ? AND race_id = ? AND driver_id IS NOT NULL",
        (user_id, race_id)
    ).fetchall()
    existing_constructors = db.execute(
        "SELECT constructor_id FROM race_picks WHERE user_id = ? AND race_id = ? AND constructor_id IS NOT NULL",
        (user_id, race_id)
    ).fetchall()

    selected_driver_ids = [str(d["driver_id"]) for d in existing_drivers]
    turbo_id = next((str(d["driver_id"]) for d in existing_drivers if d["is_turbo"]), None)
    selected_constructor_ids = [str(c["constructor_id"]) for c in existing_constructors]

    # Load lock durations from user_teams if they exist
    current_driver_locks = db.execute(
        "SELECT driver_id, lock_duration FROM user_teams WHERE user_id = ? AND driver_id IS NOT NULL AND on_cooldown = 0",
        (user_id,)
    ).fetchall()
    current_constructor_locks = db.execute(
        "SELECT constructor_id, lock_duration FROM user_teams WHERE user_id = ? AND constructor_id IS NOT NULL AND on_cooldown = 0",
        (user_id,)
    ).fetchall()
    driver_locks = {str(d["driver_id"]): d["lock_duration"] for d in current_driver_locks}
    constructor_locks = {str(c["constructor_id"]): c["lock_duration"] for c in current_constructor_locks}

    return render_template(
        "race_picks.html",
        race=race,
        drivers=drivers,
        constructors=constructors,
        budget=user["budget"],
        selected_driver_ids=selected_driver_ids,
        selected_constructor_ids=selected_constructor_ids,
        turbo_id=turbo_id,
        driver_locks=driver_locks,
        constructor_locks=constructor_locks,
        turbo_salary_cap=TURBO_SALARY_CAP,
    )


@app.route("/admin", methods=["GET", "POST"])
@login_required
def admin():
    # Check admin auth
    if not session.get("admin_auth"):
        return redirect(url_for("admin_login"))

    db = get_db()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "unlock_quali":
            race_id = request.form.get("race_id")
            db.execute("UPDATE races SET quali_locked = -1 WHERE id = ?", (race_id,))
            db.commit()
            flash("Lineup lock removed.", "success")

        elif action == "score_race":
            race_id = request.form.get("race_id")
            race = db.execute("SELECT * FROM races WHERE id = ?", (race_id,)).fetchone()
            if not race:
                flash("Race not found.", "error")
            else:
                rescore = race["completed"]
                if rescore:
                    # Undo old scores: subtract old driver/constructor points
                    old_results = db.execute("SELECT * FROM race_results WHERE race_id = ?", (race_id,)).fetchall()
                    drivers_all = db.execute("SELECT id, team FROM drivers").fetchall()
                    team_map = {d["id"]: d["team"] for d in drivers_all}
                    team_drivers = {}
                    for d in drivers_all:
                        team_drivers.setdefault(d["team"], []).append(d["id"])
                    old_result_map = {r["driver_id"]: r for r in old_results}

                    for r in old_results:
                        did = r["driver_id"]
                        driver_team = team_map.get(did)
                        tm_finish = None
                        if driver_team:
                            for tid in team_drivers.get(driver_team, []):
                                if tid != did and tid in old_result_map:
                                    tm_finish = old_result_map[tid]["position"]
                                    break
                        old_pts = calc_driver_race_points(r, tm_finish, db)
                        db.execute("UPDATE drivers SET points = points - ? WHERE id = ?", (old_pts, did))

                    for team_name, dids in team_drivers.items():
                        rp = [old_result_map[did]["position"] for did in dids if did in old_result_map and old_result_map[did]["position"] and old_result_map[did]["status"] not in ['Did not start', 'Disqualified']]
                        qp = [old_result_map[did]["quali_pos"] for did in dids if did in old_result_map and old_result_map[did]["quali_pos"]]
                        old_cpts = calc_constructor_race_points(rp, qp)
                        if old_cpts:
                            db.execute("UPDATE constructors SET points = points - ? WHERE name = ?", (old_cpts, team_name))

                    # Undo price changes (reverse dynamic pricing)
                    #undo driver salaries
                    results = db.execute("SELECT id, price, price_change, points FROM drivers").fetchall()
                    for r in results:
                        db.execute("UPDATE drivers SET price = price - ?WHERE id = ?",
                                        (r["price_change"], r["id"])
                        )
                        pick_users = db.execute(
                                    "SELECT DISTINCT user_id FROM race_picks WHERE race_id = ? AND driver_id = ?",
                                    (race_id, r["id"])
                        ).fetchall()
                        if not pick_users:
                            pick_users = db.execute(
                                "SELECT DISTINCT user_id FROM user_teams WHERE driver_id = ? AND on_cooldown = 0",
                                    (r["id"],)
                        ).fetchall()
                        
                        for u in pick_users:
                            db.execute(
                                "UPDATE users SET budget = budget - ? WHERE id = ?",
                                    (r["price_change"], u["user_id"])
                        )
                    db.commit()

                    #calculate change in driver salry based on performance
                    #undo constructor salaries
                    results = db.execute("SELECT id, price, price_change, points FROM constructors").fetchall()
                    for r in results:
                        db.execute("UPDATE constructors SET price = price - ? WHERE id = ?",
                                        (r["price_change"], r["id"])
                        )
                        pick_users = db.execute(
                                    "SELECT DISTINCT user_id FROM race_picks WHERE race_id = ? AND constructor_id = ?",
                                    (race_id, r["id"])
                        ).fetchall()
                        if not pick_users:
                            pick_users = db.execute(
                                "SELECT DISTINCT user_id FROM user_teams WHERE constructor_id = ? AND on_cooldown = 0",
                                    (r["id"],)
                        ).fetchall()
                        for u in pick_users:
                            db.execute(
                                "UPDATE users SET budget = budget - ? WHERE id = ?",
                                    (r["price_change"], u["user_id"])
                        )
                    db.commit()
                   
                # Auto-fetch results from F1 API
                results, err = fetch_race_results(race["round"])
                if err:
                    flash(f"Could not fetch results: {err}", "error")
                else:
                    # Fetch qualifying and sprint results
                    quali_map = fetch_qualifying_results(race["round"])
                    sprint_map = fetch_sprint_results(race["round"])

                    # Clear any old results for this race
                    db.execute("DELETE FROM race_results WHERE race_id = ?", (race_id,))

                    unmatched = []
                    for r in results:
                        driver_id = match_driver_by_number(db, r["driver_number"])
                        if not driver_id:
                            unmatched.append(f"#{r['driver_number']} {r['driver_name']}")
                            continue

                        quali_pos = quali_map.get(r["driver_number"])
                        sprint_pos = sprint_map.get(r["driver_name"])
                        db.execute("""
                            INSERT INTO race_results (race_id, driver_id, position, grid, quali_pos,
                                                      fastest_lap, dnf, status, laps, total_laps, sprint_pos)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (race_id, driver_id, r["position"], r["grid"], quali_pos,
                              1 if r["fastest_lap"] else 0,
                              1 if r["dnf"] else 0, r['status'],
                              r["laps"], r["total_laps"],
                              sprint_pos))

                    if unmatched:
                        flash(f"Warning: could not match drivers: {', '.join(unmatched)}", "error")

                    # Now score the race
                    score_race(db, race_id, rescore=rescore)
                    verb = "rescored" if rescore else "scored"
                    flash(f"R{race['round']} {race['name']} {verb} from live data!", "success")

        elif action == "adjust_points":
            user_id = request.form.get("user_id")
            race_id = request.form.get("race_id")
            adjustment = request.form.get("adjustment", 0, type=int)
            if user_id and race_id and adjustment:
                existing = db.execute(
                    "SELECT points FROM user_scores WHERE user_id = ? AND race_id = ?",
                    (user_id, race_id)
                ).fetchone()
                if existing:
                    db.execute(
                        "UPDATE user_scores SET points = points + ? WHERE user_id = ? AND race_id = ?",
                        (adjustment, user_id, race_id)
                    )
                else:
                    db.execute(
                        "INSERT INTO user_scores (user_id, race_id, points) VALUES (?, ?, ?)",
                        (user_id, race_id, adjustment)
                    )
                db.commit()
                user = db.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
                race = db.execute("SELECT name, round FROM races WHERE id = ?", (race_id,)).fetchone()
                sign = "+" if adjustment > 0 else ""
                flash(f"{sign}{adjustment} pts for {user['username']} on R{race['round']} {race['name']}", "success")

    races = db.execute("SELECT * FROM races ORDER BY round").fetchall()
    drivers = db.execute("SELECT * FROM drivers ORDER BY name").fetchall()
    users = db.execute("SELECT id, username FROM users ORDER BY username").fetchall()

    # Get all manual adjustments for display
    adjustments = db.execute("""
        SELECT u.username, r.round, r.name as race_name, us.points
        FROM user_scores us
        JOIN users u ON us.user_id = u.id
        JOIN races r ON us.race_id = r.id
        ORDER BY r.round, u.username
    """).fetchall()

    # Build user team summaries
    all_user_teams = {}
    for u in users:
        uid = u["id"]
        uname = u["username"]
        budget = db.execute("SELECT budget FROM users WHERE id = ?", (uid,)).fetchone()["budget"]
        team_drivers = db.execute("""
            SELECT d.name, d.price, ut.is_turbo, ut.lock_duration, ut.lock_remaining, ut.on_cooldown
            FROM user_teams ut JOIN drivers d ON ut.driver_id = d.id
            WHERE ut.user_id = ? AND ut.driver_id IS NOT NULL
            ORDER BY ut.slot
        """, (uid,)).fetchall()
        team_constructors = db.execute("""
            SELECT c.name, c.price, ut.lock_duration, ut.lock_remaining, ut.on_cooldown
            FROM user_teams ut JOIN constructors c ON ut.constructor_id = c.id
            WHERE ut.user_id = ? AND ut.constructor_id IS NOT NULL
        """, (uid,)).fetchall()
        all_user_teams[uname] = {
            "budget": budget,
            "drivers": team_drivers,
            "constructors": team_constructors,
        }

    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    return render_template("admin.html", races=races, drivers=drivers, users=users, adjustments=adjustments, all_user_teams=all_user_teams, now_utc=now_utc)


@app.route("/admin/login", methods=["GET", "POST"])
@login_required
def admin_login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ADMIN_PASSWORD:
            session["admin_auth"] = True
            return redirect(url_for("admin"))
        else:
            flash("Incorrect admin password.", "error")
    return render_template("admin_login.html")


def score_race(db, race_id, rescore=False):
    """Score a race using results already in race_results table (GridRivals scoring)."""
    results = db.execute("SELECT * FROM race_results WHERE race_id = ?", (race_id,)).fetchall()

    # Build teammate map
    drivers_all = db.execute("SELECT id, team FROM drivers").fetchall()
    team_map = {d["id"]: d["team"] for d in drivers_all}
    team_drivers = {}
    for d in drivers_all:
        team_drivers.setdefault(d["team"], []).append(d["id"])

    result_map = {r["driver_id"]: r for r in results}

    # Calculate driver points with teammate awareness
    driver_points = {}
    for r in results:
        did = r["driver_id"]
        driver_team = team_map.get(did)
        teammate_finish = None
        if driver_team:
            for tid in team_drivers.get(driver_team, []):
                if tid != did and tid in result_map:
                    teammate_finish = result_map[tid]["position"]
                    break

        pts = calc_driver_race_points(r, teammate_finish, db)
        driver_points[did] = pts
        db.execute("UPDATE drivers SET points = ? WHERE id = ?", (pts, did))

    # Calculate constructor points: map each driver's position, then sum per team
    constructor_points = {}
    for team_name, driver_ids in team_drivers.items():
        race_positions = [result_map[did]["position"] for did in driver_ids if did in result_map and result_map[did]["position"] and result_map[did]["status"] not in ['Did not start', 'Disqualified']]
        quali_positions = [result_map[did]["quali_pos"] for did in driver_ids if did in result_map and result_map[did]["quali_pos"]]
        cpts = calc_constructor_race_points(race_positions, quali_positions)
        constructor_points[team_name] = cpts
        if cpts:
            db.execute("UPDATE constructors SET points = ? WHERE name = ?", (cpts, team_name))

    # Score each user
    users = db.execute("SELECT id FROM users").fetchall()
    for user in users:
        uid = user["id"]
        user_pts = 0

        picks = db.execute(
            "SELECT driver_id, is_turbo FROM race_picks WHERE user_id = ? AND race_id = ? AND driver_id IS NOT NULL",
            (uid, race_id)
        ).fetchall()
        if not picks:
            picks = db.execute(
                "SELECT driver_id, is_turbo FROM user_teams WHERE user_id = ? AND driver_id IS NOT NULL AND on_cooldown = 0",
                (uid,)
            ).fetchall()

        # Fetch constructor picks
        cpicks = db.execute(
            "SELECT constructor_id FROM race_picks WHERE user_id = ? AND race_id = ? AND constructor_id IS NOT NULL",
            (uid, race_id)
        ).fetchall()
        if not cpicks:
            cpicks = db.execute(
                "SELECT constructor_id FROM user_teams WHERE user_id = ? AND constructor_id IS NOT NULL",
                (uid,)
            ).fetchall()

        # Incomplete lineup (< 5 drivers + 1 constructor) forfeits turbo
        has_full_lineup = len(picks) >= 5 and len(cpicks) >= 1

        for pick in picks:
            dp = driver_points.get(pick["driver_id"], 0)
            is_turbo = pick["is_turbo"] and has_full_lineup
            if is_turbo:
                dp *= 2
            user_pts += dp
            db.execute("""
                INSERT INTO pick_scores (user_id, race_id, driver_id, points, is_turbo) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, race_id, driver_id) DO UPDATE SET points = excluded.points, is_turbo = excluded.is_turbo
            """, (uid, race_id, pick["driver_id"], dp, int(is_turbo)))

        for cpick in cpicks:
            cname = db.execute("SELECT name FROM constructors WHERE id = ?", (cpick["constructor_id"],)).fetchone()
            if cname:
                cpts = constructor_points.get(cname["name"], 0)
                user_pts += cpts
                db.execute("""
                    INSERT INTO pick_constructor_scores (user_id, race_id, constructor_id, points) VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id, race_id, constructor_id) DO UPDATE SET points = excluded.points
                """, (uid, race_id, cpick["constructor_id"], cpts))

        db.execute("""
            INSERT INTO user_scores (user_id, race_id, points) VALUES (?, ?, ?)
            ON CONFLICT(user_id, race_id) DO UPDATE SET points = ?
        """, (uid, race_id, user_pts, user_pts))

    # Dynamic pricing
    update_driver_prices(db, race_id)

    if not rescore:
        process_lock_decrements(db, race_id)
    db.execute("UPDATE races SET completed = 1 WHERE id = ?", (race_id,))
    db.commit()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
