from fastapi import FastAPI, Depends, HTTPException, Header, status
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import jwt
import bcrypt
import psycopg2
import psycopg2.extras
import os
import threading
import time
import re
import html as html_lib
import httpx
import urllib.request
import urllib.parse
import urllib.error
import json as json_lib
from contextlib import contextmanager

# === MW LEGACY (commented for pivot.watch) =============================================
# from google.cloud import bigquery
# from google.oauth2 import service_account
# ==============================================================================

# ── Config from environment ───────────────────────────────────────────────────
SECRET_KEY = os.environ.get("JWT_SECRET", os.environ.get("SECRET_KEY", "pivot-watch-fallback-key"))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10080
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "alerts@pivot.watch")
GOOGLE_GEOCODING_API_KEY = os.environ.get("GOOGLE_GEOCODING_API_KEY", "")

VERSION = "0.1.2"

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable not set. "
        "In Render, link a Postgres database to this service, or set DATABASE_URL manually."
    )
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


# === MW LEGACY (commented for pivot.watch) =============================================
# COUNTRY_TO_REGION map and normalize_region_label / get_region_for_country
# helpers removed — pivot.watch uses lat/lng + radius for non-remote searches; no named regions.
# Restore from CW v0.1.6 main.py if a region-named vertical needs them.
# ==============================================================================


# === MW LEGACY (commented for pivot.watch) =============================================
# BigQuery / SSDI helpers (get_bq_client, run_bigquery, fmt_date, parse_bq_results,
# run_ssdi_query) removed — pivot.watch does not query SSDI. Restore from CW v0.1.6
# main.py if a future vertical needs SSDI access.
# ==============================================================================


# ── DB init ───────────────────────────────────────────────────────────────────

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    c = conn.cursor()

    # PostGIS auto-enable (idempotent — safe to run on every boot).
    try:
        c.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    except Exception as e:
        print("[init_db] PostGIS extension setup warning: " + str(e))

    conn.autocommit = False

    # Users (canonical, kept identical to MW/CW shape).
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Notifications (canonical shape, polymorphic via source_type / source_ref_id).
    c.execute("""CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        watchlist_id INTEGER,
        obituary_id INTEGER,
        message TEXT NOT NULL,
        sent BOOLEAN DEFAULT FALSE,
        email_sent BOOLEAN DEFAULT FALSE,
        source_type TEXT,
        source_ref_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )""")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_type TEXT")
    c.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS source_ref_id INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notifications_source ON notifications (source_type, source_ref_id)")

    # === pivot.watch schema ──────────────────────────────────────────────────
    # Searches: a user-watched job search (filters + optional geofence).
    #
    # Two booleans match the EW/MW pattern:
    #   - is_archived: legacy, kept for backward-compat only.
    #   - in_my_searches: v0.1.0+ model. A search ALWAYS stays in Watchlist (where
    #     the cron monitors it). "Save to My Searches" adds it to the My Searches
    #     collection without leaving Watchlist.
    #
    # Filter columns (any NULL = "any"):
    #   - company  ILIKE match (substring, case-insensitive)
    #   - sector   exact match against normalized category
    #   - level    exact match: entry | mid | senior | executive
    #
    # Location: radius_value drives the spatial branch.
    #   - radius_value = '5' | '10' | '25' | '50'  → lat/lng required; spatial join
    #   - radius_value = 'remote'                  → no lat/lng; match is_remote=true
    c.execute("""CREATE TABLE IF NOT EXISTS pv_searches (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        company TEXT,
        sector TEXT,
        level TEXT,
        location_name TEXT,
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION,
        radius_value TEXT NOT NULL DEFAULT '25',
        check_interval_minutes INTEGER NOT NULL DEFAULT 720,
        alert_level TEXT NOT NULL DEFAULT 'realtime',
        is_archived BOOLEAN NOT NULL DEFAULT FALSE,
        in_my_searches BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )""")
    # Generated PostGIS geometry column (auto-derived from lat/lng).
    # NULL for radius_value='remote' rows; populated otherwise.
    c.execute("""DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='pv_searches' AND column_name='geom'
        ) THEN
            ALTER TABLE pv_searches
            ADD COLUMN geom geography(Point, 4326)
            GENERATED ALWAYS AS (
                CASE
                    WHEN lat IS NOT NULL AND lng IS NOT NULL
                    THEN ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography
                    ELSE NULL
                END
            ) STORED;
        END IF;
    END $$;""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_searches_user ON pv_searches (user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_searches_geom ON pv_searches USING GIST (geom)")

    # Jobs: a single job posting from a source feed. UNIQUE (source, external_id)
    # so re-fetching the same feed never duplicates rows.
    c.execute("""CREATE TABLE IF NOT EXISTS pv_jobs (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        external_id TEXT NOT NULL,
        title TEXT,
        company TEXT,
        sector TEXT,
        level TEXT,
        location_name TEXT,
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION,
        is_remote BOOLEAN NOT NULL DEFAULT FALSE,
        salary_min DOUBLE PRECISION,
        salary_max DOUBLE PRECISION,
        salary_currency TEXT,
        description TEXT,
        url TEXT,
        posted_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_payload JSONB,
        UNIQUE (source, external_id)
    )""")
    c.execute("""DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='pv_jobs' AND column_name='geom'
        ) THEN
            ALTER TABLE pv_jobs
            ADD COLUMN geom geography(Point, 4326);
        END IF;
    END $$;""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_geom ON pv_jobs USING GIST (geom)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_posted ON pv_jobs (posted_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_source ON pv_jobs (source)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_remote ON pv_jobs (is_remote)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_sector ON pv_jobs (sector)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_level ON pv_jobs (level)")

    # Job matches: every (job, search) pair where the job satisfied the search
    # filters AND geofence. UNIQUE prevents double-alerting on rerun.
    c.execute("""CREATE TABLE IF NOT EXISTS pv_job_matches (
        id SERIAL PRIMARY KEY,
        job_id INTEGER NOT NULL,
        search_id INTEGER NOT NULL,
        distance_mi DOUBLE PRECISION,
        matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        alerted_at TIMESTAMP,
        UNIQUE (job_id, search_id),
        FOREIGN KEY (job_id) REFERENCES pv_jobs (id) ON DELETE CASCADE,
        FOREIGN KEY (search_id) REFERENCES pv_searches (id) ON DELETE CASCADE
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_matches_search ON pv_job_matches (search_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_matches_job ON pv_job_matches (job_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_matches_alerted ON pv_job_matches (alerted_at)")

    conn.commit()
    conn.close()


@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()


# === MW LEGACY (commented for pivot.watch) =============================================
# Wikipedia / SSDI / Legacy.com helpers (fetch_wiki_data, fetch_wiki_data_smart,
# normalize_name, normalize_name_for_wiki, extract_full_death_date,
# is_deceased_from_wiki, search_legacy_oneoff, send_email_notification for obits)
# all removed. Restore from CW v0.1.6 main.py if needed.
# ==============================================================================


# === CW LEGACY (commented for pivot.watch) =============================================
# State Department advisory pipeline (parse_advisory_level, parse_country_name_from_title,
# fetch_state_advisories, upsert_advisory, fire_advisory_alerts,
# reconcile_advisory_alerts_for_ship, check_state_advisories, send_advisory_email)
# all removed. pivot.watch is a jobs vertical; no weather/marine adapters.
# Restore from CW v0.1.6 main.py if a future vertical needs travel advisories.
# ==============================================================================


# === CW LEGACY (commented for pivot.watch) =============================================
# NOAA High Seas marine forecast pipeline (MARINE_AREAS, _parse_max_wave_meters,
# _sea_state_for_wave, _fetch_marine_bulletin, _extract_issued_at,
# check_marine_forecasts) all removed. pivot.watch is a jobs vertical, no marine via NWS
# CAP feed (which includes Gale/Storm/Hurricane Force/Special Marine warnings)
# and GDACS for non-US waters. Restore from CW v0.1.6 main.py if needed.
# ==============================================================================


# ── Auth helpers ──────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# ── Pydantic models ───────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class SearchItem(BaseModel):
    name: str
    company: Optional[str] = None        # NULL = any
    sector: Optional[str] = None         # NULL = any
    level: Optional[str] = None          # entry | mid | senior | executive | None=any
    location_name: Optional[str] = None  # human-readable; NULL if radius='remote'
    lat: Optional[float] = None          # required if radius != 'remote'
    lng: Optional[float] = None          # required if radius != 'remote'
    radius_value: str = "25"             # '5' | '10' | '25' | '50' | 'remote'
    alert_level: Optional[str] = "realtime"  # off | digest | realtime

class SearchUpdate(BaseModel):
    name: Optional[str] = None
    company: Optional[str] = None
    sector: Optional[str] = None
    level: Optional[str] = None
    location_name: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    radius_value: Optional[str] = None
    alert_level: Optional[str] = None
    is_archived: Optional[bool] = None
    in_my_searches: Optional[bool] = None

class SearchResponse(BaseModel):
    id: int
    name: str
    company: Optional[str]
    sector: Optional[str]
    level: Optional[str]
    location_name: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    radius_value: str
    alert_level: str
    is_archived: bool
    in_my_searches: bool
    created_at: str

# === MW LEGACY (commented for pivot.watch) ====================================
# ObituarySearch / ObituaryResult / WatchlistItem / WatchlistResponse Pydantic
# models removed -- pivot.watch uses SearchItem / SearchResponse exclusively.
# ==============================================================================


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="pivot.watch API", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        if sub is None:
            raise HTTPException(status_code=401, detail="Invalid authentication")
        user_id: int = int(sub)
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ── Health / admin ────────────────────────────────────────────────────────────

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": VERSION
    }


@app.get("/admin/delete-user")
async def admin_delete_user(email: str):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (email,))
        user = c.fetchone()
        if not user:
            return {"deleted": False, "error": "User not found"}
        user_id = user[0]
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        # Cascade deletes pv_job_matches via FK ON DELETE CASCADE.
        c.execute("DELETE FROM pv_searches WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"deleted": True, "email": email}


@app.get("/admin/stats")
async def get_stats():
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pv_searches WHERE is_archived = FALSE")
        active_searches = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pv_jobs")
        jobs = c.fetchone()[0]
        c.execute("SELECT source, COUNT(*) FROM pv_jobs GROUP BY source")
        by_source = {row[0]: row[1] for row in c.fetchall()}
        c.execute("SELECT COUNT(*) FROM pv_job_matches WHERE alerted_at IS NOT NULL")
        alerts_fired = c.fetchone()[0]
        return {
            "users": users,
            "active_searches": active_searches,
            "jobs_total": jobs,
            "jobs_by_source": by_source,
            "alerts_fired": alerts_fired,
            "version": VERSION,
        }


@app.get("/admin/check-now")
async def admin_check_now():
    """Manually trigger a full source pull + filter-join + alert pass."""
    threading.Thread(target=run_check_cycle, daemon=True).start()
    return {"message": "pivot.watch check cycle started"}


@app.get("/admin/who-owns-what")
async def admin_who_owns_what():
    """Debug endpoint: show every user and how many searches they own (active + archived).
    Used to verify the user_id filter is doing its job and we're not leaking data
    across accounts."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT u.id, u.email,
                   COUNT(p.id) FILTER (WHERE p.is_archived = FALSE) AS active,
                   COUNT(p.id) FILTER (WHERE p.is_archived = TRUE) AS archived,
                   COUNT(p.id) AS total
            FROM users u
            LEFT JOIN pv_searches p ON p.user_id = u.id
            GROUP BY u.id, u.email
            ORDER BY u.id
        """)
        rows = []
        for r in c.fetchall():
            rows.append({
                "user_id": r[0],
                "email": r[1],
                "active": int(r[2]),
                "archived": int(r[3]),
                "total": int(r[4]),
            })
        return {"users": rows, "version": VERSION}


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=Token)
async def register(user: UserCreate):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = %s", (user.email,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        password_hash = hash_password(user.password)
        c.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (user.email, password_hash))
        user_id = c.fetchone()[0]
        conn.commit()
        access_token = create_access_token(data={"sub": str(user_id)})
        return {"access_token": access_token, "token_type": "bearer"}


@app.post("/auth/login", response_model=Token)
async def login(user: UserLogin):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id, password_hash FROM users WHERE email = %s", (user.email,))
        result = c.fetchone()
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        access_token = create_access_token(data={"sub": str(result[0])})
        return {"access_token": access_token, "token_type": "bearer"}


@app.delete("/account")
async def delete_account(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM pv_searches WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"message": "Account permanently deleted"}


# ── pivot.watch Searches endpoints ────────────────────────────────────────────

@app.get("/pv/searches", response_model=List[SearchResponse])
async def list_searches(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, company, sector, level, location_name, lat, lng,
                   radius_value, alert_level, is_archived, in_my_searches, created_at
            FROM pv_searches
            WHERE user_id = %s
            ORDER BY created_at DESC
        """, (user_id,))
        items = []
        for row in c.fetchall():
            items.append({
                "id": row[0],
                "name": row[1],
                "company": row[2],
                "sector": row[3],
                "level": row[4],
                "location_name": row[5],
                "lat": float(row[6]) if row[6] is not None else None,
                "lng": float(row[7]) if row[7] is not None else None,
                "radius_value": row[8],
                "alert_level": row[9],
                "is_archived": bool(row[10]),
                "in_my_searches": bool(row[11]),
                "created_at": str(row[12]),
            })
        return items


@app.post("/pv/searches")
async def add_search(item: SearchItem, user_id: int = Depends(get_current_user)):
    # Validate radius_value.
    if item.radius_value not in ("1", "5", "10", "25", "50", "remote"):
        raise HTTPException(status_code=400, detail="radius_value must be 1/5/10/25/remote")
    # Lat/lng required unless radius is remote.
    if item.radius_value != "remote":
        if item.lat is None or item.lng is None:
            raise HTTPException(status_code=400, detail="lat/lng required when radius is not 'remote'")
        if item.lat < -90 or item.lat > 90:
            raise HTTPException(status_code=400, detail="lat must be between -90 and 90")
        if item.lng < -180 or item.lng > 180:
            raise HTTPException(status_code=400, detail="lng must be between -180 and 180")
    # Validate level if provided.
    if item.level and item.level not in ("entry", "mid", "senior", "executive"):
        raise HTTPException(status_code=400, detail="level must be entry/mid/senior/executive")
    alert_level = item.alert_level or "realtime"
    if alert_level not in ("off", "digest", "realtime"):
        alert_level = "realtime"
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO pv_searches (user_id, name, company, sector, level,
                                     location_name, lat, lng, radius_value, alert_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, (user_id, item.name, item.company, item.sector, item.level,
              item.location_name, item.lat, item.lng, item.radius_value, alert_level))
        row = c.fetchone()
        conn.commit()
        return {
            "message": "Search added",
            "id": row[0],
            "name": item.name,
            "company": item.company,
            "sector": item.sector,
            "level": item.level,
            "location_name": item.location_name,
            "lat": item.lat,
            "lng": item.lng,
            "radius_value": item.radius_value,
            "alert_level": alert_level,
            "is_archived": False,
            "in_my_searches": False,
            "created_at": str(row[1]),
        }


@app.patch("/pv/searches/{search_id}")
async def update_search(search_id: int, update: SearchUpdate, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM pv_searches WHERE id = %s AND user_id = %s", (search_id, user_id))
        if not c.fetchone():
            raise HTTPException(status_code=404, detail="Search not found")
        sets = []
        params = []
        if update.name is not None:
            sets.append("name = %s"); params.append(update.name)
        if update.company is not None:
            sets.append("company = %s"); params.append(update.company)
        if update.sector is not None:
            sets.append("sector = %s"); params.append(update.sector)
        if update.level is not None:
            if update.level and update.level not in ("entry", "mid", "senior", "executive"):
                raise HTTPException(status_code=400, detail="level must be entry/mid/senior/executive")
            sets.append("level = %s"); params.append(update.level)
        if update.location_name is not None:
            sets.append("location_name = %s"); params.append(update.location_name)
        if update.lat is not None:
            sets.append("lat = %s"); params.append(update.lat)
        if update.lng is not None:
            sets.append("lng = %s"); params.append(update.lng)
        if update.radius_value is not None:
            if update.radius_value not in ("1", "5", "10", "25", "50", "remote"):
                raise HTTPException(status_code=400, detail="radius_value must be 1/5/10/25/remote")
            sets.append("radius_value = %s"); params.append(update.radius_value)
        if update.alert_level is not None:
            if update.alert_level not in ("off", "digest", "realtime"):
                raise HTTPException(status_code=400, detail="alert_level must be off/digest/realtime")
            sets.append("alert_level = %s"); params.append(update.alert_level)
        if update.is_archived is not None:
            sets.append("is_archived = %s"); params.append(bool(update.is_archived))
        if update.in_my_searches is not None:
            sets.append("in_my_searches = %s"); params.append(bool(update.in_my_searches))
        if not sets:
            return {"message": "No changes"}
        params.append(search_id)
        params.append(user_id)
        c.execute("UPDATE pv_searches SET " + ", ".join(sets) + " WHERE id = %s AND user_id = %s", params)
        conn.commit()
        return {"message": "Search updated", "id": search_id}


@app.delete("/pv/searches/{search_id}")
async def delete_search(search_id: int, user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM pv_searches WHERE id = %s AND user_id = %s", (search_id, user_id))
        conn.commit()
        if c.rowcount == 0:
            raise HTTPException(status_code=404, detail="Search not found")
        return {"message": "Search removed"}


@app.get("/pv/searches/{search_id}/jobs")
async def get_search_jobs(search_id: int, user_id: int = Depends(get_current_user)):
    """Drawer payload — does a LIVE filter-match query against pv_jobs for this
    search's filters (company/sector/level) + location branch (radius or remote).
    Returns up to 50 results sorted by recency. Static map only rendered for
    non-remote searches (remote has no geographic anchor)."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT id, name, company, sector, level, location_name, lat, lng,
                            radius_value, is_archived, in_my_searches
                     FROM pv_searches WHERE id = %s AND user_id = %s""",
                  (search_id, user_id))
        search = c.fetchone()
        if not search:
            raise HTTPException(status_code=404, detail="Search not found")

        s_lat = float(search[6]) if search[6] is not None else None
        s_lng = float(search[7]) if search[7] is not None else None
        s_radius = search[8]

        # Filter-match query. Each filter is "NULL = any"; location branches on
        # radius_value. Sort is recency only (no severity concept for jobs).
        c.execute("""
            SELECT j.id, j.source, j.external_id, j.title, j.company, j.sector, j.level,
                   j.location_name, j.lat, j.lng, j.is_remote,
                   j.salary_min, j.salary_max, j.salary_currency,
                   j.description, j.url, j.posted_at,
                   CASE
                     WHEN s.geom IS NOT NULL AND j.geom IS NOT NULL
                     THEN ST_Distance(j.geom, s.geom) / 1609.344
                     ELSE NULL
                   END AS distance_mi
            FROM pv_jobs j, pv_searches s
            WHERE s.id = %s
              AND (s.company IS NULL OR j.company ILIKE '%%' || s.company || '%%')
              AND (s.sector IS NULL OR j.sector = s.sector)
              AND (s.level IS NULL OR j.level = s.level)
              AND (
                (s.radius_value = 'remote' AND j.is_remote = TRUE)
                OR
                (s.radius_value != 'remote' AND s.geom IS NOT NULL AND j.geom IS NOT NULL
                 AND ST_DWithin(j.geom, s.geom, (s.radius_value::int) * 1609.344))
              )
              AND j.posted_at > NOW() - INTERVAL '30 days'
            ORDER BY j.posted_at DESC
            LIMIT 50
        """, (search_id,))

        jobs = []
        job_pins = []
        for row in c.fetchall():
            j_lat = float(row[8]) if row[8] is not None else None
            j_lng = float(row[9]) if row[9] is not None else None
            jobs.append({
                "id": row[0],
                "source": row[1],
                "external_id": row[2],
                "title": row[3],
                "company": row[4],
                "sector": row[5],
                "level": row[6],
                "location_name": row[7],
                "lat": j_lat,
                "lng": j_lng,
                "is_remote": bool(row[10]),
                "salary_min": float(row[11]) if row[11] is not None else None,
                "salary_max": float(row[12]) if row[12] is not None else None,
                "salary_currency": row[13],
                "description": row[14],
                "url": row[15],
                "posted_at": str(row[16]) if row[16] else None,
                "distance_mi": round(float(row[17]), 1) if row[17] is not None else None,
            })
            if j_lat is not None and j_lng is not None:
                # (lat, lng, severity_placeholder, hazard_type_placeholder) -- the
                # static map builder uses 4-tuples; we pass 'job' as the hazard
                # to drive a single briefcase icon for all matches.
                job_pins.append((j_lat, j_lng, "minor", "job"))

        # Render the static map only for radius-based searches. Remote-only
        # searches have no geographic anchor.
        static_map_url = None
        if s_radius != "remote" and s_lat is not None and s_lng is not None:
            try:
                radius_mi = int(s_radius)
            except Exception:
                radius_mi = 25
            static_map_url = build_static_map_url(s_lat, s_lng, radius_mi, job_pins)

        return {
            "search": {
                "id": search[0],
                "name": search[1],
                "company": search[2],
                "sector": search[3],
                "level": search[4],
                "location_name": search[5],
                "lat": s_lat,
                "lng": s_lng,
                "radius_value": s_radius,
                "is_archived": bool(search[9]),
                "in_my_searches": bool(search[10]),
            },
            "jobs": jobs,
            "count": len(jobs),
            "static_map_url": static_map_url,
        }


def build_static_map_url(lat: float, lng: float, radius_mi: float, ev_pins: list) -> Optional[str]:
    """Return a Google Static Maps URL showing the place + radius circle + event pins.
    If GOOGLE_GEOCODING_API_KEY is unset, returns None (frontend just hides the map).
    Zoom is computed from the radius so the circle fills most of the frame; we don't
    let Google auto-fit because event markers far outside the radius would zoom out
    too aggressively and shrink the user's place to a dot."""
    if not GOOGLE_GEOCODING_API_KEY:
        return None
    import math
    pts = []
    R_EARTH_MI = 3958.8
    for i in range(0, 37):
        angle = (i / 36.0) * 2 * math.pi
        dlat = (radius_mi / R_EARTH_MI) * math.cos(angle) * (180.0 / math.pi)
        dlng = (radius_mi / R_EARTH_MI) * math.sin(angle) * (180.0 / math.pi) / max(0.01, math.cos(math.radians(lat)))
        pts.append((lat + dlat, lng + dlng))
    path = "color:0x0d9488ff|weight:2|fillcolor:0x0d948833|" + "|".join([str(round(p[0], 5)) + "," + str(round(p[1], 5)) for p in pts])

    markers = []
    markers.append("color:0x0d9488|label:H|" + str(lat) + "," + str(lng))
    # Per-hazard Twemoji PNG icons (same emoji rendered in the drawer cards).
    # Pinned to twemoji v14.0.2 on jsdelivr for stability. If Google can't fetch
    # the icon URL, that marker drops silently -- map + Home pin still render.
    hazard_cp = {
        "job":              "1f4bc",   # 💼 (pivot.watch: all job pins)
        "volcano":          "1f30b",   # 🌋
        "wildfire":         "1f525",   # 🔥
        "earthquake":       "1f310",   # 🌐
        "hurricane":        "1f300",   # 🌀
        "tropical_cyclone": "1f300",   # 🌀
        "tornado":          "1f32a",   # 🌪
        "tsunami":          "1f30a",   # 🌊
        "flood":            "1f30a",   # 🌊
        "severe_weather":   "26c8",    # ⛈
        "winter_storm":     "2744",    # ❄
    }
    icon_base = "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/"
    sev_color = {"extreme": "0xdc2626", "severe": "0xea580c", "moderate": "0xd97706", "minor": "0xa16207"}
    for pin in ev_pins[:20]:
        # Accept both (lat, lng, sev) legacy and (lat, lng, sev, hazard_type) v0.1.11+.
        elat, elng = pin[0], pin[1]
        sev = pin[2] if len(pin) >= 3 else "minor"
        hazard_type = pin[3] if len(pin) >= 4 else None
        cp = hazard_cp.get(hazard_type or "")
        if cp:
            markers.append("icon:" + icon_base + cp + ".png|" + str(round(elat, 5)) + "," + str(round(elng, 5)))
        else:
            # No hazard icon mapping -- fall back to a severity-colored dot.
            c = sev_color.get(sev or "minor", "0xa16207")
            markers.append("color:" + c + "|" + str(round(elat, 5)) + "," + str(round(elng, 5)))

    # Zoom math: a 640px-wide map at zoom z covers ~ 156543.03 / 2^z meters per pixel
    # at the equator, scaled by cos(lat). We want the radius diameter (2*radius) to fit
    # ~85% of the 640px width, so:
    #   2 * radius_meters = 0.85 * 640 * (156543.03 / 2^z) * cos(lat)
    # solve for z. Clamp to [3, 14] so we never zoom past country-level or street-level.
    radius_m = radius_mi * 1609.344
    cos_lat = max(0.01, math.cos(math.radians(lat)))
    target_m_per_pixel = (2 * radius_m) / (0.85 * 640)
    if target_m_per_pixel <= 0:
        zoom = 9
    else:
        zoom_float = math.log2((156543.03 * cos_lat) / target_m_per_pixel)
        zoom = max(3, min(14, int(round(zoom_float))))

    params = [
        "size=640x320",
        "scale=2",
        "maptype=terrain",
        "zoom=" + str(zoom),
        "center=" + str(lat) + "," + str(lng),
        "path=" + urllib.parse.quote(path),
    ]
    for m in markers:
        params.append("markers=" + urllib.parse.quote(m))
    params.append("key=" + GOOGLE_GEOCODING_API_KEY)
    return "https://maps.googleapis.com/maps/api/staticmap?" + "&".join(params)


# ── Google Geocoding (Location field on Add Search) ───────────────────────────

class GeocodeQuery(BaseModel):
    query: str

@app.post("/pv/geocode")
async def geocode_place(q: GeocodeQuery, user_id: int = Depends(get_current_user)):
    """Forward geocode a free-text query (city, address, landmark) via Google.
    Returns up to 5 candidates so the user can pick the right match.
    Backend-side so the API key never lives in the frontend."""
    if not GOOGLE_GEOCODING_API_KEY:
        raise HTTPException(status_code=503, detail="Geocoding service not configured")
    text = (q.query or "").strip()
    if len(text) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    try:
        url = ("https://maps.googleapis.com/maps/api/geocode/json?address="
               + urllib.parse.quote(text)
               + "&key=" + GOOGLE_GEOCODING_API_KEY)
        req = urllib.request.Request(url, headers={"User-Agent": "pivot.watch/0.1"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        print("[geocode] " + str(e))
        raise HTTPException(status_code=502, detail="Geocoding lookup failed")
    status = data.get("status")
    if status == "ZERO_RESULTS":
        return {"candidates": []}
    if status != "OK":
        # Don't leak Google error_message to client — just say it failed.
        print("[geocode] Google returned status=" + str(status) + " for query: " + text)
        raise HTTPException(status_code=502, detail="Geocoding lookup failed")
    candidates = []
    for r in (data.get("results") or [])[:5]:
        loc = ((r.get("geometry") or {}).get("location")) or {}
        lat = loc.get("lat")
        lng = loc.get("lng")
        if lat is None or lng is None:
            continue
        candidates.append({
            "formatted_address": r.get("formatted_address", ""),
            "lat": float(lat),
            "lng": float(lng),
            "place_id": r.get("place_id", ""),
        })
    return {"candidates": candidates}


# ── Notifications (alerts feed) ───────────────────────────────────────────────

@app.get("/notifications")
async def get_notifications(user_id: int = Depends(get_current_user)):
    """Alerts feed. pivot.watch writes notifications with source_type='pv_job'
    and source_ref_id pointing to pv_job_matches.id."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT n.id, n.message, n.created_at, n.watchlist_id, n.source_type, n.source_ref_id,
                   s.name AS search_name
            FROM notifications n
            LEFT JOIN pv_searches s ON n.watchlist_id = s.id
            WHERE n.user_id = %s
            ORDER BY n.created_at DESC LIMIT 100
        """, (user_id,))
        notifications = []
        for row in c.fetchall():
            notifications.append({
                "id": row[0],
                "message": row[1],
                "created_at": str(row[2]),
                "watchlist_id": row[3],
                "source_type": row[4] or "",
                "source_ref_id": row[5],
                "name": row[6] or "",
            })
        return notifications


@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user_id: int = Depends(get_current_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM notifications WHERE id = %s AND user_id = %s", (notif_id, user_id))
            conn.commit()
            return {"deleted": True}
    except Exception as e:
        print("Delete notification error: " + str(e))
        return {"deleted": False}


# ── Source adapters ───────────────────────────────────────────────────────────
# All feeds are wrapped in a Sources class so adding a new one is one new
# method, not a growing if/elif chain. Each adapter returns a normalized list
# of job dicts:
#   {source, external_id, title, company, sector, level, location_name,
#    lat, lng, is_remote, salary_min, salary_max, salary_currency,
#    description, url, posted_at, raw}
# The cron upserts these into pv_jobs keyed on (source, external_id).
#
# v0.1.0 ships with three stubs returning []. Wire each one in its own version:
#   v0.1.1 → USAJobs (federal, free, clean)
#   v0.1.2 → Adzuna  (US private-board aggregator)
#   v0.1.3 → Greenhouse (per-employer feeds for watched companies)


# ── USAJobs normalization helpers ─────────────────────────────────────────────
# Maps a single USAJobs SearchResultItem into the dict shape pv_jobs expects,
# plus the OPM occupational-series → sector and GS-grade → level mappings.

def _usajobs_sector_for_series(code) -> str:
    """OPM occupational series → one of our 12 sector slugs. All USAJobs
    postings are federal, so 'government' is the catch-all default."""
    if not code:
        return "government"
    c = str(code).zfill(4)
    # 22XX = IT family (Information Technology Management etc.)
    if c.startswith("22"):
        return "it"
    # 0810 (Civil Engineering) + 0828 (Construction Analyst) → construction
    if c in ("0810", "0828"):
        return "construction"
    # 08XX = Engineering family
    if c.startswith("08"):
        return "engineering"
    # Legal: 0901 (General Legal), 0904 (Law Clerk), 0905 (General Attorney)
    if c in ("0901", "0904", "0905"):
        return "legal"
    # Healthcare: Medical Officer, Nurse, Pharmacist, Dental, Health Tech families
    if c in ("0602", "0610", "0660", "0680", "0640", "0644", "0671", "0699"):
        return "healthcare"
    # 05XX = Accounting and Budget family
    if c.startswith("05"):
        return "finance"
    # 17XX = Education family
    if c.startswith("17"):
        return "education"
    # 02XX = Human Resources family
    if c.startswith("02"):
        return "hr"
    # 1083 = Public Affairs (closest federal match for Marketing/PR)
    if c == "1083":
        return "marketing"
    # 1170 = Realty (closest federal match for Sales)
    if c == "1170":
        return "sales"
    return "government"


def _usajobs_level_from_title(title: str) -> str:
    """Parse GS-NN grade from the position title and bucket into our four
    levels. Falls back to keyword scan, then 'mid' as default."""
    if not title:
        return "mid"
    t = title.upper()
    # Senior Executive Service / Executive Service.
    if " SES" in t or "(SES)" in t or " ES-" in t or t.endswith(" ES"):
        return "executive"
    # GS-NN pattern (also matches GS NN and GSNN).
    m = re.search(r"GS[\s\-]?(\d{1,2})", t)
    if m:
        try:
            grade = int(m.group(1))
            if grade <= 7:
                return "entry"
            if grade <= 12:
                return "mid"
            if grade <= 14:
                return "senior"
            return "executive"
        except Exception:
            pass
    # Title-keyword fallback.
    if any(w in t for w in ["INTERN", "TRAINEE", "STUDENT", "JUNIOR", "ENTRY LEVEL"]):
        return "entry"
    if any(w in t for w in ["DIRECTOR", "CHIEF", "EXECUTIVE", "VICE PRESIDENT"]):
        return "executive"
    if any(w in t for w in ["SENIOR", "PRINCIPAL", "LEAD"]):
        return "senior"
    return "mid"


def _normalize_usajobs_item(item: dict) -> Optional[dict]:
    """Map one USAJobs SearchResultItem into the pv_jobs dict shape, or None
    if essential fields are missing."""
    desc = item.get("MatchedObjectDescriptor") or {}
    external_id = item.get("MatchedObjectId") or desc.get("PositionID")
    if not external_id:
        return None

    title = desc.get("PositionTitle") or ""
    company = desc.get("OrganizationName") or desc.get("DepartmentName") or ""

    # Location: take the first entry of PositionLocation. Multi-site postings
    # exist; for v0.1.1 we anchor on the first.
    locations = desc.get("PositionLocation") or []
    loc = locations[0] if locations else {}
    location_name = (loc.get("LocationName")
                     or desc.get("PositionLocationDisplay")
                     or "")
    lat = loc.get("Latitude")
    lng = loc.get("Longitude")
    try:
        lat = float(lat) if lat not in (None, "", 0, "0", 0.0) else None
        lng = float(lng) if lng not in (None, "", 0, "0", 0.0) else None
    except Exception:
        lat = None
        lng = None

    # Remote: UserArea.Details.RemoteIndicator boolean.
    details = (desc.get("UserArea") or {}).get("Details") or {}
    is_remote = bool(details.get("RemoteIndicator"))

    # Salary: take PositionRemuneration[0]. Convert hourly to annual (×2080)
    # so the frontend formatSalary stays consistent across sources.
    rem = desc.get("PositionRemuneration") or []
    salary_min = None
    salary_max = None
    if rem:
        r = rem[0] or {}
        try:
            interval = (r.get("RateIntervalCode") or "").strip()
            mult = 2080.0 if interval == "PH" else 1.0
            sm = r.get("MinimumRange")
            xm = r.get("MaximumRange")
            if sm not in (None, ""):
                salary_min = float(sm) * mult
            if xm not in (None, ""):
                salary_max = float(xm) * mult
        except Exception:
            salary_min = None
            salary_max = None

    # Sector mapping from JobCategory[0].Code.
    cats = desc.get("JobCategory") or []
    series_code = cats[0].get("Code") if cats else ""
    sector = _usajobs_sector_for_series(series_code)

    # Level from title (GS-NN or SES/keyword).
    level = _usajobs_level_from_title(title)

    # Posted date — USAJobs returns YYYY-MM-DD.
    posted_at = desc.get("PublicationStartDate")

    # URL: PositionURI is the canonical viewable link.
    url = desc.get("PositionURI") or ""

    # Description: cap at 2KB to keep raw_payload manageable.
    description = (details.get("JobSummary") or "")
    if description and len(description) > 2000:
        description = description[:2000] + "…"

    return {
        "source": "usajobs",
        "external_id": str(external_id),
        "title": title,
        "company": company,
        "sector": sector,
        "level": level,
        "location_name": location_name,
        "lat": lat,
        "lng": lng,
        "is_remote": is_remote,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": "USD",
        "description": description,
        "url": url,
        "posted_at": posted_at,
        "raw": desc,
    }


class Sources:
    """All job feed adapters. Each fetch_* returns a list of normalized job dicts."""

    BROWSER_UA = "pivot.watch/0.1 (https://pivot.watch; alerts@pivot.watch)"

    @staticmethod
    def _http_get_json(url: str, timeout: int = 20, extra_headers: Optional[dict] = None) -> Optional[dict]:
        """Fetch JSON with the pivot.watch UA. extra_headers lets adapters add
        per-source auth headers (USAJobs Authorization-Key, etc.)."""
        try:
            headers = {"User-Agent": Sources.BROWSER_UA, "Accept": "application/json"}
            if extra_headers:
                headers.update(extra_headers)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print("[http] " + url + " failed: " + str(e))
            return None

    # ── USAJobs (federal jobs, free, requires API key + email as User-Agent) ──
    # Stub for v0.1.0. v0.1.1+ pulls from
    # https://data.usajobs.gov/api/search with these env vars:
    #   USAJOBS_API_KEY        -- request at developer.usajobs.gov
    #   USAJOBS_USER_AGENT     -- the email you registered with
    @staticmethod
    def fetch_usajobs() -> List[dict]:
        api_key = (os.environ.get("USAJOBS_API_KEY") or "").strip()
        user_agent = (os.environ.get("USAJOBS_USER_AGENT") or "").strip()
        if not api_key or not user_agent:
            print("[usajobs] USAJOBS_API_KEY or USAJOBS_USER_AGENT not set; skipping")
            return []

        headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": user_agent,
            "Authorization-Key": api_key,
            "Accept": "application/json",
        }

        out = []
        max_pages = 5
        results_per_page = 500

        for page in range(1, max_pages + 1):
            url = ("https://data.usajobs.gov/api/search"
                   + "?ResultsPerPage=" + str(results_per_page)
                   + "&Page=" + str(page))
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
            except Exception as e:
                print("[usajobs] page " + str(page) + " fetch failed: " + str(e))
                break

            search_result = data.get("SearchResult") or {}
            items = search_result.get("SearchResultItems") or []
            if not items:
                break

            for item in items:
                try:
                    normalized = _normalize_usajobs_item(item)
                    if normalized:
                        out.append(normalized)
                except Exception as e:
                    print("[usajobs] normalize failed: " + str(e))

            # Last page if we got fewer than a full page back.
            if len(items) < results_per_page:
                break

        print("[usajobs] fetched " + str(len(out)) + " jobs")
        return out

    # ── Adzuna (US private-board aggregator, free with key) ───────────────────
    # Stub for v0.1.0. v0.1.2 will pull from
    # https://api.adzuna.com/v1/api/jobs/us/search/1 with:
    #   ADZUNA_APP_ID
    #   ADZUNA_APP_KEY
    @staticmethod
    def fetch_adzuna() -> List[dict]:
        return []

    # ── Greenhouse (per-employer ATS feeds) ───────────────────────────────────
    # Stub for v0.1.0. v0.1.3 will pull from
    # https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs
    # for each company name appearing in active pv_searches rows. Public, no key.
    @staticmethod
    def fetch_greenhouse() -> List[dict]:
        return []

    @staticmethod
    def all_sources() -> List[str]:
        return ["usajobs", "adzuna", "greenhouse"]

    @staticmethod
    def fetch(source: str) -> List[dict]:
        if source == "usajobs":
            return Sources.fetch_usajobs()
        if source == "adzuna":
            return Sources.fetch_adzuna()
        if source == "greenhouse":
            return Sources.fetch_greenhouse()
        return []



# ── Cron: pull all sources, filter-match against searches, fire alerts ────────

def upsert_jobs(conn, normalized: List[dict]) -> List[int]:
    """Upsert normalized jobs into pv_jobs. Returns the ids of rows that were
    NEWLY inserted (so we only filter-match the new ones)."""
    if not normalized:
        return []
    new_ids = []
    c = conn.cursor()
    for j in normalized:
        try:
            # Geometry: NULL if no lat/lng (remote-only postings).
            geom_wkt = None
            if j.get("lat") is not None and j.get("lng") is not None:
                geom_wkt = "POINT(" + str(j["lng"]) + " " + str(j["lat"]) + ")"
            c.execute("""
                INSERT INTO pv_jobs (
                    source, external_id, title, company, sector, level,
                    location_name, lat, lng, is_remote,
                    salary_min, salary_max, salary_currency,
                    description, url, posted_at, raw_payload, geom
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s::jsonb,
                    CASE WHEN %s IS NOT NULL
                         THEN ST_SetSRID(ST_GeomFromText(%s), 4326)::geography
                         ELSE NULL END
                )
                ON CONFLICT (source, external_id) DO NOTHING
                RETURNING id
            """, (
                j["source"], j["external_id"], j.get("title"), j.get("company"),
                j.get("sector"), j.get("level"),
                j.get("location_name"), j.get("lat"), j.get("lng"),
                bool(j.get("is_remote") or False),
                j.get("salary_min"), j.get("salary_max"), j.get("salary_currency"),
                j.get("description"), j.get("url"), j.get("posted_at"),
                json_lib.dumps(j.get("raw") or {}, default=str),
                geom_wkt, geom_wkt,
            ))
            row = c.fetchone()
            if row:
                new_ids.append(row[0])
        except Exception as e:
            print("[upsert_jobs] " + j.get("source", "?") + ":" + str(j.get("external_id", "?")) + " — " + str(e))
            conn.rollback()
            c = conn.cursor()
    conn.commit()
    return new_ids


def filter_match_and_alert(conn, new_job_ids: List[int]) -> int:
    """For every newly-inserted job, find every active search whose filters
    AND geofence (or remote flag) match, insert a match row, and write a
    notification for any match that hasn't been alerted on yet."""
    if not new_job_ids:
        return 0
    c = conn.cursor()
    # Match condition mirrors the drawer SQL: company ILIKE, sector exact,
    # level exact, and the radius/remote location branch.
    c.execute("""
        INSERT INTO pv_job_matches (job_id, search_id, distance_mi)
        SELECT j.id, s.id,
               CASE
                 WHEN s.geom IS NOT NULL AND j.geom IS NOT NULL
                 THEN ST_Distance(j.geom, s.geom) / 1609.344
                 ELSE NULL
               END
        FROM pv_jobs j
        JOIN pv_searches s
          ON s.alert_level <> 'off'
         AND (s.company IS NULL OR j.company ILIKE '%%' || s.company || '%%')
         AND (s.sector  IS NULL OR j.sector = s.sector)
         AND (s.level   IS NULL OR j.level  = s.level)
         AND (
              (s.radius_value = 'remote' AND j.is_remote = TRUE)
              OR
              (s.radius_value != 'remote' AND s.geom IS NOT NULL AND j.geom IS NOT NULL
               AND ST_DWithin(j.geom, s.geom, (s.radius_value::int) * 1609.344))
         )
        WHERE j.id = ANY(%s)
        ON CONFLICT (job_id, search_id) DO NOTHING
    """, (new_job_ids,))
    conn.commit()

    # Fire alerts for every new match (alerted_at IS NULL).
    c.execute("""
        SELECT m.id, m.job_id, m.search_id, m.distance_mi,
               j.title, j.company, j.location_name,
               s.user_id, s.name
        FROM pv_job_matches m
        JOIN pv_jobs j ON m.job_id = j.id
        JOIN pv_searches s ON m.search_id = s.id
        WHERE m.alerted_at IS NULL
          AND m.job_id = ANY(%s)
    """, (new_job_ids,))
    rows = c.fetchall()
    fired = 0
    for row in rows:
        match_id, job_id, search_id, distance_mi, title, company, loc, user_id, search_name = row
        try:
            where = ""
            if loc:
                where = " — " + loc
            if distance_mi is not None:
                where = where + " (" + str(round(distance_mi, 1)) + " mi)"
            headline = (title or "New job")
            if company:
                headline = headline + " @ " + company
            message = headline + where + " matches your search: " + (search_name or "")
            c.execute("""
                INSERT INTO notifications (user_id, watchlist_id, message, source_type, source_ref_id)
                VALUES (%s, %s, %s, 'pv_job', %s)
            """, (user_id, search_id, message, match_id))
            c.execute("UPDATE pv_job_matches SET alerted_at = CURRENT_TIMESTAMP WHERE id = %s", (match_id,))
            fired += 1
        except Exception as e:
            print("[filter_match_and_alert] alert write failed for match " + str(match_id) + ": " + str(e))
            conn.rollback()
            c = conn.cursor()
    conn.commit()
    return fired


def run_check_cycle():
    """Pull every source once, upsert into pv_jobs, filter-match new jobs
    against active searches, write notifications for new matches."""
    print("[cron] pivot.watch check cycle starting at " + datetime.utcnow().isoformat())
    total_new = 0
    total_alerts = 0
    for source in Sources.all_sources():
        try:
            normalized = Sources.fetch(source)
            if not normalized:
                print("[cron] " + source + ": 0 jobs")
                continue
            with get_db() as conn:
                new_ids = upsert_jobs(conn, normalized)
                total_new += len(new_ids)
                fired = filter_match_and_alert(conn, new_ids)
                total_alerts += fired
                print("[cron] " + source + ": fetched=" + str(len(normalized))
                      + " new=" + str(len(new_ids)) + " alerts=" + str(fired))
        except Exception as e:
            print("[cron] source " + source + " failed: " + str(e))
    print("[cron] pivot.watch check cycle complete. new_jobs=" + str(total_new) + " alerts_fired=" + str(total_alerts))


def run_scheduler():
    # v0.1.1: 12-hour cycle for free tier. Per-user check_interval enforcement
    # (premium = faster cycle) is v0.2.
    # Uses plain time.time() — no scheduler library needed (sidesteps Render
    # cached-venv issues with packages like `schedule` / `apscheduler`).
    INTERVAL_SEC = 60 * 60 * 12  # 12 hours
    last_run = 0.0
    while True:
        now = time.time()
        if now - last_run >= INTERVAL_SEC:
            try:
                run_check_cycle()
            except Exception as e:
                print("[scheduler] check cycle failed: " + str(e))
            last_run = now
        time.sleep(60)


@app.get("/admin/signup-stats")
async def admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    """Read-only signup metrics for the 3Brains scoreboard.
    Requires X-Admin-Token header matching ADMIN_STATS_TOKEN env var."""
    expected = os.environ.get("ADMIN_STATS_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '24 hours'")
        signups_24h = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '7 days'")
        signups_7d = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '30 days'")
        signups_30d = c.fetchone()[0]
        c.execute("SELECT MAX(created_at) FROM users")
        latest_row = c.fetchone()
        latest = latest_row[0].isoformat() if latest_row and latest_row[0] else None
        return {
            "total_users": total_users,
            "signups_24h": signups_24h,
            "signups_7d": signups_7d,
            "signups_30d": signups_30d,
            "latest_signup_at": latest
        }


@app.on_event("startup")
async def startup_event():
    init_db()
    print("Database initialized (pivot.watch v" + VERSION + ")")
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("Background scheduler started (12-hour check cycle: usajobs + adzuna + greenhouse)")
    threading.Thread(target=run_check_cycle, daemon=True).start()
    print("Initial EW check cycle started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
