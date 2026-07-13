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

VERSION = "0.1.22"

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

    # ── v0.1.10 schema additions: where_mode + state filters ─────────────────
    # pv_searches.where_mode: 'address' | 'state' | 'anywhere'
    # pv_searches.state_code: 2-letter US state code, populated only when
    #                          where_mode = 'state'
    # pv_jobs.state: 2-letter US state code extracted at ingest time from
    #                the source's location string. Enables state-mode matching
    #                without expensive geocoding per query.
    c.execute("ALTER TABLE pv_searches ADD COLUMN IF NOT EXISTS where_mode TEXT NOT NULL DEFAULT 'address'")
    c.execute("ALTER TABLE pv_searches ADD COLUMN IF NOT EXISTS state_code CHAR(2)")
    c.execute("ALTER TABLE pv_jobs ADD COLUMN IF NOT EXISTS state CHAR(2)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_jobs_state ON pv_jobs (state)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_searches_where_mode ON pv_searches (where_mode)")
    # Backfill: existing rows with radius_value='remote' become anywhere-mode.
    # Idempotent — only flips rows that haven't been migrated yet.
    c.execute("UPDATE pv_searches SET where_mode = 'anywhere' WHERE radius_value = 'remote' AND where_mode = 'address'")

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

    # pv_signups: pivot-specific signup tracking. The `users` table is shared
    # across all apps on the same Postgres (Earth Watch, H2O Watch, etc. all
    # write there), so COUNT(*) FROM users returns the cross-app total -- not
    # useful for measuring pivot.watch adoption specifically. pv_signups
    # captures only users who registered or logged in through pivot.
    c.execute("""CREATE TABLE IF NOT EXISTS pv_signups (
        user_id INTEGER PRIMARY KEY,
        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pv_signups_seen ON pv_signups (first_seen_at)")

    # Backfill: any user who has at least one pv_searches row clearly used
    # pivot.watch. Backfill them with their earliest search time as their
    # pivot signup timestamp. Idempotent thanks to ON CONFLICT DO NOTHING.
    c.execute("""
        INSERT INTO pv_signups (user_id, first_seen_at)
        SELECT user_id, MIN(created_at) FROM pv_searches GROUP BY user_id
        ON CONFLICT (user_id) DO NOTHING
    """)

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
    location_name: Optional[str] = None  # human-readable; NULL for anywhere/state
    lat: Optional[float] = None          # required if where_mode='address'
    lng: Optional[float] = None          # required if where_mode='address'
    radius_value: str = "25"             # '1' | '5' | '10' | '25' (only used in address mode)
    where_mode: Optional[str] = None     # 'address' | 'state' | 'anywhere'; None = derive from radius_value
    state_code: Optional[str] = None     # 2-letter US state code; required when where_mode='state'
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
    where_mode: Optional[str] = None
    state_code: Optional[str] = None
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
    where_mode: str
    state_code: Optional[str]
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


@app.get("/admin/platform-stats")
async def admin_platform_stats():
    """Aggregate signup metrics for the scoreboard. Public, no auth — counts only."""
    # Per-backend config: (actual_table_name, response_key_for_scoreboard)
    targets = [("users", "pv_users")]
    result = {}
    with get_db() as conn:
        c = conn.cursor()
        for table, key in targets:
            try:
                c.execute("""
                    SELECT
                        COUNT(*),
                        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours'),
                        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days'),
                        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days'),
                        MAX(created_at)
                    FROM {}
                """.format(table))
                row = c.fetchone()
                result[key] = {
                    "total_users": row[0],
                    "signups_24h": row[1],
                    "signups_7d": row[2],
                    "signups_30d": row[3],
                    "latest_signup_at": row[4].isoformat() if row[4] else None
                }
            except Exception as e:
                conn.rollback()
                result[key] = {"error": str(e)}
        return result


@app.get("/admin/check-now")
async def admin_check_now():
    """Manually trigger a full source pull + filter-join + alert pass."""
    threading.Thread(target=run_check_cycle, daemon=True).start()
    return {"message": "pivot.watch check cycle started"}


@app.get("/admin/cleanup-wake-island")
async def admin_cleanup_wake_island():
    """One-off purge of Adzuna's polluted 'Wake Island, Honolulu' rows. These
    are jobs Adzuna couldn't precisely geocode and defaulted to the Wake
    Island administrative center coords (21.3072, -157.8465) -- the actual
    jobs are scattered nationwide (Nashville, AZ, etc.). v0.1.17 normalizer
    prevents new pollution; this endpoint clears existing rows immediately
    instead of waiting up to 12h for cron to overwrite them."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM pv_jobs
            WHERE source = 'adzuna'
              AND LOWER(location_name) LIKE 'wake island%'
        """)
        before = int(c.fetchone()[0])
        c.execute("""
            DELETE FROM pv_jobs
            WHERE source = 'adzuna'
              AND LOWER(location_name) LIKE 'wake island%'
        """)
        conn.commit()
        return {"deleted": before, "version": VERSION}


@app.get("/admin/inspect")
async def admin_inspect(sector: Optional[str] = None, source: Optional[str] = None, limit: int = 20):
    """Diagnostic endpoint. Returns (a) (source, sector) breakdown counts across
    all of pv_jobs, plus (b) a sample of recently-fetched jobs matching optional
    filters. Used to verify what the cron actually ingested vs what the drawer
    SQL is filtering against."""
    with get_db() as conn:
        c = conn.cursor()

        # Full breakdown by source × sector — every bucket with rows.
        c.execute("""
            SELECT source, COALESCE(sector, '(null)') AS sector, COUNT(*)
            FROM pv_jobs
            GROUP BY source, COALESCE(sector, '(null)')
            ORDER BY source, sector
        """)
        breakdown = [{"source": r[0], "sector": r[1], "count": r[2]} for r in c.fetchall()]

        # How many jobs have valid lat/lng (j.geom IS NOT NULL).
        c.execute("SELECT source, COUNT(*) FROM pv_jobs WHERE geom IS NOT NULL GROUP BY source")
        geom_by_source = {r[0]: r[1] for r in c.fetchall()}

        # Recent fetched_at distribution.
        c.execute("""
            SELECT
              COUNT(*) FILTER (WHERE fetched_at > NOW() - INTERVAL '1 hour') AS hr1,
              COUNT(*) FILTER (WHERE fetched_at > NOW() - INTERVAL '1 day')  AS d1,
              COUNT(*) FILTER (WHERE fetched_at > NOW() - INTERVAL '3 days') AS d3,
              COUNT(*) FILTER (WHERE fetched_at > NOW() - INTERVAL '7 days') AS d7
            FROM pv_jobs
        """)
        freshness_row = c.fetchone()
        freshness = {
            "fetched_last_hour": freshness_row[0],
            "fetched_last_day":  freshness_row[1],
            "fetched_last_3d":   freshness_row[2],
            "fetched_last_7d":   freshness_row[3],
        }

        # Sample matching the filters, most-recently-fetched first.
        where_sql = []
        params = []
        if sector:
            where_sql.append("sector = %s")
            params.append(sector)
        if source:
            where_sql.append("source = %s")
            params.append(source)
        where_clause = (" WHERE " + " AND ".join(where_sql)) if where_sql else ""

        # State breakdown for whatever filter the caller applied. Tells us
        # at-a-glance where the matching jobs are geographically, which
        # answers "does coverage exist for the geo this user is searching?".
        c.execute(
            "SELECT COALESCE(state, '(null)') AS s, COUNT(*) "
            "FROM pv_jobs" + where_clause + " "
            "GROUP BY COALESCE(state, '(null)') "
            "ORDER BY COUNT(*) DESC",
            params
        )
        breakdown_by_state = [{"state": r[0], "count": r[1]} for r in c.fetchall()]

        c.execute(
            "SELECT id, source, company, title, sector, level, location_name, "
            "lat, lng, state, is_remote, posted_at, fetched_at "
            "FROM pv_jobs" + where_clause + " "
            "ORDER BY fetched_at DESC LIMIT %s",
            params + [limit]
        )
        samples = []
        for r in c.fetchall():
            samples.append({
                "id": r[0],
                "source": r[1],
                "company": r[2],
                "title": r[3],
                "sector": r[4],
                "level": r[5],
                "location_name": r[6],
                "lat": float(r[7]) if r[7] is not None else None,
                "lng": float(r[8]) if r[8] is not None else None,
                "state": r[9],
                "is_remote": r[10],
                "posted_at": r[11].isoformat() if r[11] else None,
                "fetched_at": r[12].isoformat() if r[12] else None,
            })

        return {
            "filter_applied": {"sector": sector, "source": source, "limit": limit},
            "breakdown_by_source_sector": breakdown,
            "breakdown_by_state": breakdown_by_state,
            "rows_with_valid_geom_by_source": geom_by_source,
            "freshness": freshness,
            "sample_count": len(samples),
            "samples": samples,
        }


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
        # Record this user as a pivot.watch signup. The users table is shared
        # across apps on this Postgres, so the pv_signups row is how the
        # scoreboard distinguishes pivot users from Earth/H2O/etc.
        c.execute(
            "INSERT INTO pv_signups (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING",
            (user_id,))
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
        # If this user registered elsewhere (Earth/H2O on the shared DB) and
        # is now logging into pivot for the first time, record them as a
        # pivot user. ON CONFLICT keeps the original signup timestamp if
        # they've been here before.
        c.execute(
            "INSERT INTO pv_signups (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING",
            (result[0],))
        conn.commit()
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


# Curated direct-apply directory (v0.1.14+). Closes the data coverage gap for
# big retail / QSR / service employers who don't post to USAJobs, Adzuna, or
# Greenhouse but whose careers pages are well-known. Matching is by lower-case
# substring against `keywords`; first hit wins. Domain field powers the
# favicon URL (Google's free /s2/favicons service). When a company isn't here,
# the endpoint falls back to a constructed Google search URL.
CAREERS_DIRECTORY = {
    "walmart":         {"name": "Walmart",              "url": "https://careers.walmart.com",                   "domain": "walmart.com",            "keywords": ["walmart"]},
    "amazon":          {"name": "Amazon",               "url": "https://www.amazon.jobs",                       "domain": "amazon.com",             "keywords": ["amazon"]},
    "mcdonalds":       {"name": "McDonald's",           "url": "https://careers.mcdonalds.com",                 "domain": "mcdonalds.com",          "keywords": ["mcdonald", "mcdonalds", "mcdonald's"]},
    "homedepot":       {"name": "The Home Depot",       "url": "https://careers.homedepot.com",                 "domain": "homedepot.com",          "keywords": ["home depot", "homedepot"]},
    "kroger":          {"name": "Kroger",               "url": "https://jobs.kroger.com",                       "domain": "kroger.com",             "keywords": ["kroger"]},
    "target":          {"name": "Target",               "url": "https://corporate.target.com/careers",          "domain": "target.com",             "keywords": ["target"]},
    "costco":          {"name": "Costco",               "url": "https://www.costco.com/job-opportunities.html", "domain": "costco.com",             "keywords": ["costco"]},
    "starbucks":       {"name": "Starbucks",            "url": "https://www.starbucks.com/careers",             "domain": "starbucks.com",          "keywords": ["starbucks"]},
    "fedex":           {"name": "FedEx",                "url": "https://careers.fedex.com",                     "domain": "fedex.com",              "keywords": ["fedex"]},
    "ups":             {"name": "UPS",                  "url": "https://www.jobs-ups.com",                      "domain": "ups.com",                "keywords": ["ups", "united parcel"]},
    "lowes":           {"name": "Lowe's",               "url": "https://talent.lowes.com",                      "domain": "lowes.com",              "keywords": ["lowe's", "lowes"]},
    "bestbuy":         {"name": "Best Buy",             "url": "https://jobs.bestbuy.com",                      "domain": "bestbuy.com",            "keywords": ["best buy", "bestbuy"]},
    "ross":            {"name": "Ross Stores",          "url": "https://jobs.rossstores.com",                   "domain": "rossstores.com",         "keywords": ["ross stores", "ross dress"]},
    "tjx":             {"name": "TJX (TJ Maxx)",        "url": "https://www.jobs.tjx.com",                      "domain": "tjx.com",                "keywords": ["tj maxx", "tjmaxx", "tjx", "marshalls", "homegoods", "sierra"]},
    "albertsons":      {"name": "Albertsons",           "url": "https://www.albertsonscompanies.com/careers",   "domain": "albertsons.com",         "keywords": ["albertsons", "safeway"]},
    "publix":          {"name": "Publix",               "url": "https://corporate.publix.com/careers",          "domain": "publix.com",             "keywords": ["publix"]},
    "cvs":             {"name": "CVS Health",           "url": "https://jobs.cvshealth.com",                    "domain": "cvs.com",                "keywords": ["cvs"]},
    "walgreens":       {"name": "Walgreens",            "url": "https://jobs.walgreens.com",                    "domain": "walgreens.com",          "keywords": ["walgreens"]},
    "dollargeneral":   {"name": "Dollar General",       "url": "https://careers.dollargeneral.com",             "domain": "dollargeneral.com",      "keywords": ["dollar general", "dollargeneral"]},
    "dollartree":      {"name": "Dollar Tree",          "url": "https://www.dollartreeinfo.com/careers",        "domain": "dollartree.com",         "keywords": ["dollar tree", "dollartree", "family dollar"]},
    "wholefoods":      {"name": "Whole Foods Market",   "url": "https://www.wholefoodsmarket.com/careers",      "domain": "wholefoodsmarket.com",   "keywords": ["whole foods", "wholefoods"]},
    "traderjoes":      {"name": "Trader Joe's",         "url": "https://www.traderjoes.com/careers",            "domain": "traderjoes.com",         "keywords": ["trader joe", "traderjoes"]},
    "chipotle":        {"name": "Chipotle",             "url": "https://jobs.chipotle.com",                     "domain": "chipotle.com",           "keywords": ["chipotle"]},
    "subway":          {"name": "Subway",               "url": "https://www.subway.com/en-us/aboutus/careers",  "domain": "subway.com",             "keywords": ["subway"]},
    "burgerking":      {"name": "Burger King",          "url": "https://careers.bk.com",                        "domain": "bk.com",                 "keywords": ["burger king", "burgerking"]},
    "wendys":          {"name": "Wendy's",              "url": "https://www.wendys.com/careers",                "domain": "wendys.com",             "keywords": ["wendy", "wendys", "wendy's"]},
    "tacobell":        {"name": "Taco Bell",            "url": "https://www.tacobell.com/careers",              "domain": "tacobell.com",           "keywords": ["taco bell", "tacobell"]},
    "kfc":             {"name": "KFC",                  "url": "https://jobs.kfc.com",                          "domain": "kfc.com",                "keywords": ["kfc", "kentucky fried"]},
    "dominos":         {"name": "Domino's",             "url": "https://jobs.dominos.com",                      "domain": "dominos.com",            "keywords": ["domino", "dominos", "domino's"]},
    "pizzahut":        {"name": "Pizza Hut",            "url": "https://jobs.pizzahut.com",                     "domain": "pizzahut.com",           "keywords": ["pizza hut", "pizzahut"]},
    "chickfila":       {"name": "Chick-fil-A",          "url": "https://www.chick-fil-a.com/careers",           "domain": "chick-fil-a.com",        "keywords": ["chick-fil-a", "chickfila", "chick fil a"]},
    "darden":          {"name": "Darden (Olive Garden)","url": "https://careers.darden.com",                    "domain": "darden.com",             "keywords": ["darden", "olive garden", "longhorn", "yard house", "capital grille"]},
    "marriott":        {"name": "Marriott",             "url": "https://careers.marriott.com",                  "domain": "marriott.com",           "keywords": ["marriott"]},
    "hilton":          {"name": "Hilton",               "url": "https://jobs.hilton.com",                       "domain": "hilton.com",             "keywords": ["hilton"]},
    "hyatt":           {"name": "Hyatt",                "url": "https://careers.hyatt.com",                     "domain": "hyatt.com",              "keywords": ["hyatt"]},
    "disney":          {"name": "Disney",               "url": "https://jobs.disneycareers.com",                "domain": "disney.com",             "keywords": ["disney", "walt disney"]},
    "att":             {"name": "AT&T",                 "url": "https://www.att.jobs",                          "domain": "att.com",                "keywords": ["at&t", "att inc"]},
    "verizon":         {"name": "Verizon",              "url": "https://mycareer.verizon.com",                  "domain": "verizon.com",            "keywords": ["verizon"]},
    "tmobile":         {"name": "T-Mobile",             "url": "https://careers.t-mobile.com",                  "domain": "t-mobile.com",           "keywords": ["t-mobile", "tmobile"]},
    "macys":           {"name": "Macy's",               "url": "https://www.macysjobs.com",                     "domain": "macys.com",              "keywords": ["macy", "macys", "macy's"]},
    "nordstrom":       {"name": "Nordstrom",            "url": "https://careers.nordstrom.com",                 "domain": "nordstrom.com",          "keywords": ["nordstrom"]},
    "gap":             {"name": "Gap Inc.",             "url": "https://jobs.gapinc.com",                       "domain": "gap.com",                "keywords": ["gap inc", "old navy", "banana republic", "athleta"]},
    "bathandbody":     {"name": "Bath & Body Works",    "url": "https://careers.bbwinc.com",                    "domain": "bathandbodyworks.com",   "keywords": ["bath & body", "bath and body", "bbw"]},
    "sephora":         {"name": "Sephora",              "url": "https://www.sephora.jobs",                      "domain": "sephora.com",            "keywords": ["sephora"]},
    "ulta":            {"name": "Ulta Beauty",          "url": "https://careers.ulta.com",                      "domain": "ulta.com",               "keywords": ["ulta"]},
    "dicks":           {"name": "Dick's Sporting Goods","url": "https://careers.dicks.com",                     "domain": "dickssportinggoods.com", "keywords": ["dick's sporting", "dicks sporting"]},
    "petco":           {"name": "Petco",                "url": "https://careers.petco.com",                     "domain": "petco.com",              "keywords": ["petco"]},
    "petsmart":        {"name": "PetSmart",             "url": "https://careers.petsmart.com",                  "domain": "petsmart.com",           "keywords": ["petsmart"]},
    "samsclub":        {"name": "Sam's Club",           "url": "https://careers.samsclub.com",                  "domain": "samsclub.com",           "keywords": ["sam's club", "sams club"]},
    "aldi":            {"name": "Aldi",                 "url": "https://careers.aldi.us",                       "domain": "aldi.us",                "keywords": ["aldi"]},
    "trader_costco_combined_filler_50":  # placeholder filler removed below
                       {"name": "_filler",              "url": "",                                              "domain": "",                       "keywords": []},
}
# Trim placeholder so the dict has exactly 50 real entries.
CAREERS_DIRECTORY.pop("trader_costco_combined_filler_50", None)


# Phase 1B (v0.1.21+): sector tags for every directory entry. Stored in a
# sidecar dict so we don't have to rewrite the entire CAREERS_DIRECTORY block.
# Each value is a list of sector slugs from the canonical 21-sector taxonomy.
# Multi-tag where applicable -- e.g., Amazon spans retail + IT + logistics,
# CVS spans retail + healthcare, McDonald's spans hospitality + retail.
_DIRECTORY_SECTORS = {
    # ── Big-box & general retail ────────────────────────────────────────
    "walmart":       ["retail"],
    "target":        ["retail"],
    "costco":        ["retail"],
    "samsclub":      ["retail"],
    "kroger":        ["retail"],
    "albertsons":    ["retail"],
    "publix":        ["retail"],
    "traderjoes":    ["retail"],
    "wholefoods":    ["retail"],
    "aldi":          ["retail"],
    "ross":          ["retail"],
    "tjx":           ["retail"],
    "macys":         ["retail"],
    "nordstrom":     ["retail"],
    "gap":           ["retail"],
    "bathandbody":   ["retail"],
    "sephora":       ["retail"],
    "ulta":          ["retail"],
    "dicks":         ["retail"],
    "petco":         ["retail"],
    "petsmart":      ["retail"],
    "dollargeneral": ["retail"],
    "dollartree":    ["retail"],
    "bestbuy":       ["retail", "it"],
    "homedepot":     ["retail", "construction"],
    "lowes":         ["retail", "construction"],
    "cvs":           ["retail", "healthcare"],
    "walgreens":     ["retail", "healthcare"],

    # ── QSR / restaurants (hospitality + retail) ─────────────────────────
    "mcdonalds":     ["hospitality", "retail"],
    "starbucks":     ["hospitality", "retail"],
    "subway":        ["hospitality", "retail"],
    "burgerking":    ["hospitality", "retail"],
    "wendys":        ["hospitality", "retail"],
    "tacobell":      ["hospitality", "retail"],
    "kfc":           ["hospitality", "retail"],
    "dominos":       ["hospitality", "retail"],
    "pizzahut":      ["hospitality", "retail"],
    "chickfila":     ["hospitality", "retail"],
    "chipotle":      ["hospitality", "retail"],
    "darden":        ["hospitality"],

    # ── Hotels & resorts ────────────────────────────────────────────────
    "marriott":      ["hospitality"],
    "hilton":        ["hospitality"],
    "hyatt":         ["hospitality"],

    # ── Logistics / transport ───────────────────────────────────────────
    "fedex":         ["transport"],
    "ups":           ["transport"],

    # ── Mixed / multi-vertical (e-com, telecom, media) ──────────────────
    "amazon":        ["retail", "it", "transport"],
    "att":           ["it", "customer_service", "sales"],
    "verizon":       ["it", "customer_service", "sales"],
    "tmobile":       ["it", "customer_service", "sales"],
    "disney":        ["hospitality", "creative", "marketing"],
}


# Phase 1C (v0.1.21+): sector-balanced expansion -- ~72 additions covering
# Finance / IT / Healthcare / Manufacturing+Defense / Energy / Media+Telecom.
# These join the existing 50 to give the Browse page real cross-sector depth.
CAREERS_DIRECTORY.update({
    # ── Finance (banks, payment networks, asset managers, brokerages) ───
    "jpmorgan":      {"name": "JPMorgan Chase",     "url": "https://careers.jpmorgan.com",                            "domain": "jpmorganchase.com",      "keywords": ["jpmorgan", "jp morgan", "chase bank", "chase"],          "sectors": ["finance"]},
    "bofa":          {"name": "Bank of America",    "url": "https://careers.bankofamerica.com",                       "domain": "bankofamerica.com",      "keywords": ["bank of america", "bofa"],                                "sectors": ["finance"]},
    "wellsfargo":    {"name": "Wells Fargo",        "url": "https://www.wellsfargojobs.com",                          "domain": "wellsfargo.com",         "keywords": ["wells fargo"],                                            "sectors": ["finance"]},
    "goldman":       {"name": "Goldman Sachs",      "url": "https://www.goldmansachs.com/careers",                    "domain": "goldmansachs.com",       "keywords": ["goldman sachs", "goldman"],                               "sectors": ["finance"]},
    "morganstanley": {"name": "Morgan Stanley",     "url": "https://www.morganstanley.com/people-opportunities/careers", "domain": "morganstanley.com",   "keywords": ["morgan stanley"],                                         "sectors": ["finance"]},
    "citi":          {"name": "Citigroup",          "url": "https://jobs.citi.com",                                   "domain": "citigroup.com",          "keywords": ["citi", "citigroup", "citibank"],                          "sectors": ["finance"]},
    "capitalone":    {"name": "Capital One",        "url": "https://www.capitalonecareers.com",                       "domain": "capitalone.com",         "keywords": ["capital one"],                                            "sectors": ["finance"]},
    "amex":          {"name": "American Express",   "url": "https://jobs.americanexpress.com",                        "domain": "americanexpress.com",    "keywords": ["american express", "amex"],                               "sectors": ["finance"]},
    "schwab":        {"name": "Charles Schwab",     "url": "https://jobs.schwab.com",                                 "domain": "schwab.com",             "keywords": ["charles schwab", "schwab"],                               "sectors": ["finance"]},
    "fidelity":      {"name": "Fidelity Investments","url": "https://jobs.fidelity.com",                              "domain": "fidelity.com",           "keywords": ["fidelity"],                                               "sectors": ["finance"]},
    "visa":          {"name": "Visa",               "url": "https://corporate.visa.com/en/careers.html",              "domain": "visa.com",               "keywords": ["visa inc", "visa"],                                       "sectors": ["finance"]},
    "mastercard":    {"name": "Mastercard",         "url": "https://careers.mastercard.com",                          "domain": "mastercard.com",         "keywords": ["mastercard"],                                             "sectors": ["finance"]},
    "blackrock":     {"name": "BlackRock",          "url": "https://careers.blackrock.com",                           "domain": "blackrock.com",          "keywords": ["blackrock"],                                              "sectors": ["finance"]},
    "vanguard":      {"name": "Vanguard",           "url": "https://www.vanguardjobs.com",                            "domain": "vanguard.com",           "keywords": ["vanguard"],                                               "sectors": ["finance"]},
    "statestreet":   {"name": "State Street",       "url": "https://www.statestreet.com/us/en/asset-owner/careers",   "domain": "statestreet.com",        "keywords": ["state street"],                                           "sectors": ["finance"]},
    "pnc":           {"name": "PNC Bank",           "url": "https://www.pnc.jobs",                                    "domain": "pnc.com",                "keywords": ["pnc"],                                                    "sectors": ["finance"]},
    "usbank":        {"name": "U.S. Bank",          "url": "https://careers.usbank.com",                              "domain": "usbank.com",             "keywords": ["us bank", "u.s. bank", "usbank"],                         "sectors": ["finance"]},
    "tdbank":        {"name": "TD Bank",            "url": "https://jobs.td.com",                                     "domain": "td.com",                 "keywords": ["td bank"],                                                "sectors": ["finance"]},

    # ── IT / software / cloud / hardware ────────────────────────────────
    "microsoft":     {"name": "Microsoft",          "url": "https://careers.microsoft.com",                           "domain": "microsoft.com",          "keywords": ["microsoft"],                                              "sectors": ["it"]},
    "apple":         {"name": "Apple",              "url": "https://www.apple.com/careers",                           "domain": "apple.com",              "keywords": ["apple inc", "apple"],                                     "sectors": ["it"]},
    "google":        {"name": "Google / Alphabet",  "url": "https://careers.google.com",                              "domain": "google.com",             "keywords": ["google", "alphabet"],                                     "sectors": ["it"]},
    "meta":          {"name": "Meta",               "url": "https://www.metacareers.com",                             "domain": "meta.com",               "keywords": ["meta", "facebook", "instagram"],                          "sectors": ["it"]},
    "oracle":        {"name": "Oracle",             "url": "https://careers.oracle.com",                              "domain": "oracle.com",             "keywords": ["oracle"],                                                 "sectors": ["it"]},
    "salesforce":    {"name": "Salesforce",         "url": "https://www.salesforce.com/company/careers",              "domain": "salesforce.com",         "keywords": ["salesforce"],                                             "sectors": ["it"]},
    "ibm":           {"name": "IBM",                "url": "https://www.ibm.com/careers",                             "domain": "ibm.com",                "keywords": ["ibm"],                                                    "sectors": ["it"]},
    "cisco":         {"name": "Cisco",              "url": "https://jobs.cisco.com",                                  "domain": "cisco.com",              "keywords": ["cisco"],                                                  "sectors": ["it"]},
    "adobe":         {"name": "Adobe",              "url": "https://careers.adobe.com",                               "domain": "adobe.com",              "keywords": ["adobe"],                                                  "sectors": ["it", "creative"]},
    "intel":         {"name": "Intel",              "url": "https://jobs.intel.com",                                  "domain": "intel.com",              "keywords": ["intel"],                                                  "sectors": ["it", "manufacturing"]},
    "nvidia":        {"name": "NVIDIA",             "url": "https://www.nvidia.com/en-us/about-nvidia/careers",       "domain": "nvidia.com",             "keywords": ["nvidia"],                                                 "sectors": ["it"]},
    "servicenow":    {"name": "ServiceNow",         "url": "https://careers.servicenow.com",                          "domain": "servicenow.com",         "keywords": ["servicenow", "service now"],                              "sectors": ["it"]},
    "workday":       {"name": "Workday",            "url": "https://www.workday.com/en-us/company/careers.html",      "domain": "workday.com",            "keywords": ["workday"],                                                "sectors": ["it"]},
    "snowflake":     {"name": "Snowflake",          "url": "https://careers.snowflake.com",                           "domain": "snowflake.com",          "keywords": ["snowflake"],                                              "sectors": ["it"]},
    "netflix":       {"name": "Netflix",            "url": "https://jobs.netflix.com",                                "domain": "netflix.com",            "keywords": ["netflix"],                                                "sectors": ["it", "creative"]},

    # ── Healthcare (payers, providers, pharma, biotech) ─────────────────
    "unitedhealth":  {"name": "UnitedHealth Group", "url": "https://careers.unitedhealthgroup.com",                   "domain": "unitedhealthgroup.com",  "keywords": ["unitedhealth", "united health"],                          "sectors": ["healthcare"]},
    "elevance":      {"name": "Elevance Health",    "url": "https://careers.elevancehealth.com",                      "domain": "elevancehealth.com",     "keywords": ["elevance", "anthem"],                                     "sectors": ["healthcare"]},
    "humana":        {"name": "Humana",             "url": "https://careers.humana.com",                              "domain": "humana.com",             "keywords": ["humana"],                                                 "sectors": ["healthcare"]},
    "hca":           {"name": "HCA Healthcare",     "url": "https://careers.hcahealthcare.com",                       "domain": "hcahealthcare.com",      "keywords": ["hca"],                                                    "sectors": ["healthcare"]},
    "kaiser":        {"name": "Kaiser Permanente",  "url": "https://www.kaiserpermanentejobs.org",                    "domain": "kaiserpermanente.org",   "keywords": ["kaiser permanente", "kaiser"],                            "sectors": ["healthcare"]},
    "cigna":         {"name": "Cigna",              "url": "https://jobs.thecignagroup.com",                          "domain": "cigna.com",              "keywords": ["cigna"],                                                  "sectors": ["healthcare"]},
    "pfizer":        {"name": "Pfizer",             "url": "https://www.pfizer.com/about/careers",                    "domain": "pfizer.com",             "keywords": ["pfizer"],                                                 "sectors": ["healthcare", "science"]},
    "jnj":           {"name": "Johnson & Johnson",  "url": "https://www.careers.jnj.com",                             "domain": "jnj.com",                "keywords": ["johnson & johnson", "johnson and johnson", "j&j", "jnj"], "sectors": ["healthcare", "science"]},
    "merck":         {"name": "Merck",              "url": "https://jobs.merck.com",                                  "domain": "merck.com",              "keywords": ["merck"],                                                  "sectors": ["healthcare", "science"]},
    "lilly":         {"name": "Eli Lilly",          "url": "https://careers.lilly.com",                               "domain": "lilly.com",              "keywords": ["eli lilly", "lilly"],                                     "sectors": ["healthcare", "science"]},
    "bms":           {"name": "Bristol Myers Squibb","url": "https://careers.bms.com",                                "domain": "bms.com",                "keywords": ["bristol myers", "bms"],                                   "sectors": ["healthcare", "science"]},
    "abbvie":        {"name": "AbbVie",             "url": "https://careers.abbvie.com",                              "domain": "abbvie.com",             "keywords": ["abbvie"],                                                 "sectors": ["healthcare", "science"]},
    "astrazeneca":   {"name": "AstraZeneca",        "url": "https://careers.astrazeneca.com",                         "domain": "astrazeneca.com",        "keywords": ["astrazeneca"],                                            "sectors": ["healthcare", "science"]},
    "moderna":       {"name": "Moderna",            "url": "https://www.modernatx.com/careers",                       "domain": "modernatx.com",          "keywords": ["moderna"],                                                "sectors": ["healthcare", "science"]},
    "clevelandclinic":{"name": "Cleveland Clinic",  "url": "https://jobs.clevelandclinic.org",                        "domain": "clevelandclinic.org",    "keywords": ["cleveland clinic"],                                       "sectors": ["healthcare"]},

    # ── Manufacturing / Defense / Aerospace / Automotive ────────────────
    "boeing":        {"name": "Boeing",             "url": "https://jobs.boeing.com",                                 "domain": "boeing.com",             "keywords": ["boeing"],                                                 "sectors": ["manufacturing", "engineering"]},
    "lockheed":      {"name": "Lockheed Martin",    "url": "https://www.lockheedmartinjobs.com",                      "domain": "lockheedmartin.com",     "keywords": ["lockheed", "lockheed martin"],                            "sectors": ["manufacturing", "engineering", "government"]},
    "rtx":           {"name": "RTX (Raytheon)",     "url": "https://careers.rtx.com",                                 "domain": "rtx.com",                "keywords": ["rtx", "raytheon"],                                        "sectors": ["manufacturing", "engineering", "government"]},
    "ge":            {"name": "General Electric",   "url": "https://jobs.gecareers.com",                              "domain": "ge.com",                 "keywords": ["ge", "general electric"],                                 "sectors": ["manufacturing", "engineering"]},
    "caterpillar":   {"name": "Caterpillar",        "url": "https://careers.caterpillar.com",                         "domain": "caterpillar.com",        "keywords": ["caterpillar", "cat inc"],                                 "sectors": ["manufacturing", "construction"]},
    "ford":          {"name": "Ford Motor Company", "url": "https://corporate.ford.com/careers",                      "domain": "ford.com",               "keywords": ["ford"],                                                   "sectors": ["manufacturing", "engineering"]},
    "gm":            {"name": "General Motors",     "url": "https://search-careers.gm.com",                           "domain": "gm.com",                 "keywords": ["general motors", "gm"],                                   "sectors": ["manufacturing", "engineering"]},
    "tesla":         {"name": "Tesla",              "url": "https://www.tesla.com/careers",                           "domain": "tesla.com",              "keywords": ["tesla"],                                                  "sectors": ["manufacturing", "engineering", "it"]},
    "deere":         {"name": "John Deere",         "url": "https://careers.deere.com",                               "domain": "deere.com",              "keywords": ["john deere", "deere"],                                    "sectors": ["manufacturing", "construction"]},
    "mmm":           {"name": "3M",                 "url": "https://www.3m.com/3M/en_US/careers-us",                  "domain": "3m.com",                 "keywords": ["3m"],                                                     "sectors": ["manufacturing"]},
    "honeywell":     {"name": "Honeywell",          "url": "https://careers.honeywell.com",                           "domain": "honeywell.com",          "keywords": ["honeywell"],                                              "sectors": ["manufacturing", "engineering"]},
    "northrop":      {"name": "Northrop Grumman",   "url": "https://www.northropgrumman.com/careers",                 "domain": "northropgrumman.com",    "keywords": ["northrop", "northrop grumman"],                           "sectors": ["manufacturing", "engineering", "government"]},
    "gd":            {"name": "General Dynamics",   "url": "https://www.gd.com/careers",                              "domain": "gd.com",                 "keywords": ["general dynamics"],                                       "sectors": ["manufacturing", "engineering", "government"]},
    "boozallen":     {"name": "Booz Allen Hamilton","url": "https://careers.boozallen.com",                           "domain": "boozallen.com",          "keywords": ["booz allen", "booz allen hamilton"],                      "sectors": ["it", "government", "engineering"]},

    # ── Energy / Oil & Gas / Utilities ──────────────────────────────────
    "exxon":         {"name": "ExxonMobil",         "url": "https://jobs.exxonmobil.com",                             "domain": "exxonmobil.com",         "keywords": ["exxon", "exxonmobil"],                                    "sectors": ["engineering", "manufacturing"]},
    "chevron":       {"name": "Chevron",            "url": "https://careers.chevron.com",                             "domain": "chevron.com",            "keywords": ["chevron"],                                                "sectors": ["engineering", "manufacturing"]},
    "conoco":        {"name": "ConocoPhillips",     "url": "https://www.conocophillips.com/careers",                  "domain": "conocophillips.com",     "keywords": ["conocophillips", "conoco"],                               "sectors": ["engineering", "manufacturing"]},
    "slb":           {"name": "SLB (Schlumberger)", "url": "https://careers.slb.com",                                 "domain": "slb.com",                "keywords": ["schlumberger", "slb"],                                    "sectors": ["engineering"]},
    "nextera":       {"name": "NextEra Energy",     "url": "https://www.nexteraenergy.com/careers.html",              "domain": "nexteraenergy.com",      "keywords": ["nextera"],                                                "sectors": ["engineering"]},

    # ── Media / Streaming / Mobility ────────────────────────────────────
    "comcast":       {"name": "Comcast",            "url": "https://jobs.comcast.com",                                "domain": "comcast.com",            "keywords": ["comcast", "xfinity"],                                     "sectors": ["it", "customer_service", "creative"]},
    "spotify":       {"name": "Spotify",            "url": "https://www.lifeatspotify.com",                           "domain": "spotify.com",            "keywords": ["spotify"],                                                "sectors": ["it", "creative"]},
    "uber":          {"name": "Uber",               "url": "https://www.uber.com/us/en/careers",                      "domain": "uber.com",               "keywords": ["uber"],                                                   "sectors": ["it", "transport"]},
    "lyft":          {"name": "Lyft",               "url": "https://www.lyft.com/careers",                            "domain": "lyft.com",               "keywords": ["lyft"],                                                   "sectors": ["it", "transport"]},
    "airbnb":        {"name": "Airbnb",             "url": "https://careers.airbnb.com",                              "domain": "airbnb.com",             "keywords": ["airbnb"],                                                 "sectors": ["it", "hospitality"]},
})


# Merge sector tags into the original 50 entries (Phase 1C additions already
# include `sectors` inline).
for _k, _sect in _DIRECTORY_SECTORS.items():
    if _k in CAREERS_DIRECTORY:
        CAREERS_DIRECTORY[_k]["sectors"] = _sect


def _careers_directory_lookup(company: str) -> Optional[dict]:
    """Fuzzy-match a user-typed company string against CAREERS_DIRECTORY.
    Strips common legal suffixes first, then tries (a) exact key match,
    (b) keyword substring match. Returns the directory entry dict or None."""
    if not company:
        return None
    s = company.lower().strip()
    s = re.sub(r"\s+(inc\.?|corp\.?|llc|ltd\.?|llp|holdings?|group|co\.?|company)$",
               "", s).strip()
    if not s:
        return None
    # Exact key match
    key = re.sub(r"[^a-z0-9]", "", s)  # normalize key form
    if key in CAREERS_DIRECTORY:
        return dict(CAREERS_DIRECTORY[key], _key=key)
    # Keyword substring match
    for k, entry in CAREERS_DIRECTORY.items():
        for kw in entry.get("keywords", []):
            if kw and kw in s:
                return dict(entry, _key=k)
    return None


@app.get("/pv/careers/directory")
async def get_careers_directory():
    """Return the full curated CAREERS_DIRECTORY as a list, alphabetically by
    company name. Each entry has name, domain, url, logo_url, sectors. Used
    by the Top Employers browse page; no auth required (public discovery)."""
    rows = []
    for entry in CAREERS_DIRECTORY.values():
        rows.append({
            "name": entry["name"],
            "url": entry["url"],
            "domain": entry["domain"],
            "sectors": entry.get("sectors", []),
            "logo_url": ("https://www.google.com/s2/favicons?domain="
                         + entry["domain"] + "&sz=128"),
        })
    rows.sort(key=lambda r: r["name"].lower())
    return {"directory": rows, "count": len(rows)}


@app.get("/pv/careers")
async def get_careers_link(company: str):
    """Return a direct careers URL + logo for the given company. Uses the
    curated CAREERS_DIRECTORY first; falls back to a Google search URL when
    the company isn't in the directory. logo_url uses Google's free favicon
    service so it always renders without API keys."""
    company = (company or "").strip()
    if not company:
        raise HTTPException(status_code=400, detail="company query param required")

    entry = _careers_directory_lookup(company)
    if entry:
        return {
            "found": True,
            "name": entry["name"],
            "url": entry["url"],
            "domain": entry["domain"],
            "logo_url": ("https://www.google.com/s2/favicons?domain="
                         + entry["domain"] + "&sz=128"),
            "source": "directory",
        }

    # Fallback: Google search URL with "careers" suffix. Always shows the user
    # something useful even for employers we haven't curated.
    google_url = ("https://www.google.com/search?q="
                  + urllib.parse.quote(company.strip() + " careers"))
    return {
        "found": False,
        "name": company.title(),
        "url": google_url,
        "domain": None,
        "logo_url": None,
        "source": "google_fallback",
    }


@app.get("/pv/searches", response_model=List[SearchResponse])
async def list_searches(user_id: int = Depends(get_current_user)):
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, company, sector, level, location_name, lat, lng,
                   radius_value, where_mode, state_code,
                   alert_level, is_archived, in_my_searches, created_at
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
                "where_mode": row[9] or "address",
                "state_code": row[10],
                "alert_level": row[11],
                "is_archived": bool(row[12]),
                "in_my_searches": bool(row[13]),
                "created_at": str(row[14]),
            })
        return items


@app.post("/pv/searches")
async def add_search(item: SearchItem, user_id: int = Depends(get_current_user)):
    # Backward-compat: frontend v0.1.9 still sends radius_value='remote' to mean
    # "anywhere". Translate that into the new where_mode model before validating.
    where_mode = (item.where_mode or "").strip().lower()
    state_code = (item.state_code or "").strip().upper() or None
    radius_value = item.radius_value
    lat = item.lat
    lng = item.lng
    location_name = item.location_name

    if not where_mode:
        # Derive from radius_value if frontend hasn't supplied where_mode yet.
        if radius_value == "remote":
            where_mode = "anywhere"
            radius_value = "25"  # sentinel; ignored by drawer SQL in anywhere mode
            lat = None
            lng = None
            location_name = None
        else:
            where_mode = "address"

    if where_mode not in ("address", "state", "anywhere"):
        raise HTTPException(status_code=400, detail="where_mode must be address/state/anywhere")

    # Validate radius_value (only meaningful in address mode but always populated).
    if radius_value not in ("1", "5", "10", "25"):
        if where_mode == "address":
            raise HTTPException(status_code=400, detail="radius_value must be 1/5/10/25")
        radius_value = "25"  # harmless default for state/anywhere modes

    # Per-mode validation.
    if where_mode == "address":
        if lat is None or lng is None:
            raise HTTPException(status_code=400, detail="lat/lng required when where_mode='address'")
        if lat < -90 or lat > 90:
            raise HTTPException(status_code=400, detail="lat must be between -90 and 90")
        if lng < -180 or lng > 180:
            raise HTTPException(status_code=400, detail="lng must be between -180 and 180")
        state_code = None  # not used in address mode
    elif where_mode == "state":
        if not state_code or state_code not in _US_STATE_CODES:
            raise HTTPException(status_code=400, detail="state_code (2-letter US) required when where_mode='state'")
        lat = None
        lng = None
        location_name = None
    else:  # anywhere
        state_code = None
        lat = None
        lng = None
        location_name = None

    if item.level and item.level not in ("entry", "mid", "senior", "executive"):
        raise HTTPException(status_code=400, detail="level must be entry/mid/senior/executive")
    alert_level = item.alert_level or "realtime"
    if alert_level not in ("off", "digest", "realtime"):
        alert_level = "realtime"
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO pv_searches (user_id, name, company, sector, level,
                                     location_name, lat, lng, radius_value,
                                     where_mode, state_code, alert_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, (user_id, item.name, item.company, item.sector, item.level,
              location_name, lat, lng, radius_value,
              where_mode, state_code, alert_level))
        row = c.fetchone()
        conn.commit()
        # v: on-demand Adzuna seed. Pull private-sector jobs for THIS search's
        # location right now (background thread, so the save returns instantly),
        # instead of waiting for the 12h cron's popularity-based sampling. This
        # is what gives a brand-new "near a place" search real coverage.
        if where_mode == "address" and location_name:
            threading.Thread(target=_seed_adzuna_for_search,
                             args=(location_name,), daemon=True).start()
        return {
            "message": "Search added",
            "id": row[0],
            "name": item.name,
            "company": item.company,
            "sector": item.sector,
            "level": item.level,
            "location_name": location_name,
            "lat": lat,
            "lng": lng,
            "radius_value": radius_value,
            "where_mode": where_mode,
            "state_code": state_code,
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
            # Backward-compat: PATCH radius_value='remote' from v0.1.9 frontend
            # → translate to where_mode='anywhere' + sensible radius_value default.
            if update.radius_value == "remote":
                sets.append("where_mode = %s"); params.append("anywhere")
                sets.append("radius_value = %s"); params.append("25")
                sets.append("lat = %s"); params.append(None)
                sets.append("lng = %s"); params.append(None)
                sets.append("location_name = %s"); params.append(None)
                sets.append("state_code = %s"); params.append(None)
            elif update.radius_value in ("1", "5", "10", "25"):
                sets.append("radius_value = %s"); params.append(update.radius_value)
                # If frontend doesn't also send where_mode, treat as address mode.
                if update.where_mode is None:
                    sets.append("where_mode = %s"); params.append("address")
                    sets.append("state_code = %s"); params.append(None)
            else:
                raise HTTPException(status_code=400, detail="radius_value must be 1/5/10/25")
        if update.where_mode is not None:
            wm = update.where_mode.strip().lower()
            if wm not in ("address", "state", "anywhere"):
                raise HTTPException(status_code=400, detail="where_mode must be address/state/anywhere")
            sets.append("where_mode = %s"); params.append(wm)
            if wm != "address":
                # Clear lat/lng when leaving address mode.
                sets.append("lat = %s"); params.append(None)
                sets.append("lng = %s"); params.append(None)
                sets.append("location_name = %s"); params.append(None)
            if wm != "state":
                sets.append("state_code = %s"); params.append(None)
        if update.state_code is not None:
            sc = update.state_code.strip().upper() or None
            if sc and sc not in _US_STATE_CODES:
                raise HTTPException(status_code=400, detail="state_code must be a valid 2-letter US state")
            sets.append("state_code = %s"); params.append(sc)
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
                            radius_value, is_archived, in_my_searches,
                            where_mode, state_code
                     FROM pv_searches WHERE id = %s AND user_id = %s""",
                  (search_id, user_id))
        search = c.fetchone()
        if not search:
            raise HTTPException(status_code=404, detail="Search not found")

        s_lat = float(search[6]) if search[6] is not None else None
        s_lng = float(search[7]) if search[7] is not None else None
        s_radius = search[8]
        s_where_mode = search[11] or "address"
        s_state_code = search[12]

        # Filter-match query. Each filter is "NULL = any"; location branches on
        # radius_value. DISTINCT ON collapses identical postings (USAJobs often
        # publishes the same role at multiple grades or duty-station variants
        # under separate MatchedObjectIds -- to the user they're the same job).
        # The dedupe key is (lower(title), lower(company), location_name);
        # within a dedupe group we keep the most-recently-posted row.
        c.execute("""
            SELECT * FROM (
              SELECT DISTINCT ON (LOWER(j.title), LOWER(COALESCE(j.company,'')), COALESCE(j.location_name,''))
                     j.id, j.source, j.external_id, j.title, j.company, j.sector, j.level,
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
                  -- where_mode='address': within radius miles of saved point,
                  -- OR job is remote-classified (location-agnostic).
                  (s.where_mode = 'address' AND (
                    j.is_remote = TRUE
                    OR (s.geom IS NOT NULL AND j.geom IS NOT NULL
                        AND ST_DWithin(j.geom, s.geom,
                                       COALESCE(NULLIF(s.radius_value, 'remote'), '25')::int * 1609.344))
                  ))
                  OR
                  -- where_mode='state': job's state matches search's state,
                  -- OR job is remote-classified.
                  (s.where_mode = 'state' AND (
                    j.is_remote = TRUE
                    OR j.state = s.state_code
                  ))
                  OR
                  -- where_mode='anywhere': no geographic predicate at all.
                  (s.where_mode = 'anywhere')
                )
                AND j.fetched_at > NOW() - INTERVAL '3 days'
              ORDER BY LOWER(j.title), LOWER(COALESCE(j.company,'')), COALESCE(j.location_name,''), j.posted_at DESC
            ) dedup
            ORDER BY posted_at DESC NULLS LAST
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

        # Render the static map only for address-mode searches. State and
        # anywhere modes have no center+radius geographic anchor.
        static_map_url = None
        if s_where_mode == "address" and s_lat is not None and s_lng is not None:
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
                "where_mode": s_where_mode,
                "state_code": s_state_code,
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
        if hazard_type == "job":
            # pivot.watch: classic-shape red Google marker (default size). Distinct
            # teardrop pin shape stands out against label-heavy city maps where
            # small circle markers get lost among POI labels. No external CDN.
            markers.append("color:red|"
                           + str(round(elat, 5)) + "," + str(round(elng, 5)))
            continue
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
# v: public route — anon users must be able to geocode/preview a place before
# signing up. Save is still the auth trigger. user_id was unused in the body.
async def geocode_place(q: GeocodeQuery):
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
    """OPM occupational series → one of our 21 sector slugs. USAJobs is all
    federal, so most uncategorized series default to 'government'. Recognized
    series are routed to the more specific sector that matches private-board
    categorization (so a federal accountant and a private accountant land in
    the same 'finance' bucket)."""
    if not code:
        return "government"
    c = str(code).zfill(4)
    # 22XX = IT family (Information Technology Management etc.)
    if c.startswith("22"):
        return "it"
    # 0810 (Civil Engineering) + 0828 (Construction Analyst) → construction
    if c in ("0810", "0828"):
        return "construction"
    # 08XX rest = Engineering family
    if c.startswith("08"):
        return "engineering"
    # Legal: 0901 (General Legal), 0904 (Law Clerk), 0905 (General Attorney)
    if c in ("0901", "0904", "0905"):
        return "legal"
    # Healthcare: Medical Officer, Nurse, Pharmacist, Dental, Health Tech families
    if c in ("0602", "0610", "0660", "0680", "0640", "0644", "0671", "0699",
             "0601", "0603", "0620", "0633", "0645", "0647", "0648", "0649"):
        return "healthcare"
    # 05XX = Accounting and Budget family
    if c.startswith("05"):
        return "finance"
    # 17XX = Education family (teachers, education specialists)
    if c.startswith("17"):
        return "education"
    # 02XX = Human Resources family
    if c.startswith("02"):
        return "hr"
    # 1083 = Public Affairs (closest federal match for Marketing/PR)
    if c == "1083":
        return "marketing"
    # 1170 = Realty
    if c == "1170":
        return "real_estate"
    # 03XX = Administration family (Misc Admin, Clerical, Mgmt Analyst etc.)
    if c.startswith("03"):
        return "administration"
    # Science & Research: 13XX physical sciences, 04XX biological sciences,
    # 14XX library/info science, 1500 mathematics
    if c.startswith("13") or c.startswith("04") or c.startswith("14") or c == "1500":
        return "science"
    # Social Services: 0185 (Social Work), 0186 (Social Services Aide),
    # 0187 (Social Services), 0188 (Recreation Specialist), 0189 (Recreation Aid)
    if c.startswith("018"):
        return "social_services"
    # Transport & Logistics: 20XX (Supply), 21XX (Transportation)
    if c.startswith("21") or c.startswith("20"):
        return "transport"
    # Customer Service: 0962 (Contact Representative)
    if c == "0962":
        return "customer_service"
    # Creative & Design: 1001 (General Arts/Info), 1015 (Museum Curator),
    # 1020 (Illustrator), 1060 (Photography), 1071 (Audiovisual Production)
    if c in ("1001", "1015", "1020", "1060", "1071", "1082", "1084"):
        return "creative"
    # Sales/marketing-adjacent: 1101 (General Business), 1102 (Contracting)
    if c in ("1102", "1101"):
        return "sales"
    # Manufacturing/trades: 1601 (Equipment Facilities Services), 4XXX (Wage Grade trades)
    if c.startswith("4") or c == "1601":
        return "manufacturing"
    # Hospitality: 1667 (Steward), 7404 (Cooking) — rare in federal
    if c in ("1667", "7404"):
        return "hospitality"
    # Retail: 1144 (Commissary Mgmt), 2091 (Sales Store Clerical) — rare in federal
    if c in ("1144", "2091"):
        return "retail"
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

    # State extraction: USAJobs exposes CountrySubDivisionCode (full state name
    # like "Hawaii") on the first PositionLocation. Fall back to parsing the
    # composed location_name string if that field is missing.
    state = None
    csdc = (loc.get("CountrySubDivisionCode") or "").strip().lower()
    if csdc:
        state = US_STATE_NAME_TO_CODE.get(csdc)
    if not state:
        state = _extract_state_code(location_name)

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
        "state": state,
        "is_remote": is_remote,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": "USD",
        "description": description,
        "url": url,
        "posted_at": posted_at,
        "raw": desc,
    }


# ── Adzuna normalization helpers ──────────────────────────────────────────────
# Maps a single Adzuna /search result into the pv_jobs dict shape. Adzuna's
# 28 category tags map cleanly to most of our 21 sector slugs. Level isn't
# explicit in Adzuna -- we keyword-scan the title.

def _adzuna_sector_for_tag(tag) -> str:
    """Adzuna category tag → one of our 21 sector slugs. Unrecognized tags
    default to 'administration' as a catch-all for general/clerical roles."""
    if not tag:
        return "administration"
    t = str(tag).lower().strip()
    mapping = {
        "it-jobs":                       "it",
        "engineering-jobs":              "engineering",
        "legal-jobs":                    "legal",
        "healthcare-nursing-jobs":       "healthcare",
        "accounting-finance-jobs":       "finance",
        "sales-jobs":                    "sales",
        "pr-advertising-marketing-jobs": "marketing",
        "hr-jobs":                       "hr",
        "teaching-jobs":                 "education",
        "trade-construction-jobs":       "construction",
        "admin-jobs":                    "administration",
        "creative-design-jobs":          "creative",
        "customer-services-jobs":        "customer_service",
        "hospitality-catering-jobs":     "hospitality",
        "manufacturing-jobs":            "manufacturing",
        "property-jobs":                 "real_estate",
        "retail-jobs":                   "retail",
        "scientific-qa-jobs":            "science",
        "social-work-jobs":              "social_services",
        "logistics-warehouse-jobs":      "transport",
        "travel-jobs":                   "hospitality",
        "energy-oil-gas-jobs":           "engineering",
        "charity-voluntary-jobs":        "social_services",
        "consultancy-jobs":              "administration",
        "domestic-help-cleaning-jobs":   "customer_service",
        "graduate-jobs":                 "administration",
        "part-time-jobs":                "administration",
        "other-general-jobs":            "administration",
        "unknown":                       "administration",
    }
    return mapping.get(t, "administration")


def _level_from_title_keywords(title: str) -> str:
    """Keyword-based level guesser. Shared between Adzuna and any future
    private-board sources that lack a structured grade/level field."""
    if not title:
        return "mid"
    t = title.upper()
    if any(w in t for w in ["INTERN", "TRAINEE", "STUDENT", "JUNIOR",
                            "ENTRY LEVEL", "ENTRY-LEVEL", "GRADUATE", " JR "]):
        return "entry"
    if any(w in t for w in ["DIRECTOR", "VP", "VICE PRESIDENT", "CHIEF ",
                            "HEAD OF", "EXECUTIVE", "PRESIDENT", " CEO",
                            " CTO", " CFO", " COO"]):
        return "executive"
    if any(w in t for w in ["SENIOR", "PRINCIPAL", "LEAD ", " SR ", " SR.",
                            "STAFF "]):
        return "senior"
    return "mid"


# ── State extraction (v0.1.10) ────────────────────────────────────────────────
# Map of full US state names (lowercase) → 2-letter code. Used by all source
# normalizers to populate pv_jobs.state at ingest, which in turn powers the
# 'state' where_mode in pv_searches filtering.
US_STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN",
    "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "puerto rico": "PR", "guam": "GU", "u.s. virgin islands": "VI",
    "american samoa": "AS", "northern mariana islands": "MP",
}
_US_STATE_CODES = set(US_STATE_NAME_TO_CODE.values())
# Reverse map: 2-letter code → proper-case state name. Used to convert
# state-mode searches into natural-language strings for Adzuna's where= param.
US_STATE_CODE_TO_NAME = {v: k.title() for k, v in US_STATE_NAME_TO_CODE.items()}


def _extract_state_code(text) -> Optional[str]:
    """Extract a 2-letter US state code from a free-text location string.
    Tries (in order): explicit 2-letter code at end after comma; full state
    name match. Returns None if nothing recognized — caller is responsible
    for leaving pv_jobs.state NULL in that case."""
    if not text:
        return None
    s = str(text).strip()
    if not s:
        return None
    # Pattern 1: ", XX" or ", XX " at the end / before a space — handles
    # "Honolulu, HI", "New York, NY 10001", "Washington, DC - Remote", etc.
    m = re.search(r",\s*([A-Z]{2})\b", s)
    if m and m.group(1) in _US_STATE_CODES:
        return m.group(1)
    # Pattern 2: full state name anywhere in the string (case-insensitive).
    # Sort by length descending so "new york" matches before "new" alone.
    low = s.lower()
    for name in sorted(US_STATE_NAME_TO_CODE.keys(), key=len, reverse=True):
        # Word-boundary match to avoid "indiana" inside "indianapolis"... well
        # actually that's fine, indianapolis IS in indiana. But "ohio" inside
        # "ohiopyle" or "iowa" inside "iowan" could mislead. Use word boundary.
        if re.search(r"\b" + re.escape(name) + r"\b", low):
            return US_STATE_NAME_TO_CODE[name]
    return None


def _normalize_adzuna_item(item: dict) -> Optional[dict]:
    """Map one Adzuna /search result into the pv_jobs dict shape, or None
    if essential fields are missing."""
    external_id = item.get("id")
    if not external_id:
        return None

    title = (item.get("title") or "").strip()

    # Company: Adzuna sometimes returns "Unknown" for company.display_name.
    company = ""
    co = item.get("company") or {}
    if isinstance(co, dict):
        company = (co.get("display_name") or "").strip()
    if company.lower() == "unknown":
        company = ""

    # Location: display_name + lat/lng.
    location_name = ""
    loc = item.get("location") or {}
    if isinstance(loc, dict):
        location_name = (loc.get("display_name") or "").strip()
    lat = item.get("latitude")
    lng = item.get("longitude")
    try:
        lat = float(lat) if lat not in (None, "", 0, "0", 0.0) else None
        lng = float(lng) if lng not in (None, "", 0, "0", 0.0) else None
    except Exception:
        lat = None
        lng = None

    # State extraction: Adzuna's location.area is structured like
    # ["US", "Hawaii", "Honolulu"] -- the second element is the state name.
    # Fall back to parsing the display_name if area isn't usable.
    state = None
    if isinstance(loc, dict):
        area = loc.get("area")
        if isinstance(area, list) and len(area) >= 2:
            state = US_STATE_NAME_TO_CODE.get((area[1] or "").strip().lower())
    if not state:
        state = _extract_state_code(location_name)

    # Note: an earlier v0.1.17 filter stripped Wake Island-tagged Adzuna
    # jobs as suspected pollution. Reverted v0.1.19 -- Wake Island Airfield
    # is a working US military installation, and contractors (KBR, Lockheed,
    # Booz Allen, etc.) do staff legitimate positions there. Better to let
    # users see and decide than to silently drop potentially-real jobs.

    # Remote detection: Adzuna doesn't have a structured flag. Use explicit
    # WFH phrases rather than the bare word "remote" -- which over-fires on
    # job titles like "Remote Lodge Manager", "Remote Field Engineer",
    # "Remote-site Operations", and even the actual town of Remote, Oregon.
    # We accept false negatives (missing a few true remote jobs whose copy
    # doesn't use these phrases) in exchange for far fewer false positives.
    combined = (title + " " + location_name).lower()
    REMOTE_PHRASES = (
        "work from home", "wfh", "work-from-home",
        "fully remote", "100% remote", "100 % remote",
        "remote work", "remote position", "remote role",
        "remote job", "remote opportunity", "remote-first",
        "remote first", "work remotely", "working remotely",
        "telecommute", "telework", "telecommuting",
        "remote (us)", "remote, us", "remote, united states",
        "remote usa", "remote - us", "remote – us",
    )
    is_remote = any(p in combined for p in REMOTE_PHRASES)

    # Salary: salary_min, salary_max as floats. Already in annual USD.
    salary_min = item.get("salary_min")
    salary_max = item.get("salary_max")
    try:
        salary_min = float(salary_min) if salary_min not in (None, "") else None
    except Exception:
        salary_min = None
    try:
        salary_max = float(salary_max) if salary_max not in (None, "") else None
    except Exception:
        salary_max = None

    # Sector from category tag.
    cat = item.get("category") or {}
    tag = cat.get("tag") if isinstance(cat, dict) else None
    sector = _adzuna_sector_for_tag(tag)

    # Level from title keywords.
    level = _level_from_title_keywords(title)

    # Posted date — Adzuna gives ISO 8601 in 'created'.
    posted_at = item.get("created")

    # URL: redirect_url is the Adzuna-tracked link to the actual posting.
    url = item.get("redirect_url") or ""

    # Description: cap at 2KB.
    description = (item.get("description") or "")
    if description and len(description) > 2000:
        description = description[:2000] + "…"

    return {
        "source": "adzuna",
        "external_id": str(external_id),
        "title": title,
        "company": company,
        "sector": sector,
        "level": level,
        "location_name": location_name,
        "lat": lat,
        "lng": lng,
        "state": state,
        "is_remote": is_remote,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": "USD",
        "description": description,
        "url": url,
        "posted_at": posted_at,
        "raw": item,
    }


def _clean_location_for_adzuna(loc_name: str) -> Optional[str]:
    """Strip ", USA"/", United States" tail from a Google-geocoded location_name
    so Adzuna's where= parameter sees just the city/region."""
    if not loc_name:
        return None
    s = re.sub(r",\s*(USA|United States|United States of America)\s*$",
               "", loc_name, flags=re.IGNORECASE).strip()
    return s if s else None


def _get_adzuna_target_locations(max_locations: int = 3) -> List[str]:
    """Return distinct location strings (city or state) drawn from active
    pv_searches, ranked by how many users care about them. Used to bias
    Adzuna's where= queries toward where the user base is actually looking,
    instead of relying only on the global sort_by=date feed."""
    locations = []
    seen = set()
    try:
        with get_db() as conn:
            c = conn.cursor()
            # Address-mode: use location_name (cleaned).
            c.execute("""
                SELECT location_name, COUNT(*) AS n
                FROM pv_searches
                WHERE where_mode = 'address'
                  AND location_name IS NOT NULL
                  AND TRIM(location_name) <> ''
                  AND alert_level <> 'off'
                GROUP BY location_name
                ORDER BY n DESC
            """)
            for row in c.fetchall():
                cleaned = _clean_location_for_adzuna(row[0])
                if cleaned and cleaned.lower() not in seen:
                    locations.append(cleaned)
                    seen.add(cleaned.lower())
                    if len(locations) >= max_locations:
                        break

            # State-mode: use proper-case state name.
            if len(locations) < max_locations:
                c.execute("""
                    SELECT state_code, COUNT(*) AS n
                    FROM pv_searches
                    WHERE where_mode = 'state'
                      AND state_code IS NOT NULL
                      AND alert_level <> 'off'
                    GROUP BY state_code
                    ORDER BY n DESC
                """)
                for row in c.fetchall():
                    name = US_STATE_CODE_TO_NAME.get(row[0])
                    if name and name.lower() not in seen:
                        locations.append(name)
                        seen.add(name.lower())
                        if len(locations) >= max_locations:
                            break
    except Exception as e:
        print("[adzuna] target location query failed: " + str(e))
    return locations


# ── Greenhouse normalization helpers ──────────────────────────────────────────
# Greenhouse is a per-employer ATS. Each company has its own public board at
# https://boards-api.greenhouse.io/v1/boards/{slug}/jobs and the slug is
# determined by the employer (usually their company name as lowercase-hyphens
# or lowercase-concatenated). Lat/lng aren't in the payload, so we geocode
# the location string via Google with an in-memory per-process cache.

# In-memory geocode cache, keyed by location string. Reset on each Render
# redeploy. Cheap to rebuild (~50-200 unique locations per cron tick).
_GREENHOUSE_GEOCODE_CACHE = {}


def _get_watched_companies() -> List[str]:
    """Read distinct non-empty company filters across all non-off searches.
    Drives which Greenhouse boards we attempt to pull from on this tick."""
    companies = []
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT DISTINCT TRIM(company) AS co
                FROM pv_searches
                WHERE company IS NOT NULL
                  AND TRIM(company) <> ''
                  AND alert_level <> 'off'
                ORDER BY co
            """)
            for row in c.fetchall():
                if row[0]:
                    companies.append(row[0])
    except Exception as e:
        print("[greenhouse] failed to read watched companies: " + str(e))
    return companies


def _greenhouse_slug_variants(company: str) -> List[str]:
    """Generate likely Greenhouse board slugs for a company name. We try the
    hyphenated form first (most common), then concatenated, then first-word.
    Stops at the first variant that returns a real board."""
    if not company:
        return []
    s = company.lower().strip()
    # Strip common legal suffixes that aren't part of the slug.
    s = re.sub(r'\s+(inc\.?|corp\.?|llc|ltd\.?|llp|holdings?|group|co\.?|company)$', '', s)
    # Strip punctuation except spaces and hyphens.
    s = re.sub(r"[^\w\s\-]", "", s)
    s = s.strip()
    if not s:
        return []
    variants = []
    hyphenated = re.sub(r"\s+", "-", s)
    variants.append(hyphenated)
    concat = re.sub(r"\s+", "", s)
    if concat != hyphenated:
        variants.append(concat)
    parts = s.split()
    first = parts[0] if parts else ""
    if first and first not in (hyphenated, concat):
        variants.append(first)
    return variants


def _greenhouse_sector_for_department(dept_name) -> str:
    """Map a Greenhouse 'department' string (free-text per employer) to one
    of our 21 sector slugs. Falls back to 'administration' when unrecognized."""
    if not dept_name:
        return "administration"
    d = str(dept_name).lower()
    if any(w in d for w in ["engineering", "software", "infrastructure", "devops",
                            "platform", "backend", "frontend", "fullstack", "sre"]):
        return "engineering"
    if any(w in d for w in ["information technology", " it ", "it ", "tech support",
                            "systems", "security", "cyber"]):
        return "it"
    if "product" in d:
        return "it"
    if any(w in d for w in ["data", "analytics", "machine learning", "research",
                            " ml ", " ai ", "science"]):
        return "science"
    if any(w in d for w in ["design", "creative", "brand", " ux", " ui"]):
        return "creative"
    if any(w in d for w in ["marketing", "growth", "communications", " pr "]):
        return "marketing"
    if any(w in d for w in ["sales", "business development", "account executive",
                            "revenue"]):
        return "sales"
    if any(w in d for w in ["finance", "accounting", "treasury"]):
        return "finance"
    if any(w in d for w in ["people", "human resources", "talent", "recruit", " hr"]):
        return "hr"
    if any(w in d for w in ["legal", "compliance"]):
        return "legal"
    if any(w in d for w in ["customer", "support", "success"]):
        return "customer_service"
    if any(w in d for w in ["operations", " ops", "logistics", "supply"]):
        return "transport"
    if any(w in d for w in ["education", "teaching", "training", "learning"]):
        return "education"
    if any(w in d for w in ["health", "medical", "clinical"]):
        return "healthcare"
    return "administration"


def _geocode_location_cached(location_name: str):
    """Forward-geocode a location string via Google, caching results in memory.
    Returns (lat, lng) tuple or (None, None) on failure / no key configured."""
    if not location_name:
        return (None, None)
    key = location_name.strip().lower()
    if key in _GREENHOUSE_GEOCODE_CACHE:
        return _GREENHOUSE_GEOCODE_CACHE[key]
    if not GOOGLE_GEOCODING_API_KEY:
        _GREENHOUSE_GEOCODE_CACHE[key] = (None, None)
        return (None, None)
    try:
        url = ("https://maps.googleapis.com/maps/api/geocode/json?address="
               + urllib.parse.quote(location_name)
               + "&key=" + GOOGLE_GEOCODING_API_KEY)
        req = urllib.request.Request(url, headers={"User-Agent": "pivot.watch/0.1"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
        if data.get("status") == "OK":
            results = data.get("results") or []
            if results:
                loc = ((results[0].get("geometry") or {}).get("location")) or {}
                lat = loc.get("lat")
                lng = loc.get("lng")
                if lat is not None and lng is not None:
                    pair = (float(lat), float(lng))
                    _GREENHOUSE_GEOCODE_CACHE[key] = pair
                    return pair
    except Exception as e:
        print("[geocode-cache] '" + location_name + "' failed: " + str(e))
    _GREENHOUSE_GEOCODE_CACHE[key] = (None, None)
    return (None, None)


def _strip_html_tags(s: str) -> str:
    """Strip HTML tags and decode entities from a Greenhouse content field."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_greenhouse_item(job: dict, queried_company: str) -> Optional[dict]:
    """Map one Greenhouse /jobs item into the pv_jobs dict shape."""
    job_id = job.get("id") or job.get("internal_job_id")
    if not job_id:
        return None

    title = (job.get("title") or "").strip()
    company = (job.get("company_name") or queried_company or "").strip()

    # Location: 'location.name' if present, otherwise compose from offices[].
    location_name = ""
    loc = job.get("location") or {}
    if isinstance(loc, dict):
        location_name = (loc.get("name") or "").strip()
    if not location_name:
        offices = job.get("offices") or []
        if offices and isinstance(offices, list):
            names = []
            for o in offices:
                n = (o or {}).get("name") or (o or {}).get("location") or ""
                if n:
                    names.append(n.strip())
            location_name = ", ".join(names[:3])  # cap at 3 to keep readable

    # Remote detection: scan title + location for "remote" / "wfh" / "work from home".
    combined = (title + " " + location_name).lower()
    is_remote = "remote" in combined or "wfh" in combined or "work from home" in combined

    # Geocode the location for radius search support (cached).
    lat, lng = (None, None)
    if location_name and not is_remote:
        lat, lng = _geocode_location_cached(location_name)

    # Sector from departments[0].name (free-text per employer).
    sector = "administration"
    depts = job.get("departments") or []
    if depts and isinstance(depts, list):
        first_dept = (depts[0] or {}).get("name")
        sector = _greenhouse_sector_for_department(first_dept)

    # Level from title keywords (Greenhouse has no structured level).
    level = _level_from_title_keywords(title)

    # Description: strip HTML from 'content'.
    description = _strip_html_tags(job.get("content") or "")
    if description and len(description) > 2000:
        description = description[:2000] + "…"

    # URL: absolute_url is the canonical public link.
    url = job.get("absolute_url") or ""

    # Posted at: updated_at is the most useful timestamp Greenhouse exposes.
    posted_at = job.get("updated_at") or job.get("first_published")

    # State extraction: location_name is free-text per employer
    # ("Honolulu, HI", "New York, NY", "Remote, US", etc).
    state = _extract_state_code(location_name)

    return {
        "source": "greenhouse",
        "external_id": str(job_id),
        "title": title,
        "company": company,
        "sector": sector,
        "level": level,
        "location_name": location_name,
        "lat": lat,
        "lng": lng,
        "state": state,
        "is_remote": is_remote,
        "salary_min": None,           # Greenhouse doesn't expose salary
        "salary_max": None,
        "salary_currency": "USD",
        "description": description,
        "url": url,
        "posted_at": posted_at,
        "raw": job,
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
    # v0.1.12+: two-phase fetch. Phase 1 pulls global sort_by=date for
    # nationwide newest jobs. Phase 2 pulls location-targeted queries (where=)
    # for the cities/states users are watching, fixing the global-feed bias
    # toward whichever metro is posting most that day. Budget: 6 + 3×3 = 15
    # calls/tick × 2 ticks/day = 30/day, well under the 33/day free-tier limit.
    @staticmethod
    def fetch_adzuna() -> List[dict]:
        app_id = (os.environ.get("ADZUNA_APP_ID") or "").strip()
        app_key = (os.environ.get("ADZUNA_APP_KEY") or "").strip()
        if not app_id or not app_key:
            print("[adzuna] ADZUNA_APP_ID or ADZUNA_APP_KEY not set; skipping")
            return []

        out = []
        results_per_page = 50

        def _adzuna_page(page: int, where: Optional[str] = None) -> List[dict]:
            """One paginated fetch. Returns normalized job dicts (may be empty)."""
            url = ("https://api.adzuna.com/v1/api/jobs/us/search/" + str(page)
                   + "?app_id=" + urllib.parse.quote(app_id)
                   + "&app_key=" + urllib.parse.quote(app_key)
                   + "&results_per_page=" + str(results_per_page)
                   + "&sort_by=date")
            if where:
                url += "&where=" + urllib.parse.quote(where)
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": Sources.BROWSER_UA,
                    "Accept": "application/json",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
            except Exception as e:
                tag = "where=" + where if where else "global"
                print("[adzuna] " + tag + " page " + str(page) + " fetch failed: " + str(e))
                return []
            results = data.get("results") or []
            page_out = []
            for item in results:
                try:
                    normalized = _normalize_adzuna_item(item)
                    if normalized:
                        page_out.append(normalized)
                except Exception as e:
                    print("[adzuna] normalize failed: " + str(e))
            return page_out

        # Phase 1: nationwide newest (6 pages, ~300 jobs).
        global_count = 0
        for page in range(1, 7):
            page_out = _adzuna_page(page, where=None)
            if not page_out:
                break
            out.extend(page_out)
            global_count += len(page_out)
            if len(page_out) < results_per_page:
                break
        print("[adzuna] global phase: " + str(global_count) + " jobs across "
              + str(min(6, page)) + " pages")

        # Phase 2: location-targeted (3 pages × up to 3 user-watched locations).
        targets = _get_adzuna_target_locations(max_locations=3)
        if targets:
            print("[adzuna] location phase targets: " + ", ".join(targets))
            for loc in targets:
                loc_count = 0
                for page in range(1, 4):
                    page_out = _adzuna_page(page, where=loc)
                    if not page_out:
                        break
                    out.extend(page_out)
                    loc_count += len(page_out)
                    if len(page_out) < results_per_page:
                        break
                print("[adzuna] where='" + loc + "': " + str(loc_count) + " jobs")
        else:
            print("[adzuna] no location targets; skipping phase 2")

        print("[adzuna] fetched " + str(len(out)) + " jobs (pre-dedupe)")
        return out

    # ── Greenhouse (per-employer ATS feeds) ───────────────────────────────────
    # Stub for v0.1.0. v0.1.7+ pulls from
    # https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs for each
    # distinct company filter appearing in active pv_searches rows. Public, no
    # key, no auth. The slug is unknown ahead of time so we try a few common
    # variants per company name (hyphenated, concatenated, first-word).
    @staticmethod
    def fetch_greenhouse() -> List[dict]:
        companies = _get_watched_companies()
        if not companies:
            print("[greenhouse] no company filters in active searches; skipping")
            return []
        print("[greenhouse] companies in active searches: " + ", ".join(companies))

        out = []
        hits = 0
        for company in companies:
            slug_used = None
            jobs_payload = None
            # Try each slug variant until one returns 200 with a 'jobs' key.
            for slug in _greenhouse_slug_variants(company):
                url = ("https://boards-api.greenhouse.io/v1/boards/"
                       + urllib.parse.quote(slug) + "/jobs?content=true")
                try:
                    req = urllib.request.Request(url, headers={
                        "User-Agent": Sources.BROWSER_UA,
                        "Accept": "application/json",
                    })
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
                    if isinstance(data, dict) and "jobs" in data:
                        slug_used = slug
                        jobs_payload = data.get("jobs") or []
                        break
                except urllib.error.HTTPError as e:
                    # 404 is the normal "no board for this slug" -- try next variant silently.
                    if e.code != 404:
                        print("[greenhouse] " + slug + " HTTP " + str(e.code) + " — " + str(e))
                except Exception as e:
                    print("[greenhouse] " + slug + " failed: " + str(e))

            if not slug_used:
                print("[greenhouse] no board found for '" + company + "' (tried variants)")
                continue

            hits += 1
            print("[greenhouse] " + company + " → " + slug_used + ": " + str(len(jobs_payload)) + " jobs")
            for job in jobs_payload:
                try:
                    normalized = _normalize_greenhouse_item(job, company)
                    if normalized:
                        out.append(normalized)
                except Exception as e:
                    print("[greenhouse] normalize failed: " + str(e))

        print("[greenhouse] fetched " + str(len(out)) + " jobs from " + str(hits) + " companies")
        return out

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
    NEWLY inserted (so we only filter-match the new ones, never re-alert on
    existing rows). On conflict, existing rows are UPDATED -- this lets
    sector/level mapping changes (e.g. expanding the OPM-series → sector map)
    propagate to all existing jobs on the next cron tick, instead of leaving
    them stuck at whatever bucket they were originally categorized into.
    The xmax=0 trick distinguishes a true INSERT from a DO UPDATE re-write."""
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
                    location_name, lat, lng, state, is_remote,
                    salary_min, salary_max, salary_currency,
                    description, url, posted_at, raw_payload, geom
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s::jsonb,
                    CASE WHEN %s IS NOT NULL
                         THEN ST_SetSRID(ST_GeomFromText(%s), 4326)::geography
                         ELSE NULL END
                )
                ON CONFLICT (source, external_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    company = EXCLUDED.company,
                    sector = EXCLUDED.sector,
                    level = EXCLUDED.level,
                    location_name = EXCLUDED.location_name,
                    lat = EXCLUDED.lat,
                    lng = EXCLUDED.lng,
                    state = EXCLUDED.state,
                    is_remote = EXCLUDED.is_remote,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    salary_currency = EXCLUDED.salary_currency,
                    description = EXCLUDED.description,
                    url = EXCLUDED.url,
                    posted_at = EXCLUDED.posted_at,
                    fetched_at = CURRENT_TIMESTAMP,
                    raw_payload = EXCLUDED.raw_payload,
                    geom = EXCLUDED.geom
                RETURNING id, (xmax = 0) AS inserted
            """, (
                j["source"], j["external_id"], j.get("title"), j.get("company"),
                j.get("sector"), j.get("level"),
                j.get("location_name"), j.get("lat"), j.get("lng"), j.get("state"),
                bool(j.get("is_remote") or False),
                j.get("salary_min"), j.get("salary_max"), j.get("salary_currency"),
                j.get("description"), j.get("url"), j.get("posted_at"),
                json_lib.dumps(j.get("raw") or {}, default=str),
                geom_wkt, geom_wkt,
            ))
            row = c.fetchone()
            # Only treat as "new" if this was a true INSERT (xmax = 0).
            # DO UPDATE conflicts return the row but with xmax != 0; skip those
            # so filter_match_and_alert never re-fires on existing jobs.
            if row and row[1]:
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
              (s.where_mode = 'address' AND (
                j.is_remote = TRUE
                OR (s.geom IS NOT NULL AND j.geom IS NOT NULL
                    AND ST_DWithin(j.geom, s.geom,
                                   COALESCE(NULLIF(s.radius_value, 'remote'), '25')::int * 1609.344))
              ))
              OR
              (s.where_mode = 'state' AND (
                j.is_remote = TRUE
                OR j.state = s.state_code
              ))
              OR
              (s.where_mode = 'anywhere')
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


def _fetch_adzuna_where(where: str, max_pages: int = 3) -> List[dict]:
    """On-demand Adzuna pull for a single location string (a just-saved search's
    location). Mirrors fetch_adzuna's per-location phase but standalone, so
    saving a "near a place" search seeds jobs for exactly that place instead of
    waiting for the 12h cron's popularity-based sampling."""
    app_id = (os.environ.get("ADZUNA_APP_ID") or "").strip()
    app_key = (os.environ.get("ADZUNA_APP_KEY") or "").strip()
    if not app_id or not app_key or not where:
        return []
    results_per_page = 50
    out = []
    for page in range(1, max_pages + 1):
        url = ("https://api.adzuna.com/v1/api/jobs/us/search/" + str(page)
               + "?app_id=" + urllib.parse.quote(app_id)
               + "&app_key=" + urllib.parse.quote(app_key)
               + "&results_per_page=" + str(results_per_page)
               + "&sort_by=date"
               + "&where=" + urllib.parse.quote(where))
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": Sources.BROWSER_UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json_lib.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:
            print("[adzuna on-demand] where='" + where + "' page " + str(page) + " failed: " + str(e))
            break
        results = data.get("results") or []
        for item in results:
            try:
                normalized = _normalize_adzuna_item(item)
                if normalized:
                    out.append(normalized)
            except Exception as e:
                print("[adzuna on-demand] normalize failed: " + str(e))
        if len(results) < results_per_page:
            break
    print("[adzuna on-demand] where='" + where + "': " + str(len(out)) + " jobs")
    return out


def _seed_adzuna_for_search(location_name: str):
    """Background: pull Adzuna for a just-saved search's location and upsert into
    pv_jobs. Fire-and-forget so the save response isn't blocked."""
    try:
        where = _clean_location_for_adzuna(location_name)
        if not where:
            return
        normalized = _fetch_adzuna_where(where)
        if not normalized:
            return
        with get_db() as conn:
            upsert_jobs(conn, normalized)
        print("[adzuna on-demand] seeded " + str(len(normalized)) + " jobs for '" + where + "'")
    except Exception as e:
        print("[adzuna on-demand] seed failed: " + str(e))


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
    """Pivot-watch-specific signup metrics for the 3Brains scoreboard.
    Requires X-Admin-Token header matching ADMIN_STATS_TOKEN env var.

    Counts from pv_signups (not the shared users table) so the dashboard
    reflects pivot.watch users only -- not the cross-app total of every
    app sharing this Postgres. pv_signups is populated by /auth/register
    and /auth/login, and backfilled at startup from pv_searches authors."""
    expected = os.environ.get("ADMIN_STATS_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM pv_signups")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pv_signups WHERE first_seen_at >= NOW() - INTERVAL '24 hours'")
        signups_24h = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pv_signups WHERE first_seen_at >= NOW() - INTERVAL '7 days'")
        signups_7d = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pv_signups WHERE first_seen_at >= NOW() - INTERVAL '30 days'")
        signups_30d = c.fetchone()[0]
        c.execute("SELECT MAX(first_seen_at) FROM pv_signups")
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
