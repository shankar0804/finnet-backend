import re
import time
import sqlite3
import logging
import threading
from collections import OrderedDict
from openai import OpenAI
from database.db import supabase

logger = logging.getLogger(__name__)

import os

NVIDIA_KEY = os.environ.get("NVIDIA_KEY", "").strip()

# ── Persistent in-memory SQLite per platform ──
# We keep ONE sqlite connection per platform alive for the process lifetime.
# A lazy refresh re-populates the table when CACHE_TTL expires — without
# rebuilding the schema or re-materializing pandas DataFrames per query.
# Significant CPU/RAM win vs. the old per-request sqlite + to_sql path.
CACHE_TTL = int(os.environ.get("MCP_CACHE_TTL", "60"))
_DB_LOCK = threading.Lock()
_DB_CONNS: dict = {}          # platform -> sqlite3.Connection
_DB_LAST_REFRESH: dict = {}   # platform -> epoch seconds

# ── Result cache for identical recent questions ──
# Key: (platform, normalized_query). Value: (expiry_ts, result_dict).
# Saves the LLM + SQL round-trip when the same question is asked twice
# within RESULT_CACHE_TTL seconds.
RESULT_CACHE_TTL = int(os.environ.get("MCP_RESULT_CACHE_TTL", "30"))
_RESULT_CACHE_MAX = 64
_RESULT_CACHE: "OrderedDict[tuple, tuple]" = OrderedDict()
_RESULT_CACHE_LOCK = threading.Lock()

# Schema definitions — all three platform tables
SCHEMA_IG = {
    "table": "influencers",
    "columns": {
        "id": "text",
        "username": "text",
        "creator_name": "text",
        "profile_link": "text",
        "platform": "text",
        "niche": "text",
        "language": "text",
        "location": "text",
        "followers": "integer",
        "avg_views": "integer",
        "engagement_rate": "real",
        "avg_video_length": "integer",
        "avd": "text",
        "skip_rate": "text",
        "age_13_17": "text",
        "age_18_24": "text",
        "age_25_34": "text",
        "age_35_44": "text",
        "age_45_54": "text",
        "male_pct": "text",
        "female_pct": "text",
        "gender": "text",
        "city_1": "text",
        "city_2": "text",
        "city_3": "text",
        "city_4": "text",
        "city_5": "text",
        "contact_numbers": "text",
        "mail_id": "text",
        "managed_by": "text",
        "creator_group_id": "text",
        "last_scraped_at": "text",
        "last_ocr_at": "text",
        "last_manual_at": "text",
    }
}

SCHEMA_YT = {
    "table": "youtube_creators",
    "columns": {
        "id": "text",
        "channel_id": "text",
        "channel_handle": "text",
        "channel_name": "text",
        "profile_link": "text",
        "niche": "text",
        "language": "text",
        "gender": "text",
        "location": "text",
        "subscribers": "integer",
        "total_videos": "integer",
        "avg_long_views": "integer",
        "long_engagement_rate": "real",
        "avg_long_duration": "integer",
        "avg_short_views": "integer",
        "short_engagement_rate": "real",
        "avg_short_duration": "integer",
        "avd": "text",
        "skip_rate": "text",
        "age_13_17": "text",
        "age_18_24": "text",
        "age_25_34": "text",
        "age_35_44": "text",
        "age_45_54": "text",
        "male_pct": "text",
        "female_pct": "text",
        "city_1": "text",
        "city_2": "text",
        "city_3": "text",
        "city_4": "text",
        "city_5": "text",
        "contact_numbers": "text",
        "mail_id": "text",
        "managed_by": "text",
        "creator_group_id": "text",
        "last_scraped_at": "text",
        "last_ocr_at": "text",
        "last_manual_at": "text",
    }
}

SCHEMA_LI = {
    "table": "linkedin_creators",
    "columns": {
        "id": "text",
        "profile_id": "text",
        "full_name": "text",
        "profile_link": "text",
        "headline": "text",
        "summary": "text",
        "current_company": "text",
        "current_title": "text",
        "industry": "text",
        "niche": "text",
        "language": "text",
        "gender": "text",
        "location": "text",
        "connections": "integer",
        "contact_numbers": "text",
        "mail_id": "text",
        "managed_by": "text",
        "creator_group_id": "text",
        "last_scraped_at": "text",
        "last_manual_at": "text",
    }
}

ALLOWED_TABLES = {'INFLUENCERS', 'YOUTUBE_CREATORS', 'LINKEDIN_CREATORS'}

# ─── Platform signal detection ────────────────────────────────────
# We use simple keyword matching to infer which platform the user means.
# If NONE of these signals is found, we return a `clarify` response and
# the bot asks the user to pick one.
IG_KEYWORDS = {
    'instagram', 'insta', 'ig', 'reel', 'reels', 'post', 'posts',
    'followers', 'avg views', 'avg_views', 'average views',
    'influencer', 'influencers',
}
YT_KEYWORDS = {
    'youtube', 'yt', 'channel', 'channels', 'subscriber', 'subscribers',
    'subs', 'short', 'shorts', 'long-form', 'longform', 'video views',
    'watch time', 'ctr',
}
LI_KEYWORDS = {
    'linkedin', 'li',
    'connection', 'connections',
    'company', 'companies',
    'headline', 'headlines',
    'professional', 'professionals',
    'employee', 'employees',
    'industry', 'industries',
    'ceo', 'founder', 'founders', 'vp', 'cto', 'manager', 'managers', 'director', 'directors',
}


def detect_platform(user_query: str):
    """Return 'instagram' | 'youtube' | 'linkedin' | None based on keywords.

    We tokenize on word boundaries so we don't match "ig" inside "strategic".
    """
    if not user_query:
        return None
    text = f' {user_query.lower()} '
    scores = {'instagram': 0, 'youtube': 0, 'linkedin': 0}
    for kw in IG_KEYWORDS:
        if re.search(rf'(?<![A-Za-z]){re.escape(kw)}(?![A-Za-z])', text):
            scores['instagram'] += 1
    for kw in YT_KEYWORDS:
        if re.search(rf'(?<![A-Za-z]){re.escape(kw)}(?![A-Za-z])', text):
            scores['youtube'] += 1
    for kw in LI_KEYWORDS:
        if re.search(rf'(?<![A-Za-z]){re.escape(kw)}(?![A-Za-z])', text):
            scores['linkedin'] += 1

    best = max(scores, key=scores.get)
    if scores[best] == 0:
        return None
    # Tie-breaking: if two platforms tie, we can't decide → ask
    sorted_scores = sorted(scores.values(), reverse=True)
    if len(sorted_scores) >= 2 and sorted_scores[0] == sorted_scores[1] and sorted_scores[0] > 0:
        return None
    return best


# ─── Query sanity patches (safety net for the LLM) ────────────────
_WORD_COUNTS = {
    'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
    'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
}


def _extract_limit_from_query(q: str):
    """Return an integer N if the user asked for 'N creators / give me N / top N / first N', else None.

    Handles phrasings like:
      - "top 3 / first 5 / only 10 / latest 20"
      - "give me 3", "show us 5", "list me 10"
      - "give me a list of 10", "show me a set of 20"
      - "10 creators", "10 finance creators", "10 top beauty influencers"
      - "give me three creators" (number-words one..ten)
    """
    if not q:
        return None
    ql = q.lower()

    # "top 3" / "first 5" / "only 10" / "latest 20"
    m = re.search(r'\b(?:top|first|only|last|latest)\s+(\d{1,4})\b', ql)
    if m:
        return int(m.group(1))

    # "give/show/find/list/get me [a list/set/batch of] 3"
    m = re.search(
        r'\b(?:give|show|list|find|fetch|get)\s+'
        r'(?:me\s+|us\s+)?'
        r'(?:a\s+(?:list|set|batch|bunch|couple|few|handful)\s+(?:of\s+)?)?'
        r'(\d{1,4})\b',
        ql,
    )
    if m:
        return int(m.group(1))

    # "10 creators", "10 finance creators", "10 top beauty influencers"
    # Up to 3 adjective words allowed between the number and the noun.
    m = re.search(
        r'\b(\d{1,4})\s+(?:[a-z][a-z\-]{1,20}\s+){0,3}'
        r'(?:creator|creators|channel|channels|profile|profiles|'
        r'influencer|influencers|result|results|row|rows|account|accounts|page|pages)\b',
        ql,
    )
    if m:
        n = int(m.group(1))
        if n <= 500:
            return n

    # "give me three creators" (number-words)
    m = re.search(
        r'\b(?:top|first|only|give|show|list)\s+(?:me\s+|us\s+)?'
        r'(?:a\s+(?:list|set|batch)\s+(?:of\s+)?)?'
        r'(one|two|three|four|five|six|seven|eight|nine|ten)\b',
        ql,
    )
    if m:
        return _WORD_COUNTS.get(m.group(1))
    return None


def _patch_sql_with_user_limit(sql: str, user_query: str) -> str:
    """If the user asked for a specific count but the LLM forgot LIMIT, inject it."""
    requested = _extract_limit_from_query(user_query)
    if not requested:
        return sql
    if re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE):
        return sql
    cleaned = sql.strip().rstrip(';').strip()
    return f"{cleaned} LIMIT {requested}"

# Build column listings for the prompt
def _cols_str(schema):
    return "\n".join(f"  - {col} ({dtype})" for col, dtype in schema["columns"].items())


# ─── SQL SAFETY VALIDATOR ───────────────────────────────────────────

def validate_sql(sql: str) -> tuple:
    """
    Multi-layer safety validation for LLM-generated SQL.
    Returns (is_safe: bool, error_message: str, cleaned_sql: str)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query", ""

    cleaned = sql.strip().rstrip(";").strip()

    # Layer 1: Must be a SELECT statement (read-only)
    if not cleaned.upper().startswith("SELECT"):
        return False, "Only SELECT queries are allowed", ""

    # Layer 2: Block dangerous keywords
    dangerous_keywords = [
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
        'TRUNCATE', 'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'ATTACH',
        'DETACH', 'PRAGMA', 'MERGE', 'CALL',
    ]
    sql_upper = cleaned.upper()
    for kw in dangerous_keywords:
        if re.search(rf'\b{kw}\b', sql_upper):
            return False, f"Blocked: dangerous keyword '{kw}' detected", ""

    # Layer 3: Only allow the known tables
    from_matches = re.findall(r'\bFROM\s+(\w+)', sql_upper)
    join_matches = re.findall(r'\bJOIN\s+(\w+)', sql_upper)
    all_tables = from_matches + join_matches
    for table in all_tables:
        if table not in ALLOWED_TABLES:
            return False, f"Blocked: unauthorized table '{table}'", ""

    # Layer 4: Ensure at least one valid table is referenced
    if not any(t in sql_upper for t in ALLOWED_TABLES):
        return False, "Query must reference a valid table (influencers, youtube_creators, or linkedin_creators)", ""

    # Layer 5: Enforce a safety LIMIT to prevent huge result sets
    if 'LIMIT' not in sql_upper:
        cleaned += " LIMIT 200"

    # Layer 6: Block subqueries to unauthorized tables
    subquery_froms = re.findall(r'\(\s*SELECT[^)]*FROM\s+(\w+)', sql_upper)
    for table in subquery_froms:
        if table not in ALLOWED_TABLES:
            return False, f"Blocked: subquery references unauthorized table '{table}'", ""

    return True, "OK", cleaned


# ─── SYSTEM PROMPT FOR MULTI-TABLE SQL GENERATION ────────────────────

SYSTEM_PROMPT = f"""You are an expert SQLite query writer for an influencer talent agency database.

**Your job:** Convert the user's natural language question into a SINGLE valid SQLite SELECT query.

**Database: SQLite (in-memory)**

There are THREE tables. Choose the correct one based on the user's question:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Table 1: `influencers`** (Instagram creators)
{_cols_str(SCHEMA_IG)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Table 2: `youtube_creators`** (YouTube channels)
{_cols_str(SCHEMA_YT)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Table 3: `linkedin_creators`** (LinkedIn profiles)
{_cols_str(SCHEMA_LI)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**How to choose the table (VERY IMPORTANT):**
- The user message will ALWAYS begin with an explicit "Platform:" hint — trust it.
- If platform hint says "instagram" → use `influencers` (text values for followers/views).
- If platform hint says "youtube"   → use `youtube_creators`.
- If platform hint says "linkedin"  → use `linkedin_creators`.
- Queries touch ONE table at a time. No JOINs.

**Rules:**
1. Write ONLY a single SELECT statement. No explanations, no markdown, no code fences.
2. Use ONLY the columns listed above for the chosen table. Never invent columns.
3. **DEFAULT columns** for Instagram queries: `creator_name, username, profile_link, niche, followers, avg_views`
4. **DEFAULT columns** for YouTube queries:  `channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate, avg_short_views, short_engagement_rate`
5. **DEFAULT columns** for LinkedIn queries: `full_name, profile_id, profile_link, headline, current_company, connections`
6. For text columns, empty/missing values may be NULL or empty string ''. Use: `(column IS NULL OR TRIM(column) = '')`
7. For numeric columns, missing data is NULL or 0. To find valid data: `column IS NOT NULL AND column > 0`
8. Use case-insensitive matching for text: `LOWER(column) LIKE LOWER('%value%')`
9. **Number shorthand (CRITICAL)**:
   - "500k" / "500K" means 500000. "1.2m" / "1.2M" means 1200000. "10b" means 10000000000.
   - ALWAYS expand these in the SQL — never write `followers > 500`, write `followers > 500000`.
10. **Count phrases (CRITICAL)**: If the user says "give me N", "show me N", "top N", "first N", "only N", or any variant specifying a count of creators, ALWAYS add `LIMIT N` to the query. This includes phrases with words in between like "give me a list of N" or "show me the top N".
11. **Threshold phrasing**:
    - "above X" / "over X" / "more than X" → `column > X`
    - "below X" / "under X" / "less than X" → `column < X`
    - "at least X" → `column >= X`
    - "between X and Y" → `column BETWEEN X AND Y`
12. When the user mentions a count AND a threshold (e.g. "3 creators above 500k"), ALWAYS add both `WHERE column > <threshold>` and `ORDER BY column DESC LIMIT <count>`.
13. **Default ordering**: when the query is about "top", "best", "biggest", or any size word, ORDER BY the primary metric DESC (followers / subscribers / connections).
14. **Engagement rate scale (CRITICAL)**: `engagement_rate`, `long_engagement_rate`, and `short_engagement_rate` are stored as **percentage-numbers** (e.g. `7.5` means 7.5%, `0.8` means 0.8%). So:
    - "engagement rate over 5%" / "ER > 5%" / "above 5% engagement" → `engagement_rate > 5` (NEVER `> 0.05`).
    - "at least 2%" → `engagement_rate >= 2`.
    - Ignore the `%` sign — use the number as-is.
15. **City columns vs location (CRITICAL)**:
    - `location` = the creator's OWN home city (where the creator lives).
    - `city_1`, `city_2`, `city_3`, `city_4`, `city_5` = top 5 cities of the creator's AUDIENCE (audience demographics, ordered by share).
    - Phrases like "audience in Delhi", "fans from Mumbai", "viewers in Bangalore", "Delhi audience", **"delhi as city 1"**, "city 1 is delhi", or "top audience city = X" refer to the city_* columns, NOT `location`.
    - "Delhi as city 1" specifically means **`LOWER(city_1) LIKE LOWER('%delhi%')`**.
    - "audience in delhi" (without specifying rank) means the city appears in ANY of city_1..city_5:
      `(LOWER(city_1) LIKE '%delhi%' OR LOWER(city_2) LIKE '%delhi%' OR LOWER(city_3) LIKE '%delhi%' OR LOWER(city_4) LIKE '%delhi%' OR LOWER(city_5) LIKE '%delhi%')`.
    - Phrases like "creators from Delhi", "Delhi-based creator", "located in Mumbai" refer to `location`.
16. Output ONLY the raw SQL. Nothing else.

**Examples (follow these PATTERNS exactly):**

User: "Platform: instagram. show all creators"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers

User: "Platform: instagram. Give me 3 creators above 500k"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 500000 ORDER BY followers DESC LIMIT 3

User: "Platform: instagram. Show me 5 creators with more than 1M followers"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 1000000 ORDER BY followers DESC LIMIT 5

User: "Platform: instagram. Top 10 creators"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 0 ORDER BY followers DESC LIMIT 10

User: "Platform: youtube. youtube creators with more than 100k subscribers"
SELECT channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate FROM youtube_creators WHERE subscribers IS NOT NULL AND subscribers > 100000 ORDER BY subscribers DESC

User: "Platform: youtube. give me 3 youtube channels above 500k subscribers"
SELECT channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate FROM youtube_creators WHERE subscribers IS NOT NULL AND subscribers > 500000 ORDER BY subscribers DESC LIMIT 3

User: "Platform: youtube. show me youtube shorts engagement rates"
SELECT channel_name, channel_handle, subscribers, avg_short_views, short_engagement_rate FROM youtube_creators WHERE short_engagement_rate IS NOT NULL AND short_engagement_rate > 0 ORDER BY short_engagement_rate DESC

User: "Platform: linkedin. linkedin profiles in tech"
SELECT full_name, profile_id, profile_link, headline, current_company, industry, connections FROM linkedin_creators WHERE LOWER(industry) LIKE '%tech%' OR LOWER(headline) LIKE '%tech%'

User: "Platform: linkedin. top 3 profiles with most connections"
SELECT full_name, profile_id, profile_link, headline, current_company, connections FROM linkedin_creators WHERE connections IS NOT NULL AND connections > 0 ORDER BY connections DESC LIMIT 3

User: "Platform: instagram. who has the most followers"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 0 ORDER BY followers DESC LIMIT 1

User: "Platform: instagram. beauty creators in mumbai"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE LOWER(niche) LIKE LOWER('%beauty%') AND LOWER(location) LIKE LOWER('%mumbai%')

User: "Platform: instagram. 10 finance creators with over 5% engagement rate and delhi as city 1"
SELECT creator_name, username, profile_link, niche, followers, avg_views, engagement_rate, city_1 FROM influencers WHERE LOWER(niche) LIKE LOWER('%finance%') AND engagement_rate IS NOT NULL AND engagement_rate > 5 AND LOWER(city_1) LIKE LOWER('%delhi%') ORDER BY engagement_rate DESC LIMIT 10

User: "Platform: instagram. give me a list of 10 finance creators with over 5% engagement rate"
SELECT creator_name, username, profile_link, niche, followers, avg_views, engagement_rate FROM influencers WHERE LOWER(niche) LIKE LOWER('%finance%') AND engagement_rate IS NOT NULL AND engagement_rate > 5 ORDER BY engagement_rate DESC LIMIT 10

User: "Platform: instagram. creators whose audience is mostly from mumbai"
SELECT creator_name, username, profile_link, niche, followers, city_1, city_2, city_3 FROM influencers WHERE LOWER(city_1) LIKE LOWER('%mumbai%') OR LOWER(city_2) LIKE LOWER('%mumbai%') OR LOWER(city_3) LIKE LOWER('%mumbai%') OR LOWER(city_4) LIKE LOWER('%mumbai%') OR LOWER(city_5) LIKE LOWER('%mumbai%')

User: "Platform: youtube. top 5 youtube channels with long engagement above 3%"
SELECT channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate FROM youtube_creators WHERE long_engagement_rate IS NOT NULL AND long_engagement_rate > 3 ORDER BY long_engagement_rate DESC LIMIT 5

User: "Platform: instagram. creators whose niche is blank"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE niche IS NULL OR TRIM(niche) = ''

User: "Platform: instagram. count of creators by niche"
SELECT niche, COUNT(*) as count FROM influencers WHERE niche IS NOT NULL AND TRIM(niche) != '' GROUP BY niche ORDER BY count DESC

User: "Platform: linkedin. linkedin creators at google"
SELECT full_name, profile_id, profile_link, headline, current_company, connections FROM linkedin_creators WHERE LOWER(current_company) LIKE '%google%'"""


# ─── Table metadata (used for building schema + deterministic insight) ──

_PLATFORM_TO_SCHEMA = {
    'instagram': SCHEMA_IG,
    'youtube': SCHEMA_YT,
    'linkedin': SCHEMA_LI,
}


def _ensure_platform_db(platform: str) -> sqlite3.Connection:
    """Return a persistent sqlite connection for the given platform.

    Loads the table from Supabase the first time, and refreshes in-place
    every ``CACHE_TTL`` seconds using INSERT/DELETE (no pandas, no schema
    rebuilds). Thread-safe.
    """
    schema = _PLATFORM_TO_SCHEMA[platform]
    table = schema['table']
    cols = schema['columns']
    col_names = list(cols.keys())

    with _DB_LOCK:
        conn = _DB_CONNS.get(platform)
        if conn is None:
            conn = sqlite3.connect(':memory:', check_same_thread=False)
            col_defs = ', '.join(f'"{c}" {t}' for c, t in cols.items())
            conn.execute(f'CREATE TABLE IF NOT EXISTS {table} ({col_defs})')
            _DB_CONNS[platform] = conn
            _DB_LAST_REFRESH[platform] = 0

        now = time.time()
        last = _DB_LAST_REFRESH.get(platform, 0)
        if (now - last) <= CACHE_TTL:
            return conn

        try:
            resp = supabase.table(table).select('*').execute()
            records = resp.data or []
        except Exception as fetch_err:
            logger.error(f"[MCP] Supabase fetch failed for {table}: {fetch_err}")
            # Keep serving the stale cache if we already have data
            _DB_LAST_REFRESH[platform] = now - (CACHE_TTL // 2)
            return conn

        placeholders = ','.join('?' for _ in col_names)
        col_list = ','.join(f'"{c}"' for c in col_names)
        rows = [tuple(rec.get(c) for c in col_names) for rec in records]

        try:
            conn.execute('BEGIN')
            conn.execute(f'DELETE FROM {table}')
            if rows:
                conn.executemany(
                    f'INSERT INTO {table} ({col_list}) VALUES ({placeholders})',
                    rows,
                )
            conn.commit()
            _DB_LAST_REFRESH[platform] = now
        except Exception as load_err:
            conn.rollback()
            logger.error(f"[MCP] Failed to refresh {table}: {load_err}")

        return conn


def _normalize_query(q: str) -> str:
    return ' '.join((q or '').lower().split())


def _result_cache_get(platform: str, user_query: str):
    key = (platform, _normalize_query(user_query))
    with _RESULT_CACHE_LOCK:
        item = _RESULT_CACHE.get(key)
        if not item:
            return None
        expiry, value = item
        if time.time() > expiry:
            _RESULT_CACHE.pop(key, None)
            return None
        _RESULT_CACHE.move_to_end(key)
        return value


def _result_cache_put(platform: str, user_query: str, value: dict):
    key = (platform, _normalize_query(user_query))
    with _RESULT_CACHE_LOCK:
        _RESULT_CACHE[key] = (time.time() + RESULT_CACHE_TTL, value)
        _RESULT_CACHE.move_to_end(key)
        while len(_RESULT_CACHE) > _RESULT_CACHE_MAX:
            _RESULT_CACHE.popitem(last=False)


def _build_insight(platform: str, rows: list, sql: str) -> str:
    """Deterministic short summary — replaces the old LLM insight call."""
    n = len(rows)
    if n == 0:
        return "No results matched your query. Try loosening the filters (e.g. a lower follower threshold)."

    sql_upper = (sql or '').upper()
    is_aggregate = any(fn in sql_upper for fn in ('COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX('))

    if is_aggregate and n == 1:
        vals = ', '.join(f"{k}: {v}" for k, v in rows[0].items() if v not in (None, ''))
        return f"Result: {vals}" if vals else f"Found {n} result(s)."

    if n == 1:
        row = rows[0]
        if platform == 'youtube':
            name = row.get('channel_name') or row.get('channel_handle') or 'Unknown'
            subs = row.get('subscribers')
            niche = row.get('niche')
            bits = [f"*{name}*"]
            if subs:
                bits.append(f"{subs:,} subscribers" if isinstance(subs, (int, float)) else str(subs))
            if niche:
                bits.append(str(niche))
            return "Found 1 result: " + " · ".join(bits)
        if platform == 'linkedin':
            name = row.get('full_name') or row.get('profile_id') or 'Unknown'
            headline = row.get('headline')
            conns = row.get('connections')
            bits = [f"*{name}*"]
            if headline:
                bits.append(str(headline))
            if conns:
                bits.append(f"{conns:,} connections" if isinstance(conns, (int, float)) else str(conns))
            return "Found 1 result: " + " · ".join(bits)
        name = row.get('creator_name') or row.get('username') or 'Unknown'
        followers = row.get('followers')
        niche = row.get('niche')
        bits = [f"*{name}*"]
        if followers:
            bits.append(f"{followers:,} followers" if isinstance(followers, (int, float)) else str(followers))
        if niche:
            bits.append(str(niche))
        return "Found 1 result: " + " · ".join(bits)

    label = {'instagram': 'Instagram creators',
             'youtube': 'YouTube channels',
             'linkedin': 'LinkedIn profiles'}.get(platform, 'results')
    return f"Found {n} {label}."


async def execute_mcp_query(user_query: str, skip_insight: bool = False, platform: str = None) -> dict:
    """Direct SQL approach: LLM writes SQL, we validate & execute safely.

    Now supports all three tables: influencers, youtube_creators, linkedin_creators.

    Platform resolution:
      1. If `platform` arg is provided explicitly (e.g. after the bot asked the user),
         use it directly.
      2. Otherwise we detect from the user's text via `detect_platform`.
      3. If still ambiguous, return a `{'type': 'clarify'}` response so the caller
         can ask the user which platform they meant.
    """
    try:
        # ── Resolve platform ──
        resolved_platform = None
        if platform and platform.strip().lower() in ('instagram', 'youtube', 'linkedin'):
            resolved_platform = platform.strip().lower()
        else:
            resolved_platform = detect_platform(user_query)

        if not resolved_platform:
            return {
                'type': 'clarify',
                'message': "Which platform did you mean — Instagram, YouTube, or LinkedIn?",
                'reason': 'platform_ambiguous',
            }

        # ── Result cache: identical question within RESULT_CACHE_TTL ──
        cached = _result_cache_get(resolved_platform, user_query)
        if cached is not None:
            return cached

        # ── Ensure persistent SQLite DB for this platform only ──
        conn = _ensure_platform_db(resolved_platform)

        # Quick empty check
        schema = _PLATFORM_TO_SCHEMA[resolved_platform]
        cnt_row = conn.execute(f"SELECT COUNT(*) FROM {schema['table']}").fetchone()
        if not cnt_row or cnt_row[0] == 0:
            result = {
                "type": "data",
                "platform": resolved_platform,
                "data": [],
                "insight": f"No {resolved_platform} creators in the database yet.",
            }
            return result

        # ── Ask LLM to write SQL (only one LLM call per query now) ──
        client_llm = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=NVIDIA_KEY)
        hinted_query = f"Platform: {resolved_platform}. {user_query}"

        llm_response = client_llm.chat.completions.create(
            model="meta/llama-3.1-8b-instruct",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": hinted_query},
            ],
            temperature=0.0,
            max_tokens=256,
        )

        raw_sql = llm_response.choices[0].message.content.strip()
        raw_sql = raw_sql.replace('```sql', '').replace('```', '').strip()
        lines = raw_sql.split('\n')
        sql_lines = [l for l in lines if l.strip() and not l.strip().startswith('--') and not l.strip().startswith('#')]
        if sql_lines:
            raw_sql = ' '.join(sql_lines)

        raw_sql = _patch_sql_with_user_limit(raw_sql, user_query)
        logger.info(f"[MCP] Platform: {resolved_platform} | LLM SQL: {raw_sql}")

        is_safe, error_msg, cleaned_sql = validate_sql(raw_sql)
        if not is_safe:
            logger.warning(f"[MCP] SQL blocked: {error_msg} | Raw: {raw_sql}")
            return {"type": "error", "message": f"Query safety check failed: {error_msg}"}

        logger.info(f"[MCP] Executing SQL: {cleaned_sql}")

        # ── Execute using the stdlib cursor (no pandas) ──
        def _run_sql(sql: str):
            cur = conn.execute(sql)
            col_names = [d[0] for d in (cur.description or [])]
            rows = cur.fetchall()
            return col_names, rows

        try:
            col_names, rows = _run_sql(cleaned_sql)
        except sqlite3.Error as sql_err:
            logger.error(f"[MCP] SQL execution error: {sql_err} | SQL: {cleaned_sql}")
            try:
                fallback_sql = f"SELECT * FROM {schema['table']} LIMIT 50"
                col_names, rows = _run_sql(fallback_sql)
                cleaned_sql = fallback_sql
                logger.info(f"[MCP] Fell back to: {fallback_sql}")
            except Exception:
                return {"type": "error", "message": f"Query error: {sql_err}", "sql": cleaned_sql}

        data_rows = [
            {col: ('' if val is None else val) for col, val in zip(col_names, r)}
            for r in rows
        ]

        insight_text = ''
        if not skip_insight:
            insight_text = _build_insight(resolved_platform, data_rows, cleaned_sql)

        result = {
            "type": "data",
            "platform": resolved_platform,
            "sql": cleaned_sql,
            "data": data_rows,
            "insight": insight_text,
        }

        _result_cache_put(resolved_platform, user_query, result)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"AI Search Failed: {str(e)}")
