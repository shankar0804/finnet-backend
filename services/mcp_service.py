import json
import re
import time
import sqlite3
import logging
import pandas as pd
from openai import OpenAI
from database.db import supabase

logger = logging.getLogger(__name__)

# ── Data cache to avoid fetching from Supabase on every query ──
_cache = {"ig": None, "yt": None, "li": None, "timestamp": 0}
CACHE_TTL = 60  # seconds

import os

NVIDIA_KEY = os.environ.get("NVIDIA_KEY", "").strip()

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

**How to choose the table:**
- If the user says "Instagram", "IG", "reel", "followers" → use `influencers`
- If the user says "YouTube", "YT", "channel", "subscribers", "shorts", "long-form" → use `youtube_creators`
- If the user says "LinkedIn", "LI", "connections", "company", "headline" → use `linkedin_creators`
- If the user says "all creators" or "all platforms" or doesn't specify → use `influencers` (default)
- You can query ONLY ONE table per query. Do NOT join tables.
- The tables are linked by `creator_group_id` (same UUID = same person across platforms), but cross-table JOINs are NOT needed for most queries.

**Rules:**
1. Write ONLY a single SELECT statement. No explanations, no markdown, no code fences.
2. Use ONLY the columns listed above for the chosen table. Never invent columns.
3. **DEFAULT columns** for Instagram queries: `creator_name, username, profile_link, niche, followers, avg_views`
4. **DEFAULT columns** for YouTube queries: `channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate, avg_short_views, short_engagement_rate`
5. **DEFAULT columns** for LinkedIn queries: `full_name, profile_id, profile_link, headline, current_company, connections`
6. For text columns, empty/missing values may be NULL or empty string ''. Use: `(column IS NULL OR TRIM(column) = '')`
7. For numeric columns, missing data is NULL or 0. To find valid data: `column IS NOT NULL AND column > 0`
8. Use case-insensitive matching for text: `LOWER(column) LIKE LOWER('%value%')`
9. Do NOT add a LIMIT unless the user says "top N" or a specific number.
10. Output ONLY the raw SQL. Nothing else.

**Examples:**

User: "show all creators"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers

User: "youtube creators with more than 100k subscribers"
SELECT channel_name, channel_handle, profile_link, niche, subscribers, avg_long_views, long_engagement_rate FROM youtube_creators WHERE subscribers > 100000 ORDER BY subscribers DESC

User: "show me youtube shorts engagement rates"
SELECT channel_name, channel_handle, subscribers, avg_short_views, short_engagement_rate FROM youtube_creators WHERE short_engagement_rate IS NOT NULL AND short_engagement_rate > 0 ORDER BY short_engagement_rate DESC

User: "linkedin profiles in tech"
SELECT full_name, profile_id, profile_link, headline, current_company, industry, connections FROM linkedin_creators WHERE LOWER(industry) LIKE '%tech%' OR LOWER(headline) LIKE '%tech%'

User: "who has the most followers"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 0 ORDER BY followers DESC LIMIT 1

User: "beauty creators in mumbai"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE LOWER(niche) LIKE LOWER('%beauty%') AND LOWER(location) LIKE LOWER('%mumbai%')

User: "creators whose niche is blank"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE niche IS NULL OR TRIM(niche) = ''

User: "top 5 youtube channels by engagement"
SELECT channel_name, channel_handle, subscribers, avg_long_views, long_engagement_rate FROM youtube_creators WHERE long_engagement_rate IS NOT NULL AND long_engagement_rate > 0 ORDER BY long_engagement_rate DESC LIMIT 5

User: "count of creators by niche"
SELECT niche, COUNT(*) as count FROM influencers WHERE niche IS NOT NULL AND TRIM(niche) != '' GROUP BY niche ORDER BY count DESC

User: "show me all youtube data"
SELECT * FROM youtube_creators

User: "linkedin creators at google"
SELECT full_name, profile_id, profile_link, headline, current_company, connections FROM linkedin_creators WHERE LOWER(current_company) LIKE '%google%'"""


# ─── INSIGHT PROMPT ─────────────────────────────────────────────────

INSIGHT_SYSTEM = """You are a helpful data analyst for an influencer talent agency. The user asked a question and we queried the database. Your job is to DIRECTLY ANSWER their question based on the results.

Rules:
- Be specific and actionable. Name specific creators, numbers, and fields.
- Use <strong> for emphasis on key names and numbers.
- No code blocks, no markdown — just plain text with HTML bold tags.
- Keep it concise (2-5 sentences).
- If the data shows blank/null/empty fields, LIST them specifically (e.g. "Missing fields for @creator: niche, location, email").
- If 0 results, say so clearly and suggest what the user might try instead.
- NEVER say "the query returned no results" if data IS present. Analyze the actual data."""


async def execute_mcp_query(user_query: str, skip_insight: bool = False) -> dict:
    """Direct SQL approach: LLM writes SQL, we validate & execute safely.
    
    Now supports all three tables: influencers, youtube_creators, linkedin_creators.
    """
    try:
        # 1. Fetch data from all tables (with 60s cache)
        now = time.time()
        if _cache["ig"] is None or (now - _cache["timestamp"]) > CACHE_TTL:
            ig_resp = supabase.table("influencers").select("*").execute()
            yt_resp = supabase.table("youtube_creators").select("*").execute()
            li_resp = supabase.table("linkedin_creators").select("*").execute()
            _cache["ig"] = ig_resp.data or []
            _cache["yt"] = yt_resp.data or []
            _cache["li"] = li_resp.data or []
            _cache["timestamp"] = now

        ig_records = _cache["ig"]
        yt_records = _cache["yt"]
        li_records = _cache["li"]

        total = len(ig_records) + len(yt_records) + len(li_records)
        if total == 0:
            return {"type": "data", "data": [], "insight": "The database is currently empty across all platforms."}

        # Load all tables into in-memory SQLite
        conn = sqlite3.connect(':memory:')
        if ig_records:
            pd.DataFrame(ig_records).to_sql('influencers', conn, index=False)
        else:
            # Create empty table so queries don't fail
            pd.DataFrame(columns=list(SCHEMA_IG["columns"].keys())).to_sql('influencers', conn, index=False)

        if yt_records:
            pd.DataFrame(yt_records).to_sql('youtube_creators', conn, index=False)
        else:
            pd.DataFrame(columns=list(SCHEMA_YT["columns"].keys())).to_sql('youtube_creators', conn, index=False)

        if li_records:
            pd.DataFrame(li_records).to_sql('linkedin_creators', conn, index=False)
        else:
            pd.DataFrame(columns=list(SCHEMA_LI["columns"].keys())).to_sql('linkedin_creators', conn, index=False)

        # 2. Ask LLM to write SQL directly
        client_llm = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=NVIDIA_KEY)

        llm_response = client_llm.chat.completions.create(
            model="meta/llama-3.1-8b-instruct",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_query}
            ],
            temperature=0.0,
            max_tokens=512
        )

        raw_sql = llm_response.choices[0].message.content.strip()

        # Clean markdown code fences if the LLM wraps the output
        raw_sql = raw_sql.replace('```sql', '').replace('```', '').strip()
        # Remove any leading explanation text (take last SQL-looking line block)
        lines = raw_sql.split('\n')
        sql_lines = [l for l in lines if l.strip() and not l.strip().startswith('--') and not l.strip().startswith('#')]
        if sql_lines:
            raw_sql = ' '.join(sql_lines)

        logger.info(f"[MCP] LLM generated SQL: {raw_sql}")

        # 3. Validate SQL for safety
        is_safe, error_msg, cleaned_sql = validate_sql(raw_sql)
        if not is_safe:
            logger.warning(f"[MCP] SQL blocked: {error_msg} | Raw: {raw_sql}")
            return {
                "type": "error",
                "message": f"Query safety check failed: {error_msg}"
            }

        logger.info(f"[MCP] Executing SQL: {cleaned_sql}")

        # 4. Execute the validated SQL
        try:
            result_df = pd.read_sql_query(cleaned_sql, conn)
        except sqlite3.Error as sql_err:
            logger.error(f"[MCP] SQL execution error: {sql_err} | SQL: {cleaned_sql}")
            # Fallback: try a simple SELECT * query on the most likely table
            try:
                # Detect which table was intended
                sql_upper = cleaned_sql.upper()
                if 'YOUTUBE_CREATORS' in sql_upper:
                    fallback_sql = "SELECT * FROM youtube_creators LIMIT 50"
                elif 'LINKEDIN_CREATORS' in sql_upper:
                    fallback_sql = "SELECT * FROM linkedin_creators LIMIT 50"
                else:
                    fallback_sql = "SELECT * FROM influencers LIMIT 50"
                result_df = pd.read_sql_query(fallback_sql, conn)
                cleaned_sql = fallback_sql
                logger.info(f"[MCP] Fell back to: {fallback_sql}")
            except:
                return {"type": "error", "message": f"Query error: {sql_err}", "sql": cleaned_sql}

        # 5. Generate insight — smart answer to the user's question
        insight_text = f"Found {len(result_df)} result(s)."
        '''
        try:
            sample_size = min(30, len(result_df))
            sample_data = result_df.head(sample_size).to_json(orient='records')

            insight_prompt = [
                {"role": "system", "content": INSIGHT_SYSTEM},
                {"role": "user", "content": f"User's question: \"{user_query}\"\n\nSQL used: {cleaned_sql}\n\nQuery returned {len(result_df)} rows. Data:\n{sample_data}"}
            ]

            insight_response = client_llm.chat.completions.create(
                model="meta/llama-3.1-8b-instruct",
                messages=insight_prompt,
                temperature=0.3,
                max_tokens=500
            )
            insight_text = insight_response.choices[0].message.content.strip()
        except Exception as insight_err:
            logger.error(f"[MCP] Insight generation error: {insight_err}")
            insight_text = f"Found {len(result_df)} result(s)."
        '''

        return {
            "type": "data",
            "sql": cleaned_sql,
            "data": result_df.fillna('').to_dict(orient="records"),
            "insight": insight_text
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"AI Search Failed: {str(e)}")
