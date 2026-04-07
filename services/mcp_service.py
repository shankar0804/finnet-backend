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
_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 60  # seconds

import os

NVIDIA_KEY = os.environ.get("NVIDIA_KEY", "").strip()

# Schema definition - single source of truth
SCHEMA = {
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
        "last_scraped_at": "text",
        "last_ocr_at": "text",
        "last_manual_at": "text",
    }
}

VALID_COLUMNS = set(SCHEMA["columns"].keys())

# Columns with types for the prompt
COLUMNS_WITH_TYPES = "\n".join(f"  - {col} ({dtype})" for col, dtype in SCHEMA["columns"].items())


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

    # Layer 3: Only allow the 'influencers' table
    from_matches = re.findall(r'\bFROM\s+(\w+)', sql_upper)
    join_matches = re.findall(r'\bJOIN\s+(\w+)', sql_upper)
    all_tables = from_matches + join_matches
    for table in all_tables:
        if table != 'INFLUENCERS':
            return False, f"Blocked: unauthorized table '{table}'", ""

    # Layer 4: Ensure at least one valid table is referenced
    if 'INFLUENCERS' not in sql_upper:
        return False, "Query must reference the 'influencers' table", ""

    # Layer 5: Enforce a safety LIMIT to prevent huge result sets
    if 'LIMIT' not in sql_upper:
        cleaned += " LIMIT 200"

    # Layer 6: Block subqueries to other tables
    subquery_froms = re.findall(r'\(\s*SELECT[^)]*FROM\s+(\w+)', sql_upper)
    for table in subquery_froms:
        if table != 'INFLUENCERS':
            return False, f"Blocked: subquery references unauthorized table '{table}'", ""

    return True, "OK", cleaned


# ─── SYSTEM PROMPT FOR DIRECT SQL GENERATION ────────────────────────

SYSTEM_PROMPT = f"""You are an expert SQLite query writer for an influencer talent agency database.

**Your job:** Convert the user's natural language question into a SINGLE valid SQLite SELECT query.

**Database: SQLite (in-memory)**
**Table: `influencers`**
**Columns:**
{COLUMNS_WITH_TYPES}

**Rules:**
1. Write ONLY a single SELECT statement. No explanations, no markdown, no code fences.
2. Use ONLY the columns listed above. Never invent columns.
3. The table name is `influencers` — use ONLY this table.
4. **DEFAULT columns** for most queries: `creator_name, username, profile_link, niche, followers, avg_views`. Only add extra columns if the user explicitly asks about them.
5. Use `SELECT *` ONLY when the user asks about ALL data, missing fields, or analytical deep-dives.
6. For text columns, empty/missing values may be NULL or empty string ''. To check for missing data, use: `(column IS NULL OR TRIM(column) = '')`
7. For numeric columns (followers, avg_views, engagement_rate, avg_video_length), missing data is NULL or 0. To find valid data: `column IS NOT NULL AND column > 0`
8. Use case-insensitive matching for text: `LOWER(column) LIKE LOWER('%value%')`
9. Do NOT add a LIMIT unless the user says "top N" or a specific number. Return all matching results by default.
10. For "lowest"/"least"/"minimum" → ORDER BY column ASC LIMIT 1 (filter out NULLs/0s for numeric columns)
11. For "highest"/"most"/"maximum"/"top" → ORDER BY column DESC LIMIT 1 (or N)
12. For analytical questions like "what fields are blank/missing for each creator", SELECT all columns and return all rows.
13. Output ONLY the raw SQL. Nothing else.

**Examples:**

User: "show all creators"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers

User: "give me all creators with minimum 100k avg views"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE avg_views IS NOT NULL AND avg_views >= 100000 ORDER BY avg_views DESC

User: "who has the most followers"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 0 ORDER BY followers DESC LIMIT 1

User: "lowest follower count person"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers IS NOT NULL AND followers > 0 ORDER BY followers ASC LIMIT 1

User: "beauty creators in mumbai"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE LOWER(niche) LIKE LOWER('%beauty%') AND LOWER(location) LIKE LOWER('%mumbai%')

User: "what data is missing for each creator"
SELECT * FROM influencers

User: "creators whose niche is blank"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE niche IS NULL OR TRIM(niche) = ''

User: "top 5 by engagement rate"
SELECT creator_name, username, profile_link, niche, followers, avg_views, engagement_rate FROM influencers WHERE engagement_rate IS NOT NULL AND engagement_rate > 0 ORDER BY engagement_rate DESC LIMIT 5

User: "who has no email"
SELECT creator_name, username, profile_link, niche, followers, avg_views, mail_id FROM influencers WHERE mail_id IS NULL OR TRIM(mail_id) = ''

User: "count of creators by niche"
SELECT niche, COUNT(*) as count FROM influencers WHERE niche IS NOT NULL AND TRIM(niche) != '' GROUP BY niche ORDER BY count DESC

User: "average followers"
SELECT AVG(followers) as avg_followers, MIN(followers) as min_followers, MAX(followers) as max_followers FROM influencers WHERE followers IS NOT NULL AND followers > 0 LIMIT 1

User: "creators with more than 1 million followers"
SELECT creator_name, username, profile_link, niche, followers, avg_views FROM influencers WHERE followers > 1000000 ORDER BY followers DESC

User: "show me engagement rate for all creators"
SELECT creator_name, username, niche, followers, avg_views, engagement_rate FROM influencers WHERE engagement_rate IS NOT NULL AND engagement_rate > 0 ORDER BY engagement_rate DESC"""


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
    """Direct SQL approach: LLM writes SQL, we validate & execute safely."""
    try:
        # 1. Fetch data (with 60s cache to avoid repeated Supabase round-trips)
        now = time.time()
        if _cache["data"] is None or (now - _cache["timestamp"]) > CACHE_TTL:
            response = supabase.table("influencers").select("*").execute()
            _cache["data"] = response.data
            _cache["timestamp"] = now

        db_records = _cache["data"]
        if not db_records:
            return {"type": "data", "data": [], "insight": "The database is currently empty."}

        # Load into in-memory SQLite for querying
        df = pd.DataFrame(db_records)
        conn = sqlite3.connect(':memory:')
        df.to_sql('influencers', conn, index=False)

        # 2. Ask LLM to write SQL directly
        client_llm = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=NVIDIA_KEY)

        llm_response = client_llm.chat.completions.create(
            model="meta/llama-3.3-70b-instruct",
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
            # Fallback: try a simple SELECT * query
            try:
                fallback_sql = "SELECT * FROM influencers LIMIT 50"
                result_df = pd.read_sql_query(fallback_sql, conn)
                cleaned_sql = fallback_sql
                logger.info("[MCP] Fell back to SELECT * FROM influencers")
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
