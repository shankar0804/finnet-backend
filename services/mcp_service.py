import json
import time
import sqlite3
import pandas as pd
from openai import OpenAI
from database.db import supabase

import os

# ── Data cache to avoid fetching from Supabase on every query ──
_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 60  # seconds

NVIDIA_KEY = os.environ.get("NVIDIA_KEY", "")

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
VALID_OPS = {"=", "!=", ">", "<", ">=", "<=", "like"}


def build_sql_from_spec(spec: dict) -> str:
    """Deterministically builds a safe SQL query from a validated JSON spec."""
    columns = spec.get("columns", ["*"])
    if columns == ["*"] or not columns:
        col_str = "*"
    else:
        safe_cols = [c for c in columns if c in VALID_COLUMNS]
        col_str = ", ".join(safe_cols) if safe_cols else "*"

    sql = f"SELECT {col_str} FROM influencers"

    # Build WHERE clause
    filters = spec.get("filters", [])
    conditions = []
    for f in filters:
        col = f.get("column", "")
        op = f.get("op", "=").lower()
        val = f.get("value")

        if col not in VALID_COLUMNS or op not in VALID_OPS:
            continue

        col_type = SCHEMA["columns"][col]

        if op == "like":
            conditions.append(f"LOWER({col}) LIKE LOWER('%{val}%')")
        elif col_type in ("integer", "real"):
            try:
                num_val = float(val) if col_type == "real" else int(val)
                conditions.append(f"{col} {op} {num_val}")
            except (ValueError, TypeError):
                conditions.append(f"LOWER({col}) LIKE LOWER('%{val}%')")
        else:
            safe_val = str(val).replace("'", "''")
            if op == "=":
                conditions.append(f"LOWER({col}) LIKE LOWER('%{safe_val}%')")
            else:
                conditions.append(f"{col} {op} '{safe_val}'")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    # ORDER BY
    sort = spec.get("sort")
    if sort:
        sort_col = sort.get("column", "")
        sort_dir = "DESC" if sort.get("direction", "").upper() == "DESC" else "ASC"
        if sort_col in VALID_COLUMNS:
            sql += f" ORDER BY {sort_col} {sort_dir}"

    # LIMIT
    limit = spec.get("limit")
    if limit and isinstance(limit, int) and 0 < limit <= 500:
        sql += f" LIMIT {limit}"
    else:
        sql += " LIMIT 50"

    return sql


SYSTEM_PROMPT = f"""You are a database query translator. Convert user questions into a JSON filter specification.

**Database table: `influencers`**
**Available columns (use ONLY these exact names):**
{json.dumps(SCHEMA["columns"], indent=2)}

**Output format (JSON only, no markdown, no explanation):**
{{
  "columns": ["*"] or ["column1", "column2"],
  "filters": [
    {{"column": "exact_column_name", "op": "=|!=|>|<|>=|<=|like", "value": "value"}}
  ],
  "sort": {{"column": "column_name", "direction": "ASC|DESC"}},
  "limit": 50
}}

**Rules:**
- For text searches, always use op "like"
- For numbers (followers, following_count, avg_views, posts_count, engagement_rate), use numeric operators
- Output ONLY the JSON object, nothing else

**Examples:**
User: "Show me Harish's data"
{{"columns": ["*"], "filters": [{{"column": "creator_name", "op": "like", "value": "harish"}}], "limit": 50}}

User: "Find creators with more than 1M followers"
{{"columns": ["username", "creator_name", "followers", "category"], "filters": [{{"column": "followers", "op": ">", "value": 1000000}}], "sort": {{"column": "followers", "direction": "DESC"}}, "limit": 50}}

User: "Who has following less than 600"
{{"columns": ["username", "creator_name", "followers", "following_count"], "filters": [{{"column": "following_count", "op": "<", "value": 600}}], "limit": 50}}

User: "Show all tech creators"
{{"columns": ["*"], "filters": [{{"column": "category", "op": "like", "value": "tech"}}], "limit": 50}}

User: "Top 5 by engagement rate"
{{"columns": ["username", "creator_name", "followers", "engagement_rate"], "filters": [], "sort": {{"column": "engagement_rate", "direction": "DESC"}}, "limit": 5}}

User: "Show everyone"
{{"columns": ["*"], "filters": [], "limit": 50}}"""


async def execute_mcp_query(user_query: str, skip_insight: bool = False) -> dict:
    """Structured JSON approach: LLM outputs a filter spec, Python builds SQL deterministically."""
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

        df = pd.DataFrame(db_records)
        conn = sqlite3.connect(':memory:')
        df.to_sql('influencers', conn, index=False)

        # 2. Ask LLM to produce structured JSON (NOT raw SQL)
        client_llm = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=NVIDIA_KEY)

        llm_response = client_llm.chat.completions.create(
            model="meta/llama-3.1-8b-instruct",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_query}
            ],
            temperature=0.0,
            max_tokens=256
        )

        raw_output = llm_response.choices[0].message.content.strip()

        # 3. Parse the JSON spec (with fallback)
        try:
            raw_output = raw_output.replace('```json', '').replace('```', '').strip()
            spec = json.loads(raw_output)
        except json.JSONDecodeError:
            return {
                "type": "error",
                "message": f"AI returned invalid format. Raw: {raw_output[:200]}"
            }

        # 4. Build SQL deterministically from the validated spec
        sql = build_sql_from_spec(spec)

        # 5. Execute
        try:
            result_df = pd.read_sql_query(sql, conn)
        except sqlite3.Error as sql_err:
            return {"type": "error", "message": f"Query error: {sql_err}", "sql": sql}

        # 6. Generate insight (skip for WhatsApp bot — it formats its own reply)
        insight_text = f"Found {len(result_df)} result(s)."
        if not skip_insight:
            insight_prompt = [
                {"role": "system", "content": "You are a data analyst. Write 1-2 sentences summarizing the query results. Use <strong> for emphasis. No code blocks, no markdown, just plain text with HTML bold tags."},
                {"role": "user", "content": f"Question: {user_query}\nResults ({len(result_df)} rows): {result_df.head(10).to_json(orient='records')}"}
            ]

            insight_response = client_llm.chat.completions.create(
                model="meta/llama-3.1-8b-instruct",
                messages=insight_prompt,
                temperature=0.3,
                max_tokens=128
            )
            insight_text = insight_response.choices[0].message.content.strip()

        return {
            "type": "data",
            "sql": sql,
            "data": result_df.fillna('').to_dict(orient="records"),
            "insight": insight_text
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"AI Search Failed: {str(e)}")
