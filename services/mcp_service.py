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
VALID_OPS = {"=", "!=", ">", "<", ">=", "<=", "like", "is_null", "is_not_null"}


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

        # Handle null/blank checks
        if op == "is_null":
            conditions.append(f"({col} IS NULL OR TRIM({col}) = '')")
            continue
        elif op == "is_not_null":
            conditions.append(f"({col} IS NOT NULL AND TRIM({col}) != '')")
            continue

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


SYSTEM_PROMPT = f"""You are an expert database query translator for an influencer talent agency. Convert ANY user question into a precise JSON filter specification that will be used to query a SQL database.

**Database table: `influencers`**
**Available columns with types:**
{json.dumps(SCHEMA["columns"], indent=2)}

**Available filter operators:**
- `=` — exact match (for text, becomes case-insensitive LIKE)
- `!=` — not equal
- `>`, `<`, `>=`, `<=` — numeric comparisons
- `like` — partial text match (case-insensitive)
- `is_null` — column is empty, blank, or missing (no value needed)
- `is_not_null` — column has a non-empty value (no value needed)

**Output format (JSON only, NO markdown, NO explanation):**
{{
  "columns": ["*"] or ["col1", "col2", ...],
  "filters": [
    {{"column": "exact_column_name", "op": "operator", "value": "value or null for is_null/is_not_null"}}
  ],
  "sort": {{"column": "column_name", "direction": "ASC|DESC"}} or null,
  "limit": number
}}

**Critical Rules:**
1. Use ONLY column names from the schema above. Never invent columns.
2. For text searches use `like`. For numbers use numeric operators.
3. For questions about missing/blank/empty data, use `is_null`. For questions about filled/complete data, use `is_not_null`.
4. Select ONLY the columns relevant to the question. If the user asks about specific fields (niche, location, etc.), include those columns.
5. When the user asks about "blank" or "missing" or "empty" fields for creators, filter WHERE those specific columns `is_null`.
6. When the user asks "what data is missing" without specifying a field, select ALL columns so the answer can show which are blank.
7. Think carefully about what the user is ACTUALLY asking. Translate their intent into the right filters.
8. Default limit is 50. Use a smaller limit only if the user asks for a specific number (e.g. "top 5").
9. Output ONLY valid JSON. No text before or after."""


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

        # 6. Generate insight — always provide a smart answer to the user's question
        insight_text = f"Found {len(result_df)} result(s)."
        try:
            # Show more data to the LLM for better analysis
            sample_data = result_df.head(20).to_json(orient='records')
            
            insight_prompt = [
                {"role": "system", "content": "You are a helpful data analyst for an influencer agency. The user asked a question and we queried the database. Your job is to DIRECTLY ANSWER their question based on the results. Be specific and actionable. Use <strong> for emphasis. No code blocks, no markdown — just plain text with HTML bold tags. Keep it concise (2-4 sentences max)."},
                {"role": "user", "content": f"User's question: \"{user_query}\"\n\nQuery returned {len(result_df)} rows. Data sample:\n{sample_data}"}
            ]

            insight_response = client_llm.chat.completions.create(
                model="meta/llama-3.1-8b-instruct",
                messages=insight_prompt,
                temperature=0.3,
                max_tokens=300
            )
            insight_text = insight_response.choices[0].message.content.strip()
        except Exception as insight_err:
            # Non-fatal — fall back to basic count
            insight_text = f"Found {len(result_df)} result(s)."

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
