"""
100-case test harness for the WhatsApp-bot / MCP query pipeline.

What this covers (pure logic, no LLM / network):

  1. _extract_limit_from_query        — 40 natural-language phrasings
  2. detect_platform                  — 20 phrasings
  3. validate_sql (safety)            — 25 cases (good + malicious)
  4. _patch_sql_with_user_limit       — 15 integration cases

Run:
    cd backend
    python test_query_pipeline.py

Exit code 0 when all pass, non-zero if anything fails.

Do NOT depend on Supabase / NVIDIA / network — keep it deterministic
so it can run in CI.
"""
from __future__ import annotations

import os
import sys
import traceback
from typing import Any, Callable

# Ensure we can import the service even without env vars set.
os.environ.setdefault("NVIDIA_KEY", "test-key")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

from services.mcp_service import (
    _extract_limit_from_query,
    _patch_sql_with_user_limit,
    detect_platform,
    validate_sql,
)

# ────────────────────────────────────────────────────────────────────
# Test infrastructure
# ────────────────────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

TOTAL = 0
PASSED = 0
FAILED: list[tuple[str, str, Any, Any]] = []


def run(section: str, name: str, fn: Callable[[], tuple[Any, Any]]) -> None:
    """fn returns (expected, actual); test passes iff expected == actual."""
    global TOTAL, PASSED
    TOTAL += 1
    try:
        expected, actual = fn()
        if expected == actual:
            PASSED += 1
        else:
            FAILED.append((section, name, expected, actual))
    except Exception as e:  # pragma: no cover
        FAILED.append((section, name, "<no exception>", f"{type(e).__name__}: {e}"))
        traceback.print_exc()


# ────────────────────────────────────────────────────────────────────
# 1. LIMIT extraction — 40 cases
# ────────────────────────────────────────────────────────────────────

LIMIT_CASES: list[tuple[str, int | None]] = [
    # Classic "top N / first N / only N"
    ("top 3 finance creators",                                            3),
    ("first 5 creators",                                                  5),
    ("only 10 creators",                                                 10),
    ("latest 20 profiles",                                               20),
    ("last 50 channels",                                                 50),

    # Classic "give/show me N"
    ("give me 3 creators",                                                3),
    ("show me 5 channels",                                                5),
    ("list 7 influencers",                                                7),
    ("find 4 creators",                                                   4),
    ("get me 12 profiles",                                               12),

    # The reported failure family — "give me a list of N …"
    ("give me a list of 10 finance creators",                            10),
    ("give me a list  of 10 finance creators with over 5% engagement",   10),
    ("give me a list of 10 finance creators with over 5% engagement rate and delhi as city 1", 10),
    ("show me a list of 15 beauty influencers in mumbai",                15),
    ("give me a set of 8 finance channels",                               8),
    ("show me a batch of 20 finance creators",                           20),
    ("give me a bunch of 6 profiles",                                     6),
    ("list me a handful of 5 creators",                                   5),
    ("find me a few 3 creators",                                          3),
    ("give me the top 10 finance creators",                              10),

    # Number-adjective-noun (the other broken family)
    ("10 creators",                                                      10),
    ("10 finance creators",                                              10),
    ("10 top beauty creators",                                           10),
    ("5 top finance channels",                                            5),
    ("20 indian beauty profiles",                                        20),
    ("3 hindi finance influencers",                                       3),
    ("50 results",                                                       50),

    # Number-word phrasings
    ("give me three creators",                                            3),
    ("show me five channels",                                             5),
    ("top ten creators",                                                 10),
    ("first seven profiles",                                              7),

    # Negative cases — no explicit limit
    ("show all creators",                                              None),
    ("beauty creators in mumbai",                                      None),
    ("who has the most followers",                                     None),
    ("engagement rate over 5%",                                        None),
    ("finance creators with over 5 percent engagement",                None),
    ("creators in delhi",                                              None),

    # Edge cases — shouldn't misread bare numbers as counts
    ("creators over 500k",                                             None),  # 500k is threshold, not LIMIT
    ("engagement above 5",                                             None),
    ("profiles under 100",                                             None),

    # Extreme value safety — should cap
    ("10000 creators",                                                 None),  # over 500 cap
]

for text, expected in LIMIT_CASES:
    # Bind vars via default args to avoid late-binding pitfalls.
    def _case(_t=text, _e=expected):
        return (_e, _extract_limit_from_query(_t))
    run("LIMIT", text[:80], _case)


# ────────────────────────────────────────────────────────────────────
# 2. Platform detection — 20 cases
# ────────────────────────────────────────────────────────────────────

PLATFORM_CASES: list[tuple[str, str | None]] = [
    ("finance creators on instagram",                    "instagram"),
    ("show me IG profiles",                              "instagram"),
    ("who has the most followers on insta",              "instagram"),
    ("reels performance stats",                          "instagram"),
    ("influencers with avg views over 100k",             "instagram"),

    ("top youtube channels",                             "youtube"),
    ("show yt creators with most subscribers",           "youtube"),
    ("channel with most subs",                           "youtube"),
    ("youtube shorts engagement",                        "youtube"),
    ("creators with watch time over 10 min",             "youtube"),

    ("linkedin profiles in tech",                        "linkedin"),
    ("li connections above 5000",                        "linkedin"),
    ("professionals at google",                          "linkedin"),
    ("headline includes founder",                        "linkedin"),
    ("top 5 companies by employees",                     "linkedin"),

    # Ambiguous / none
    ("show me all creators",                             None),
    ("creators in mumbai",                               None),
    ("highest engagement rate",                          None),
    ("beauty niche",                                     None),
    ("delhi audience top city",                          None),
]

for text, expected in PLATFORM_CASES:
    def _case(_t=text, _e=expected):
        return (_e, detect_platform(_t))
    run("PLATFORM", text[:80], _case)


# ────────────────────────────────────────────────────────────────────
# 3. SQL safety validator — 25 cases
# ────────────────────────────────────────────────────────────────────

# Each case: (label, sql, expected_is_safe, optional_expected_limit_appended)
SQL_CASES: list[tuple[str, str, bool]] = [
    # Good queries
    ("select all IG",           "SELECT * FROM influencers",                                       True),
    ("select YT",               "SELECT channel_name, subscribers FROM youtube_creators LIMIT 10", True),
    ("select LI",               "SELECT full_name FROM linkedin_creators WHERE connections > 1000",True),
    ("select with where",       "SELECT creator_name FROM influencers WHERE followers > 500000",   True),
    ("count",                   "SELECT COUNT(*) FROM influencers",                                True),
    ("group by",                "SELECT niche, COUNT(*) FROM influencers GROUP BY niche",          True),
    ("order by + limit",        "SELECT * FROM influencers ORDER BY followers DESC LIMIT 5",       True),
    ("inline LIMIT survives",   "SELECT * FROM influencers LIMIT 10",                              True),

    # Malicious — must all be blocked
    ("drop table",              "DROP TABLE influencers",                                          False),
    ("delete",                  "DELETE FROM influencers",                                         False),
    ("update",                  "UPDATE influencers SET followers = 0",                            False),
    ("insert",                  "INSERT INTO influencers VALUES (1)",                              False),
    ("alter",                   "ALTER TABLE influencers ADD COLUMN hack TEXT",                    False),
    ("create",                  "CREATE TABLE hack (id TEXT)",                                     False),
    ("truncate",                "TRUNCATE influencers",                                            False),
    ("pragma",                  "PRAGMA table_info(influencers)",                                  False),
    ("attach",                  "ATTACH DATABASE 'x.db' AS x",                                     False),

    # Chained / tricky
    ("stacked drop",            "SELECT 1; DROP TABLE influencers",                                False),
    # Defense-in-depth: validator blocks dangerous keywords even inside comments.
    # The comment itself is harmless but parsing SQL comments in a regex is fragile,
    # so we prefer to reject outright. This is an INTENTIONAL false-positive.
    ("drop in comment blocked", "SELECT 1 -- DROP TABLE influencers",                              False),
    ("union unauthorized",      "SELECT * FROM influencers UNION SELECT * FROM sqlite_master",     False),
    ("unauthorized table",      "SELECT * FROM users",                                             False),
    ("subquery unauth",         "SELECT * FROM influencers WHERE id IN (SELECT id FROM users)",    False),
    ("no table",                "SELECT 1+1",                                                      False),
    ("empty",                   "",                                                                False),
    ("not select",              "WITH x AS (SELECT 1) SELECT * FROM x",                            False),
]

for label, sql, expected_safe in SQL_CASES:
    def _case(_s=sql, _e=expected_safe):
        ok, _msg, _cleaned = validate_sql(_s)
        return (_e, ok)
    run("VALIDATE_SQL", label, _case)

# Safety LIMIT enforcement — specific expectation:
def _safety_limit_auto_added():
    _ok, _msg, cleaned = validate_sql("SELECT * FROM influencers")
    return (True, "LIMIT 200" in cleaned.upper())
run("VALIDATE_SQL", "auto LIMIT 200 when absent", _safety_limit_auto_added)

def _safety_limit_kept():
    _ok, _msg, cleaned = validate_sql("SELECT * FROM influencers LIMIT 7")
    return (True, "LIMIT 7" in cleaned.upper() and "LIMIT 200" not in cleaned.upper())
run("VALIDATE_SQL", "user LIMIT preserved", _safety_limit_kept)


# ────────────────────────────────────────────────────────────────────
# 4. _patch_sql_with_user_limit (integration) — 15 cases
# ────────────────────────────────────────────────────────────────────

PATCH_CASES: list[tuple[str, str, str, str]] = [
    # (label, nl_query, llm_sql, must_contain_substring)
    ("inject when missing",
     "give me 3 finance creators",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%finance%'",
     "LIMIT 3"),
    ("list of N — our bug",
     "give me a list of 10 finance creators with over 5% engagement rate and delhi as city 1",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%finance%'",
     "LIMIT 10"),
    ("top N",
     "top 5 beauty creators",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%beauty%' ORDER BY followers DESC",
     "LIMIT 5"),
    ("N adjective creators",
     "10 finance creators",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%finance%'",
     "LIMIT 10"),
    ("number words",
     "give me three creators",
     "SELECT * FROM influencers",
     "LIMIT 3"),
    ("do not override existing",
     "give me 5 creators",
     "SELECT * FROM influencers LIMIT 20",
     "LIMIT 20"),                                         # existing LIMIT preserved
    ("do not inject when none asked",
     "show all creators",
     "SELECT * FROM influencers",
     "SELECT * FROM influencers"),                        # unchanged — no LIMIT added here
    ("strip trailing semicolon",
     "give me 3 creators",
     "SELECT * FROM influencers;",
     "LIMIT 3"),
    ("phrases — a list",
     "show me a list of 15 creators",
     "SELECT * FROM influencers",
     "LIMIT 15"),
    ("phrases — a batch",
     "find me a batch of 20 profiles",
     "SELECT * FROM influencers",
     "LIMIT 20"),
    ("phrases — a set",
     "give me a set of 8 channels",
     "SELECT * FROM youtube_creators",
     "LIMIT 8"),
    ("phrases — a bunch",
     "show me a bunch of 6 profiles",
     "SELECT * FROM influencers",
     "LIMIT 6"),
    ("phrases — the top N",
     "give me the top 10 finance creators",
     "SELECT * FROM influencers",
     "LIMIT 10"),
    ("no limit for vague",
     "beauty creators in mumbai",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%beauty%'",
     "SELECT * FROM influencers WHERE LOWER(niche) LIKE '%beauty%'"),
    ("no limit for threshold",
     "creators over 500k",
     "SELECT * FROM influencers WHERE followers > 500000",
     "SELECT * FROM influencers WHERE followers > 500000"),
]

for label, nl, sql, must in PATCH_CASES:
    def _case(_nl=nl, _sql=sql, _must=must):
        out = _patch_sql_with_user_limit(_sql, _nl)
        return (True, _must.upper() in out.upper())
    run("PATCH_LIMIT", label, _case)


# ────────────────────────────────────────────────────────────────────
# Report
# ────────────────────────────────────────────────────────────────────

def main() -> int:
    by_section: dict[str, list[tuple[str, Any, Any]]] = {}
    for sec, name, exp, act in FAILED:
        by_section.setdefault(sec, []).append((name, exp, act))

    print()
    print("=" * 64)
    print(f"  NL -> SQL pipeline test harness")
    print("=" * 64)
    print(f"  Total : {TOTAL}")
    print(f"  {GREEN}Pass  : {PASSED}{RESET}")
    print(f"  {RED if FAILED else GREEN}Fail  : {len(FAILED)}{RESET}")
    print()

    if FAILED:
        for sec, items in by_section.items():
            print(f"{YELLOW}-- {sec} failures --{RESET}")
            for name, exp, act in items:
                print(f"  {RED}[X] {name}{RESET}")
                print(f"      expected: {exp!r}")
                print(f"      actual  : {act!r}")
            print()
        return 1
    print(f"{GREEN}All {TOTAL} checks passed.{RESET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
