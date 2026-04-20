"""Campaign entry builder.

Given some combination of:
  * a scraped post dict (from post_scraper_service.fetch_post_data), and/or
  * an OCR result dict  (from ocr_service.run_post_ocr_pipeline), and
  * form overrides     (amount, deliverable_type, poc, notes, delivery_date),

this service:
  1. Merges the two data sources (scrape wins, OCR fills gaps).
  2. Resolves the creator from the right platform's table (influencers /
     youtube_creators / linkedin_creators).
  3. If the creator is missing and we have enough scraped data, auto-stubs
     a minimal row so the entry isn't orphaned.
  4. Reverse-updates the creator row with any fresh data (followers,
     avd, skip_rate, etc.) that the scrape / OCR surfaced.
  5. Inserts into campaign_entries.

The function returns a status dict so bulk callers can aggregate:
  {status: 'created' | 'skipped' | 'failed', entry?, reason?, missing_creator?}
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from database.db import supabase

logger = logging.getLogger(__name__)

# DB-level deliverable_type CHECK constraint only allows these.
_ALLOWED_DELIVERABLES = {"Reel", "Story", "Post", "Video", "Other"}
_DELIVERABLE_ALIASES = {
    "short": "Reel",      # YouTube Shorts map to Reel
    "shorts": "Reel",
    "reel": "Reel",
    "reels": "Reel",
    "story": "Story",
    "post": "Post",
    "video": "Video",
    "igtv": "Video",
}


# ═══════════════════════════════════════════════════════════
# 1. Merge scrape + OCR
# ═══════════════════════════════════════════════════════════

# Integer metric fields — if scrape has > 0, keep it; else accept OCR.
_METRIC_INT_FIELDS = (
    "video_views",
    "play_count",
    "likes",
    "comments",
    "shares",
    "saves",
    "impressions",
    "reacts",
    "reshares",
    "duration_secs",
    "followers",
)

# String fields — scrape wins when non-empty.
_METRIC_STR_FIELDS = ("username", "creator_name", "profile_link", "post_timestamp", "deliverable_type")


def merge_scrape_and_ocr(scraped: Optional[dict], ocr: Optional[dict]) -> dict:
    """Combine a scrape dict and an OCR-result dict into a single normalized dict.

    Precedence: scrape > OCR. OCR only fills fields where scrape returned
    zero / empty. Unknown keys from either source are preserved.
    """
    merged = dict(scraped or {})
    if not ocr:
        return merged

    for k in _METRIC_INT_FIELDS:
        if not merged.get(k):
            v = ocr.get(k)
            if v:
                merged[k] = v

    for k in _METRIC_STR_FIELDS:
        if not merged.get(k):
            v = ocr.get(k)
            if v:
                merged[k] = v

    # OCR-only extras: AVD / CTR / skip_rate / reach — pass through so the
    # reverse-update step can push them onto the creator row.
    for k in ("avd", "ctr", "skip_rate", "reach"):
        if ocr.get(k):
            merged[k] = ocr[k]

    # Recompute engagement_rate if it wasn't set but we now have views+likes+comments.
    if not merged.get("engagement_rate"):
        v = merged.get("video_views") or merged.get("impressions") or 0
        eng = (merged.get("likes") or 0) + (merged.get("comments") or 0) + (merged.get("shares") or 0)
        if v > 0:
            merged["engagement_rate"] = round((eng / v) * 100, 2)
    return merged


# ═══════════════════════════════════════════════════════════
# 2. Creator resolution
# ═══════════════════════════════════════════════════════════


def _lookup_creator(platform: str, username: str, channel_id: str = "") -> Optional[dict]:
    """Fetch existing creator row for this platform, or None."""
    if not username and not channel_id:
        return None
    try:
        if platform == "instagram":
            r = supabase.table("influencers").select("*").eq("username", username).execute()
            return r.data[0] if r.data else None
        if platform == "youtube":
            # Try handle first, then channel_id
            r = supabase.table("youtube_creators").select("*").eq("channel_handle", username).execute()
            if r.data:
                return r.data[0]
            if channel_id:
                r = supabase.table("youtube_creators").select("*").eq("channel_id", channel_id).execute()
                return r.data[0] if r.data else None
            return None
        if platform == "linkedin":
            r = supabase.table("linkedin_creators").select("*").eq("profile_id", username).execute()
            return r.data[0] if r.data else None
    except Exception as e:
        logger.warning(f"[ENTRY] creator lookup failed ({platform}/{username}): {e}")
    return None


def _auto_stub_creator(platform: str, scraped: dict) -> Optional[dict]:
    """Create a creator row when the post scrape surfaced a new creator.

    For Instagram / YouTube we run the FULL creator-level scraper so the
    row ends up with proper followers/subs, avg views, engagement rate,
    avg video length, etc. — not just the one-post slice. If the full
    scrape fails (insufficient public data / actor error) we fall back
    to a minimal stub from the post-level data.

    Required for the fallback stub: username (or channel_id for YT) +
    a follower/subscriber count. Returns the inserted row or None if
    we genuinely don't have enough to seed a row.
    """
    username = (scraped.get("username") or "").lstrip("@").strip()
    followers = int(scraped.get("followers") or 0)
    channel_id = scraped.get("_channel_id") or ""
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        if platform == "instagram":
            if not username:
                return None
            # Run the real profile+reel scraper so the new row gets filled
            # with avg_views / engagement_rate / avg_video_length etc.
            try:
                from services.scraper_service import (
                    fetch_influencer_data,
                    InsufficientDataError,
                )
                full = fetch_influencer_data(username)
                full["platform"] = full.get("platform") or "Instagram"
                r = supabase.table("influencers").insert(full).execute()
                logger.info(f"[ENTRY] Auto-stubbed IG @{username} via full profile scrape")
                return r.data[0] if r.data else full
            except InsufficientDataError as e:
                logger.warning(f"[ENTRY] IG full-scrape insufficient for @{username}: {e}")
            except Exception as e:
                logger.warning(f"[ENTRY] IG full-scrape errored for @{username}: {e}")

            # Minimal fallback stub
            if followers <= 0:
                return None
            row = {
                "username": username,
                "creator_name": scraped.get("creator_name") or username,
                "profile_link": scraped.get("profile_link") or f"https://instagram.com/{username}",
                "platform": "Instagram",
                "followers": followers,
                "avg_views": scraped.get("video_views") or 0,
                "engagement_rate": scraped.get("engagement_rate") or 0.0,
                "avg_video_length": scraped.get("duration_secs") or 0,
                "last_scraped_at": now_iso,
            }
            r = supabase.table("influencers").insert(row).execute()
            return r.data[0] if r.data else row

        if platform == "youtube":
            if not username and not channel_id:
                return None
            # Run the full YouTube channel scraper (long-form + shorts) so
            # we get subs, total_videos, avg long/short views, ER, etc.
            try:
                from services.youtube_scraper_service import fetch_youtube_data
                from services.scraper_service import InsufficientDataError
                lookup = username or channel_id
                full = fetch_youtube_data(lookup)
                r = supabase.table("youtube_creators").insert(full).execute()
                logger.info(
                    f"[ENTRY] Auto-stubbed YT {full.get('channel_handle') or full.get('channel_id')} "
                    f"via full channel scrape"
                )
                return r.data[0] if r.data else full
            except InsufficientDataError as e:
                logger.warning(f"[ENTRY] YT full-scrape insufficient for {username or channel_id}: {e}")
            except Exception as e:
                logger.warning(f"[ENTRY] YT full-scrape errored for {username or channel_id}: {e}")

            # Minimal fallback stub
            if followers <= 0:
                return None
            row = {
                "channel_id": channel_id or username,
                "channel_handle": username,
                "channel_name": scraped.get("creator_name") or username,
                "profile_link": scraped.get("profile_link") or "",
                "subscribers": followers,
                "last_scraped_at": now_iso,
            }
            r = supabase.table("youtube_creators").insert(row).execute()
            return r.data[0] if r.data else row

        if platform == "linkedin":
            # LinkedIn posts rarely expose follower count on the post object —
            # the rule allows auto-stub when we at least have a username.
            if not username:
                return None
            row = {
                "profile_id": username,
                "full_name": scraped.get("creator_name") or username,
                "profile_link": scraped.get("profile_link") or f"https://www.linkedin.com/in/{username}/",
                "connections": followers if followers > 0 else 0,
                "last_scraped_at": now_iso,
            }
            r = supabase.table("linkedin_creators").insert(row).execute()
            return r.data[0] if r.data else row
    except Exception as e:
        logger.warning(f"[ENTRY] auto-stub failed ({platform}/{username}): {e}")
    return None


# ═══════════════════════════════════════════════════════════
# 3. Reverse-update creator with fresh scrape/OCR data
# ═══════════════════════════════════════════════════════════


def _reverse_update_creator(platform: str, creator: dict, merged: dict) -> list:
    """Backfill fields on the creator row that are currently empty/zero.

    Returns the list of keys we actually updated (for logging / audit)."""
    if not creator:
        return []

    updates: dict = {}

    def _needs(field: str, new_val) -> bool:
        if new_val in (None, "", 0, 0.0):
            return False
        cur = creator.get(field)
        # Fill if current is empty/0/null
        return cur in (None, "", 0, 0.0)

    # Common numeric: followers / subscribers / connections
    if platform == "instagram":
        if _needs("followers", merged.get("followers")):
            updates["followers"] = merged["followers"]
        if _needs("avg_views", merged.get("video_views")):
            updates["avg_views"] = merged["video_views"]
        if _needs("engagement_rate", merged.get("engagement_rate")):
            updates["engagement_rate"] = merged["engagement_rate"]
        if _needs("avg_video_length", merged.get("duration_secs")):
            updates["avg_video_length"] = merged["duration_secs"]
        # OCR-only analytic fields
        if _needs("avd", merged.get("avd")):
            updates["avd"] = merged["avd"]
        if _needs("skip_rate", merged.get("skip_rate")):
            updates["skip_rate"] = merged["skip_rate"]
    elif platform == "youtube":
        if _needs("subscribers", merged.get("followers")):
            updates["subscribers"] = merged["followers"]
        if _needs("avd", merged.get("avd")):
            updates["avd"] = merged["avd"]
        if _needs("skip_rate", merged.get("skip_rate")):
            updates["skip_rate"] = merged["skip_rate"]
    elif platform == "linkedin":
        if _needs("connections", merged.get("followers")):
            updates["connections"] = merged["followers"]

    if not updates:
        return []

    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        updates["last_scraped_at"] = now_iso
        if platform == "instagram":
            supabase.table("influencers").update(updates).eq("username", creator["username"]).execute()
        elif platform == "youtube":
            key = "channel_id" if creator.get("channel_id") else "channel_handle"
            supabase.table("youtube_creators").update(updates).eq(key, creator[key]).execute()
        elif platform == "linkedin":
            supabase.table("linkedin_creators").update(updates).eq("profile_id", creator["profile_id"]).execute()
        return [k for k in updates.keys() if k != "last_scraped_at"]
    except Exception as e:
        logger.warning(f"[ENTRY] reverse-update failed ({platform}): {e}")
        return []


# ═══════════════════════════════════════════════════════════
# 4. Main orchestrator
# ═══════════════════════════════════════════════════════════


def _normalize_deliverable(raw) -> str:
    if not raw:
        return "Reel"
    v = str(raw).strip()
    if v in _ALLOWED_DELIVERABLES:
        return v
    return _DELIVERABLE_ALIASES.get(v.lower(), "Other")


def build_entry(
    campaign_id: str,
    scraped: Optional[dict] = None,
    ocr: Optional[dict] = None,
    overrides: Optional[dict] = None,
    platform_hint: Optional[str] = None,
) -> dict:
    """Compose a campaign_entries row and insert it.

    Args:
        campaign_id: target campaign UUID.
        scraped: dict from post_scraper_service.fetch_post_data (may be None).
        ocr: dict from ocr_service.run_post_ocr_pipeline['result'] (may be None).
        overrides: user-provided fields (amount, deliverable_type, poc, notes,
                   delivery_date, content_link, creator_username).
        platform_hint: when scrape+ocr both miss platform, the caller can hint.

    Returns:
        {"status": "created", "entry": {...}, "updated_creator": [fields...]}
        {"status": "skipped", "reason": "...", "missing_creator": "..."}
        {"status": "failed",  "reason": "..."}
    """
    overrides = overrides or {}
    merged = merge_scrape_and_ocr(scraped, ocr)

    # Username can come from scrape, OCR, or the form (last-resort fallback).
    username = (
        (merged.get("username") or "").lstrip("@").strip()
        or (overrides.get("creator_username") or "").lstrip("@").strip()
    )
    if not username:
        return {
            "status": "skipped",
            "reason": "Could not determine creator username from link, screenshot, or form.",
        }

    platform = (merged.get("platform") or platform_hint or "instagram").lower()

    # Put username back into merged so stub-creator and others can use it uniformly.
    merged["username"] = username

    # Resolve creator
    creator = _lookup_creator(platform, username, merged.get("_channel_id", ""))
    if not creator:
        creator = _auto_stub_creator(platform, merged)
        if not creator:
            return {
                "status": "skipped",
                "reason": (
                    f"Creator @{username} is not in the {platform} database and the "
                    f"{'link' if scraped else 'screenshot'} didn't include enough data "
                    f"(followers/subscribers) to auto-add them. Please add the creator first."
                ),
                "missing_creator": username,
                "platform": platform,
            }

    # Reverse-update creator with fresh fields
    updated_fields = _reverse_update_creator(platform, creator, merged)

    # Compose campaign_entries row
    now_iso = datetime.now(timezone.utc).isoformat()
    deliverable = _normalize_deliverable(
        overrides.get("deliverable_type") or merged.get("deliverable_type")
    )

    row = {
        "campaign_id": campaign_id,
        "creator_username": username,
        "platform": platform,
        "deliverable_type": deliverable,
        "status": overrides.get("status", "pending"),
        "content_link": overrides.get("content_link") or merged.get("post_url") or "",
        "notes": overrides.get("notes", ""),
        "amount": overrides.get("amount", 0) or 0,
        "delivery_date": overrides.get("delivery_date") or None,
        "poc": overrides.get("poc", ""),
        # Scrape + OCR metrics
        "video_views": merged.get("video_views", 0) or 0,
        "play_count": merged.get("play_count", 0) or 0,
        "likes": merged.get("likes", 0) or 0,
        "comments": merged.get("comments", 0) or 0,
        "shares": merged.get("shares", 0) or 0,
        "saves": merged.get("saves", 0) or 0,
        "impressions": merged.get("impressions", 0) or 0,
        "reacts": merged.get("reacts", 0) or 0,
        "reshares": merged.get("reshares", 0) or 0,
        "duration_secs": merged.get("duration_secs", 0) or 0,
        "engagement_rate": merged.get("engagement_rate", 0.0) or 0.0,
        "post_timestamp": merged.get("post_timestamp") or None,
        "data_source": "scrape" if scraped else ("ocr" if ocr else "manual"),
        "last_enriched_at": now_iso if (scraped or ocr) else None,
    }

    # post_timestamp may be a free-text string from OCR (e.g. "2 weeks ago").
    # Postgres will reject that on a TIMESTAMPTZ column, so we only send ISO
    # strings through. Anything else is dropped (the UI can't show it anyway).
    pt = row.get("post_timestamp")
    if pt and not _looks_like_iso(pt):
        row["post_timestamp"] = None

    try:
        resp = supabase.table("campaign_entries").insert(row).execute()
    except Exception as e:
        return {"status": "failed", "reason": f"DB insert failed: {e}"}

    entry = resp.data[0] if resp.data else row
    return {
        "status": "created",
        "entry": entry,
        "updated_creator_fields": updated_fields,
        "platform": platform,
    }


def _looks_like_iso(s: str) -> bool:
    """Cheap check — we accept anything fromisoformat can parse."""
    if not isinstance(s, str):
        return False
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False
