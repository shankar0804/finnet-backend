"""Per-post scraper service.

Given a single content URL (Instagram reel, YouTube video, or LinkedIn post)
this service returns a *normalized* dict of post-level metrics that a
campaign entry can consume directly.

Unlike the creator-level scrapers in `scraper_service.py` and
`youtube_scraper_service.py`, we only pull metrics for the ONE post the
user linked — no aggregation, no IQR smoothing — because the entry is
meant to capture the actual delivered post's performance.
"""

from __future__ import annotations

import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs

from services.scraper_service import _run_apify_actor

logger = logging.getLogger(__name__)

# Configurable actor ids — defaults work with the standard Apify Store.
# LinkedIn has no universally-reliable free actor, so it's behind an env
# switch that the operator wires up when they subscribe to one.
APIFY_IG_REEL_ACTOR = os.environ.get(
    "APIFY_IG_REEL_ACTOR", "apify/instagram-reel-scraper"
)
APIFY_YT_VIDEO_ACTOR = os.environ.get(
    "APIFY_YT_VIDEO_ACTOR", "streamers/youtube-scraper"
)
APIFY_LINKEDIN_POST_ACTOR = os.environ.get(
    "APIFY_LINKEDIN_POST_ACTOR", ""
)  # e.g. "apimaestro/linkedin-post-scraper" — set in env to enable


class UnsupportedPlatformError(ValueError):
    """URL doesn't match a supported platform (IG reel, YT video, LI post)."""


class PostNotFoundError(ValueError):
    """Scraper ran but returned no usable data for the given URL."""


# ═══════════════════════════════════════════════════════════
# Platform detection
# ═══════════════════════════════════════════════════════════

_IG_POST_PATH_RE = re.compile(r"/(reel|reels|p|tv)/([A-Za-z0-9_\-]+)", re.IGNORECASE)
_YT_WATCH_RE = re.compile(r"[?&]v=([A-Za-z0-9_\-]{6,})")
_YT_SHORT_RE = re.compile(r"/shorts/([A-Za-z0-9_\-]{6,})")
_YT_BE_RE = re.compile(r"youtu\.be/([A-Za-z0-9_\-]{6,})")
_LI_POST_RE = re.compile(
    r"linkedin\.com/(?:feed/update|posts|embed/feed/update|pulse)/([A-Za-z0-9_%\-:\.]+)",
    re.IGNORECASE,
)


def detect_platform(url: str) -> str:
    """Return 'instagram' | 'youtube' | 'linkedin' or raise UnsupportedPlatformError."""
    if not url or not isinstance(url, str):
        raise UnsupportedPlatformError("Empty URL")
    u = url.strip().lower()
    if "instagram.com" in u:
        return "instagram"
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "linkedin.com" in u:
        return "linkedin"
    raise UnsupportedPlatformError(
        f"URL does not look like Instagram, YouTube, or LinkedIn: {url}"
    )


def _canonical_ig_url(url: str) -> str:
    """Strip query string + tracking params from an IG post URL.

    Apify's instagram-reel-scraper silently returns nothing when it gets
    URLs like `.../p/<shortcode>/?hl=en` or `.../reel/<shortcode>/?igsh=...`.
    We rebuild the URL from just the shortcode so the actor never sees
    anything it might reject.
    """
    if not url:
        return url
    m = _IG_POST_PATH_RE.search(url)
    if not m:
        return url.split("?")[0].rstrip("/")
    kind, shortcode = m.group(1).lower(), m.group(2)
    # reel / reels / p / tv — keep the original path type so the actor
    # handles the right variant.
    if kind in ("reel", "reels"):
        path = "reel"
    elif kind == "tv":
        path = "tv"
    else:
        path = "p"
    return f"https://www.instagram.com/{path}/{shortcode}/"


def _canonical_yt_url(url: str) -> str:
    """Reduce a YouTube URL to the bare video form the actor accepts."""
    if not url:
        return url
    if m := _YT_SHORT_RE.search(url):
        return f"https://www.youtube.com/shorts/{m.group(1)}"
    if m := _YT_BE_RE.search(url):
        return f"https://www.youtube.com/watch?v={m.group(1)}"
    if m := _YT_WATCH_RE.search(url):
        return f"https://www.youtube.com/watch?v={m.group(1)}"
    return url.split("&")[0]


def _canonical_li_url(url: str) -> str:
    """Strip the tracking trail off a LinkedIn post URL."""
    if not url:
        return url
    return url.split("?")[0].rstrip("/")


# ═══════════════════════════════════════════════════════════
# Normalization helpers
# ═══════════════════════════════════════════════════════════


def _safe_int(val) -> int:
    try:
        if val is None or val == "":
            return 0
        return int(val)
    except (TypeError, ValueError):
        return 0


def _safe_float(val) -> float:
    try:
        if val is None or val == "":
            return 0.0
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _iso(ts) -> Optional[str]:
    if not ts:
        return None
    if isinstance(ts, str):
        return ts  # Apify usually gives an ISO string already
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _empty_result(platform: str, post_url: str) -> dict:
    return {
        "platform": platform,
        "username": "",
        "creator_name": "",
        "profile_link": "",
        "followers": 0,
        "post_url": post_url,
        "video_views": 0,
        "play_count": 0,
        "likes": 0,
        "comments": 0,
        "shares": 0,
        "saves": 0,
        "impressions": 0,
        "reacts": 0,
        "reshares": 0,
        "duration_secs": 0,
        "engagement_rate": 0.0,
        "post_timestamp": None,
        "deliverable_type": None,
        "data_source": "scrape",
    }


# ═══════════════════════════════════════════════════════════
# Instagram reel / post
# ═══════════════════════════════════════════════════════════


def _extract_ig_owner(item: dict) -> tuple:
    """Return (username, full_name) by scanning all known key variants."""
    # Flat keys used by apify/instagram-reel-scraper + apify/instagram-scraper
    owner = (
        item.get("ownerUsername")
        or item.get("username")
        or item.get("owner_username")
    )
    full_name = (
        item.get("ownerFullName")
        or item.get("fullName")
        or item.get("owner_full_name")
    )
    # Nested {"owner": {...}} shape
    if not owner:
        o = item.get("owner") or {}
        owner = o.get("username") or o.get("userName") or o.get("handle")
        full_name = full_name or o.get("full_name") or o.get("fullName") or o.get("name")
    # Fallback: parse the post URL itself — IG embeds the username before /reel/ sometimes,
    # and profile_url blobs are common.
    if not owner:
        for k in ("ownerProfileUrl", "profileUrl", "ownerUrl"):
            v = item.get(k)
            if isinstance(v, str):
                m = re.search(r"instagram\.com/([A-Za-z0-9_.]+)/?", v)
                if m:
                    owner = m.group(1)
                    break
    return (owner or "").lstrip("@"), (full_name or "")


def _fetch_instagram(url: str) -> dict:
    clean_url = _canonical_ig_url(url)
    logger.info(f"[PostScrape] Instagram: {url} -> {clean_url}")
    try:
        items = _run_apify_actor(
            APIFY_IG_REEL_ACTOR,
            {"directUrls": [clean_url], "resultsLimit": 1},
        )
    except Exception as e:
        logger.warning(f"[PostScrape] IG reel actor failed for {clean_url}: {e}")
        items = []

    logger.info(f"[PostScrape] IG reel actor returned {len(items)} item(s)")
    if not items:
        # The reel actor returns nothing for non-video posts (/p/ carousels, photos)
        # and for URLs it considers malformed. Fall back to the general scraper.
        logger.info("[PostScrape] Trying apify/instagram-scraper fallback")
        try:
            items = _run_apify_actor(
                "apify/instagram-scraper",
                {"directUrls": [clean_url], "resultsLimit": 1, "addParentData": False},
            )
        except Exception as e:
            logger.warning(f"[PostScrape] IG fallback scraper failed: {e}")
            items = []
        logger.info(f"[PostScrape] IG fallback returned {len(items)} item(s)")

    if not items:
        raise PostNotFoundError(
            f"Instagram scrapers returned no data for {clean_url} — "
            f"post may be private, deleted, or the actor is down."
        )

    item = items[0]
    owner, creator_name = _extract_ig_owner(item)
    if not owner:
        logger.warning(
            f"[PostScrape] IG item returned but no owner — keys were: {list(item.keys())[:15]}"
        )

    views = _safe_int(item.get("videoViewCount") or item.get("videoPlayCount"))
    plays = _safe_int(item.get("videoPlayCount") or item.get("videoViewCount"))
    likes = _safe_int(item.get("likesCount"))
    comments = _safe_int(item.get("commentsCount"))
    duration = _safe_int(item.get("videoDuration"))

    # IG reel scraper exposes followersCount on the owner blob sometimes.
    followers = _safe_int(
        item.get("ownerFollowersCount")
        or item.get("owner", {}).get("followers_count")
        or 0
    )

    eng_rate = 0.0
    if views > 0:
        eng_rate = round(((likes + comments) / views) * 100, 2)

    # Deliverable guess from product type
    product_type = (item.get("productType") or item.get("type") or "").lower()
    deliverable = None
    if product_type in ("clips", "reel"):
        deliverable = "Reel"
    elif product_type in ("igtv",):
        deliverable = "Video"
    elif product_type in ("feed", "carousel", "graphimage", "graphsidecar"):
        deliverable = "Post"

    result = _empty_result("instagram", url)
    result.update(
        {
            "username": owner,
            "creator_name": creator_name,
            "profile_link": f"https://instagram.com/{owner}" if owner else "",
            "followers": followers,
            "video_views": views,
            "play_count": plays,
            "likes": likes,
            "comments": comments,
            "duration_secs": duration,
            "engagement_rate": eng_rate,
            "post_timestamp": _iso(item.get("timestamp")),
            "deliverable_type": deliverable,
        }
    )
    return result


# ═══════════════════════════════════════════════════════════
# YouTube
# ═══════════════════════════════════════════════════════════


def _parse_yt_duration(raw) -> int:
    """Accept 'mm:ss', 'hh:mm:ss', or int seconds."""
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        parts = str(raw).strip().split(":")
        parts = [int(p) for p in parts]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 1:
            return parts[0]
    except (ValueError, TypeError):
        pass
    return 0


def _fetch_youtube(url: str) -> dict:
    clean_url = _canonical_yt_url(url)
    logger.info(f"[PostScrape] YouTube: {url} -> {clean_url}")
    items = _run_apify_actor(
        APIFY_YT_VIDEO_ACTOR,
        {
            "startUrls": [{"url": clean_url}],
            "maxResults": 1,
            "maxResultsShorts": 1,
            "maxResultStreams": 0,
        },
    )
    if not items:
        raise PostNotFoundError(f"YouTube scraper returned no data for {clean_url}")

    item = items[0]
    about = item.get("aboutChannelInfo") or {}

    # streamers/youtube-scraper often omits channelUsername for single-video
    # input. Fall back to parsing it out of channelUrl / channelHandle / input.url.
    channel_handle = (
        about.get("channelUsername")
        or item.get("channelUsername")
        or about.get("channelHandle")
        or item.get("channelHandle")
        or ""
    )
    if not channel_handle:
        for ref in (
            about.get("channelUrl"),
            item.get("channelUrl"),
            about.get("channelProfileUrl"),
            item.get("channelProfileUrl"),
        ):
            if not isinstance(ref, str):
                continue
            m = re.search(r"youtube\.com/@([A-Za-z0-9_.\-]+)", ref)
            if m:
                channel_handle = m.group(1)
                break
    channel_handle = channel_handle.lstrip("@")

    channel_name = about.get("channelName") or item.get("channelName") or ""
    channel_id = about.get("channelId") or item.get("channelId") or ""
    subscribers = _safe_int(
        about.get("numberOfSubscribers") or item.get("numberOfSubscribers")
    )

    if not channel_handle and not channel_id:
        logger.warning(
            f"[PostScrape] YT item missing channel id+handle — keys were: {list(item.keys())[:15]}"
        )

    views = _safe_int(item.get("viewCount"))
    likes = _safe_int(item.get("likes"))
    comments = _safe_int(item.get("commentsCount"))
    duration = _parse_yt_duration(item.get("duration"))

    eng_rate = 0.0
    if views > 0:
        eng_rate = round(((likes + comments) / views) * 100, 2)

    deliverable = "Video"
    # Shorts: YT flags these with isShort or duration <= 60
    if item.get("isShort") or (duration and duration <= 60 and "/shorts/" in url.lower()):
        deliverable = "Short"

    result = _empty_result("youtube", url)
    # Prefer the readable handle as username; only fall back to the raw
    # channel_id (UCxxx…) so downstream creator resolution still works even
    # when the actor didn't expose a handle.
    result.update(
        {
            "username": channel_handle or channel_id,
            "creator_name": channel_name,
            "profile_link": (
                f"https://www.youtube.com/@{channel_handle}"
                if channel_handle
                else (f"https://www.youtube.com/channel/{channel_id}" if channel_id else "")
            ),
            "followers": subscribers,
            "video_views": views,
            "play_count": views,  # YouTube only exposes one view count
            "likes": likes,
            "comments": comments,
            "duration_secs": duration,
            "engagement_rate": eng_rate,
            "post_timestamp": _iso(
                item.get("date") or item.get("uploadDate") or item.get("publishedAt")
            ),
            "deliverable_type": deliverable,
            # Carry channel_id so the entry builder can resolve youtube_creators row
            "_channel_id": channel_id,
        }
    )
    return result


# ═══════════════════════════════════════════════════════════
# LinkedIn
# ═══════════════════════════════════════════════════════════


def _extract_linkedin_profile_id(item: dict) -> str:
    """Best-effort extraction of the author's public identifier."""
    for key in ("authorPublicIdentifier", "authorUsername", "authorId"):
        if item.get(key):
            return str(item[key])
    author = item.get("author") or {}
    for key in ("publicIdentifier", "username", "handle", "id"):
        if author.get(key):
            return str(author[key])
    profile_url = (
        item.get("authorProfileUrl") or author.get("url") or author.get("profileUrl") or ""
    )
    m = re.search(r"linkedin\.com/in/([A-Za-z0-9_\-]+)", profile_url)
    if m:
        return m.group(1)
    return ""


def _fetch_linkedin(url: str) -> dict:
    if not APIFY_LINKEDIN_POST_ACTOR:
        raise UnsupportedPlatformError(
            "LinkedIn post scraping is not configured. "
            "Set APIFY_LINKEDIN_POST_ACTOR in env (e.g. apimaestro/linkedin-post-scraper) "
            "to enable."
        )
    clean_url = _canonical_li_url(url)
    logger.info(f"[PostScrape] LinkedIn: {url} -> {clean_url}")
    # Different LinkedIn actors use different input keys — try the two common ones.
    try:
        items = _run_apify_actor(
            APIFY_LINKEDIN_POST_ACTOR,
            {"postUrls": [clean_url]},
        )
    except Exception:
        items = _run_apify_actor(
            APIFY_LINKEDIN_POST_ACTOR,
            {"urls": [clean_url]},
        )
    if not items:
        raise PostNotFoundError(f"LinkedIn scraper returned no data for {url}")

    item = items[0]
    profile_id = _extract_linkedin_profile_id(item)
    full_name = (
        item.get("authorName")
        or (item.get("author") or {}).get("name")
        or (item.get("author") or {}).get("fullName")
        or ""
    )

    likes = _safe_int(
        item.get("numLikes") or item.get("totalReactionCount") or item.get("reactionsCount")
    )
    comments = _safe_int(item.get("numComments") or item.get("commentsCount"))
    shares = _safe_int(item.get("numShares") or item.get("sharesCount") or item.get("reposts"))
    impressions = _safe_int(item.get("impressionsCount") or item.get("views"))

    eng_rate = 0.0
    if impressions > 0:
        eng_rate = round(((likes + comments + shares) / impressions) * 100, 2)

    result = _empty_result("linkedin", url)
    result.update(
        {
            "username": profile_id,
            "creator_name": full_name,
            "profile_link": f"https://www.linkedin.com/in/{profile_id}/" if profile_id else "",
            "impressions": impressions,
            "reacts": likes,
            "comments": comments,
            "reshares": shares,
            "engagement_rate": eng_rate,
            "post_timestamp": _iso(item.get("postedAt") or item.get("date") or item.get("timestamp")),
            "deliverable_type": "Post",
        }
    )
    return result


# ═══════════════════════════════════════════════════════════
# Public entry point
# ═══════════════════════════════════════════════════════════


def fetch_post_data(url: str) -> dict:
    """Detect platform from URL and return a normalized post-metrics dict.

    Raises:
        UnsupportedPlatformError — URL doesn't match IG/YT/LinkedIn (or
            LinkedIn scraper isn't configured).
        PostNotFoundError — actor ran but returned nothing for this URL.
        ValueError — Apify token missing or other config issue.
    """
    platform = detect_platform(url)
    if platform == "instagram":
        return _fetch_instagram(url)
    if platform == "youtube":
        return _fetch_youtube(url)
    if platform == "linkedin":
        return _fetch_linkedin(url)
    raise UnsupportedPlatformError(platform)
