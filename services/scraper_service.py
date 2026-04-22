import os
import time
import json
import math
import statistics
import requests
import urllib3
import logging
import base64 as b64
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

APIFY_TOKEN = os.environ.get('APIFY_TOKEN', '')

# Minimum number of reels with view data needed to produce meaningful metrics
MIN_REELS_FOR_METRICS = 3


class InsufficientDataError(Exception):
    """Raised when there is not enough reel/video data to calculate meaningful metrics.
    The caller should NOT save zeros to the database when this is raised."""
    pass


# ═══════════════════════════════════════════════════════════
# Shared numeric helpers (also used by youtube_scraper_service)
# ═══════════════════════════════════════════════════════════

# Preference order for Instagram view/play count fields. Apify returns a mix
# depending on whether the data came from the profile scraper or the reel
# scraper, and some reels only have one of the two populated.
_IG_VIEW_FIELDS = ("videoViewCount", "videoPlayCount", "playsCount", "viewCount")


def _pick_view_count(item: dict, fields: tuple = _IG_VIEW_FIELDS) -> int:
    """Return the first positive view/play count available on the item.

    Falling back across fields avoids silently dropping reels that only have
    `videoPlayCount` populated (common with the profile scraper)."""
    for key in fields:
        val = item.get(key)
        if val is not None:
            try:
                n = int(val)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                continue
    return 0


def _robust_mean(values: list) -> float:
    """Return a robust mean of positive view counts.

    Strategy:
      * n >= 6  → IQR outlier removal using proper linear-interpolation
                  quartiles (statistics.quantiles, n=4, inclusive method).
      * 3 <= n < 6 → trim the single highest and single lowest (classic
                  trimmed mean — IQR is unreliable with so few points).
      * n < 3   → plain arithmetic mean.

    Final sanity check: if the arithmetic mean is more than 2× the median,
    a heavy-tail outlier survived the trimming — fall back to the median
    so one viral video can't double the reported average."""
    vals = []
    for v in values:
        try:
            x = float(v)
            if x > 0:
                vals.append(x)
        except (TypeError, ValueError):
            continue

    n = len(vals)
    if n == 0:
        return 0.0

    if n >= 6:
        try:
            q1, _med, q3 = statistics.quantiles(vals, n=4, method="inclusive")
            iqr = q3 - q1
            lo = q1 - 1.5 * iqr
            hi = q3 + 1.5 * iqr
            trimmed = [v for v in vals if lo <= v <= hi]
        except statistics.StatisticsError:
            trimmed = vals
    elif n >= 3:
        s = sorted(vals)
        trimmed = s[1:-1]
    else:
        trimmed = vals

    if not trimmed:
        trimmed = vals

    mean = sum(trimmed) / len(trimmed)
    median = statistics.median(vals)
    # Heavy-tail guard: if mean is still more than 2× the median, one
    # outlier survived IQR. Prefer the median — it's far more stable.
    if median > 0 and mean > 2 * median:
        return float(median)
    return mean


def _round_to_sig_figs(value: float, sig_figs: int = 3) -> int:
    """Round a positive number to `sig_figs` significant figures.

    Keeps the answer honest while still being display-friendly:
        127_432   → 127_000  (vs. old 150_000)
        1_540_217 → 1_540_000
        27_400    → 27_400
        8_340     → 8_340
    """
    if value is None or value <= 0:
        return 0
    try:
        magnitude = math.floor(math.log10(value))
        factor = 10 ** (magnitude - sig_figs + 1)
        return int(round(value / factor) * factor)
    except (ValueError, OverflowError):
        return int(round(value))


# ═══════════════════════════════════════════════════════════
# Shared Metrics Calculator
# ═══════════════════════════════════════════════════════════

def _calculate_metrics(target_reels: list, source: str = "profile") -> dict:
    """Calculate avg_views, engagement_rate, avg_video_length from a list of reel dicts.

    Works with both profile scraper and reel scraper output formats.
    The reel scraper returns `videoDuration` directly, while the profile scraper
    encodes it in the videoUrl's efg parameter.

    Returns dict with: avg_views, engagement_rate, avg_video_length
    """

    # --- Avg Views (robust mean + 3-sig-fig rounding) ---
    # Gather per-reel view counts, falling back across field names so reels
    # that only have `videoPlayCount` populated are not dropped.
    views = [_pick_view_count(r) for r in target_reels]
    views = [v for v in views if v > 0]

    avg_views_raw = _robust_mean(views)
    avg_views = _round_to_sig_figs(avg_views_raw, sig_figs=3)

    # --- Engagement Rate ---
    # ER = (likes + comments) / views * 100, weighted by the *actual* views
    # on each reel (not the trimmed mean). Uses the same fallback view field
    # so we don't divide by zero when only playCount is available.
    total_views = sum(views)
    total_likes = sum((r.get("likesCount") or 0) for r in target_reels)
    total_comments = sum((r.get("commentsCount") or 0) for r in target_reels)
    engagement_rate = 0.0
    if total_views > 0:
        engagement_rate = round(((total_likes + total_comments) / total_views) * 100, 2)

    # --- Average Video Length (seconds) ---
    durations = []
    for r in target_reels:
        # Reel scraper returns videoDuration directly
        dur = r.get("videoDuration")
        if dur and isinstance(dur, (int, float)) and dur > 0:
            durations.append(int(dur))
            continue

        # Profile scraper fallback: decode from videoUrl's efg param
        vid_url = r.get("videoUrl", "")
        if not vid_url:
            continue
        try:
            parsed = urlparse(vid_url)
            efg_vals = parse_qs(parsed.query).get("efg", [])
            if efg_vals:
                efg_json = json.loads(b64.b64decode(efg_vals[0] + "==").decode("utf-8", errors="ignore"))
                efg_dur = efg_json.get("duration_s")
                if efg_dur and isinstance(efg_dur, (int, float)) and efg_dur > 0:
                    durations.append(int(efg_dur))
        except Exception:
            pass

    # Outlier removal for durations too
    if len(durations) > 2:
        durations_sorted = sorted(durations)
        valid_durations = durations_sorted[1:-1]
    else:
        valid_durations = durations
    avg_video_length = round(sum(valid_durations) / len(valid_durations)) if valid_durations else 0

    return {
        "avg_views": int(avg_views),
        "engagement_rate": engagement_rate,
        "avg_video_length": avg_video_length,
        "_views_count": len(views),  # internal: how many reels had view data
    }


# ═══════════════════════════════════════════════════════════
# Apify Actor Runner (shared helper)
# ═══════════════════════════════════════════════════════════

def _run_apify_actor(actor_id: str, run_input: dict) -> list:
    """Start an Apify actor, poll for completion, and return the dataset items."""
    # Apify API expects actor IDs with ~ separator in URLs (e.g. "streamers~youtube-channel-scraper")
    api_actor_id = actor_id.replace("/", "~")
    start_url = f"https://api.apify.com/v2/acts/{api_actor_id}/runs?token={APIFY_TOKEN}"

    resp = requests.post(start_url, json=run_input, verify=False)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to start Apify actor {actor_id}: {resp.text}")

    run_data = resp.json().get("data", {})
    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")

    # Poll for completion. 2s interval is a good balance: short enough that
    # we pick up completion quickly (saves ~2s per actor run vs 3s), but
    # long enough that we don't hammer Apify's API.
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    while True:
        time.sleep(2)
        status_resp = requests.get(status_url, verify=False)
        status = status_resp.json().get("data", {}).get("status")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            if status != "SUCCEEDED":
                raise Exception(f"Apify run ({actor_id}) finished with status: {status}")
            break

    # Retrieve results
    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"
    dataset_resp = requests.get(dataset_url, verify=False)
    return dataset_resp.json()


# ═══════════════════════════════════════════════════════════
# Reel Scraper Fallback
# ═══════════════════════════════════════════════════════════

def _fetch_reels_fallback(username: str) -> list:
    """Use the dedicated Apify Instagram Reel Scraper to fetch recent reels.
    This is called when the profile scraper doesn't return enough video data.
    Returns a list of reel dicts compatible with _calculate_metrics().
    """
    logger.info(f"[Service] Profile scraper had insufficient reels for @{username}, "
                f"falling back to dedicated reel scraper...")

    run_input = {
        "directUrls": [f"https://www.instagram.com/{username}/"],
        "resultsLimit": 12,
    }

    items = _run_apify_actor("apify~instagram-reel-scraper", run_input)

    if not items:
        logger.warning(f"[Service] Reel scraper returned no results for @{username}")
        return []

    # The reel scraper returns items with: videoViewCount, videoPlayCount,
    # likesCount, commentsCount, videoDuration, timestamp, etc.
    # Sort by timestamp (newest first), skip the newest (incomplete data), take up to 10
    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    if len(items) > 1:
        items = items[1:]  # Skip newest reel (may have incomplete engagement data)

    target_reels = items[:10]
    logger.info(f"[Service] Reel scraper returned {len(target_reels)} usable reels for @{username}")
    return target_reels


# ═══════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════

def fetch_influencer_data(username: str) -> dict:
    """Scrapes raw data from Instagram via Apify and generates a formatted Influencer representation.

    Flow:
    1. Run profile scraper → get profile info + latest posts
    2. Filter posts for video reels
    3. If < MIN_REELS_FOR_METRICS usable reels → fallback to dedicated reel scraper
    4. If still not enough data → raise InsufficientDataError (caller must NOT save to DB)
    """
    if not APIFY_TOKEN:
        raise ValueError("Apify API token not configured.")

    logger.info(f"[Service] Starting Apify REST profile scraper for {username}...")

    # 1. Run profile scraper
    run_input = {"usernames": [username], "resultsLimit": 20}
    items = _run_apify_actor("apify~instagram-profile-scraper", run_input)

    if not items:
        raise ValueError("No profile data found for this user.")

    profile_data = items[0]

    # Guard: Apify sometimes returns profile shell with no actual data
    if profile_data.get("followersCount") is None and profile_data.get("postsCount") is None:
        raise ValueError(
            f"Instagram returned an empty profile for @{username}. "
            "This usually means Instagram blocked the scrape. Please try again in a minute."
        )

    # 2. Isolate reels from profile data: filter to videos/clips, exclude pinned
    latest_posts = profile_data.get("latestPosts", [])
    if not latest_posts and "latestIgtvVideos" in profile_data:
        latest_posts = profile_data.get("latestIgtvVideos", [])

    reels = [
        p for p in latest_posts
        if (p.get("type", "").lower() == "video" or p.get("productType", "").lower() in ["clips", "igtv"])
        and not p.get("isPinned", False)
    ]

    reels.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    if len(reels) > 0:
        reels = reels[1:]  # Exclude newest (incomplete data)

    target_reels = reels[:10]

    # 3. Check if we have enough reels — if not, fallback to reel scraper.
    # Use the same field-fallback logic as the metric calculator, otherwise
    # we'd miss reels where only videoPlayCount is populated.
    usable_views = [_pick_view_count(r) for r in target_reels]
    usable_views = [v for v in usable_views if v > 0]
    data_source = "profile"

    if len(usable_views) < MIN_REELS_FOR_METRICS:
        logger.warning(
            f"[Service] Only {len(usable_views)} reels with view data from profile scraper "
            f"(need {MIN_REELS_FOR_METRICS}). Trying reel scraper fallback..."
        )
        fallback_reels = _fetch_reels_fallback(username)
        if fallback_reels:
            target_reels = fallback_reels
            data_source = "reel_scraper"

    # 4. Calculate metrics
    metrics = _calculate_metrics(target_reels, source=data_source)

    # 5. Final check: if still not enough data, refuse to save zeros
    if metrics["_views_count"] < MIN_REELS_FOR_METRICS:
        raise InsufficientDataError(
            f"Not enough reel data available for @{username} to calculate meaningful metrics. "
            f"Only found {metrics['_views_count']} reel(s) with view data "
            f"(minimum {MIN_REELS_FOR_METRICS} required). "
            f"Creator was NOT added to the database."
        )

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "username": profile_data.get("username", username),
        "creator_name": profile_data.get("fullName", username),
        "profile_link": f"https://instagram.com/{profile_data.get('username', username)}",
        "platform": "Instagram",
        "followers": profile_data.get("followersCount", 0),
        "avg_views": metrics["avg_views"],
        "engagement_rate": metrics["engagement_rate"],
        "avg_video_length": metrics["avg_video_length"],
        # Timestamps
        "last_scraped_at": now_iso,
    }
