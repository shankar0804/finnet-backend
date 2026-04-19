"""YouTube Creator Scraper Service.

Uses TWO dedicated Apify actors for complete data:
1. streamers/youtube-scraper — full video data with views, likes, comments, duration, subs
2. streamers/youtube-shorts-scraper — full shorts data with views, likes, comments, duration

This gives us proper engagement rate calculation (likes+comments / views).
"""

import os
import re
import logging
from datetime import datetime, timezone
from services.scraper_service import _run_apify_actor, InsufficientDataError

logger = logging.getLogger(__name__)

APIFY_TOKEN = os.environ.get('APIFY_TOKEN', '')
MIN_VIDEOS_FOR_METRICS = 3


def _extract_channel_handle(url_or_handle: str) -> str:
    """Normalize input to a channel handle (without @)."""
    url_or_handle = url_or_handle.strip().rstrip('/')

    # Extract @handle from URL
    match = re.search(r'youtube\.com/@([^/\s?]+)', url_or_handle)
    if match:
        return match.group(1)

    # Already a handle
    if url_or_handle.startswith('@'):
        return url_or_handle[1:]

    # Channel ID — can't convert to handle, return as-is
    if url_or_handle.startswith('UC') and len(url_or_handle) == 24:
        return url_or_handle

    # Remove youtube.com prefix if present
    url_or_handle = re.sub(r'https?://(www\.)?youtube\.com/', '', url_or_handle)
    url_or_handle = url_or_handle.strip('/')

    return url_or_handle


def _parse_duration_to_seconds(duration_str) -> int:
    """Parse 'HH:MM:SS' or 'MM:SS' or 'SS' to seconds."""
    if not duration_str:
        return 0
    try:
        return int(float(duration_str))
    except (ValueError, TypeError):
        pass

    parts = str(duration_str).split(':')
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 1:
            return int(parts[0])
    except (ValueError, TypeError):
        pass
    return 0


def _calculate_video_metrics(videos: list) -> dict:
    """Calculate avg views, engagement rate, avg duration from a list of video items.

    Each video item should have: viewCount, likes, commentsCount, duration.
    Engagement Rate = (likes + comments) / views * 100

    Uses IQR outlier removal on views for avg_views calculation.
    """
    # Filter to videos with view data
    valid = [v for v in videos if (v.get('viewCount') or 0) > 0]
    views_count = len(valid)

    if views_count == 0:
        return {'avg_views': 0, 'engagement_rate': 0.0, 'avg_video_length': 0, '_views_count': 0}

    # ── Avg Views with IQR ──
    views = sorted([v['viewCount'] for v in valid])
    n = len(views)

    if n >= 4:
        q1 = views[n // 4]
        q3 = views[(3 * n) // 4]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        filtered_views = [v for v in views if lower <= v <= upper]
        if not filtered_views:
            filtered_views = views
    elif n > 2:
        filtered_views = views[1:-1]
    else:
        filtered_views = views

    avg_views = int(sum(filtered_views) / len(filtered_views)) if filtered_views else 0

    # ── Engagement Rate ──
    total_engagements = 0
    total_views_for_er = 0
    for v in valid:
        vw = v.get('viewCount', 0) or 0
        likes = v.get('likes', 0) or 0
        comments = v.get('commentsCount', 0) or 0
        total_engagements += likes + comments
        total_views_for_er += vw

    engagement_rate = round((total_engagements / total_views_for_er) * 100, 2) if total_views_for_er > 0 else 0.0

    # ── Avg Duration ──
    durations = [_parse_duration_to_seconds(v.get('duration')) for v in valid]
    durations = [d for d in durations if d > 0]
    avg_duration = int(sum(durations) / len(durations)) if durations else 0

    return {
        'avg_views': avg_views,
        'engagement_rate': engagement_rate,
        'avg_video_length': avg_duration,
        '_views_count': views_count,
    }


def fetch_youtube_data(channel_input: str) -> dict:
    """Scrapes YouTube channel using two actors: one for videos, one for shorts.

    Returns dict with channel metadata + separate long-form and Shorts metrics.
    """
    if not APIFY_TOKEN:
        raise ValueError("Apify API token not configured.")

    handle = _extract_channel_handle(channel_input)
    channel_url = f"https://www.youtube.com/@{handle}"
    logger.info(f"[YouTube] Starting scrape for: {channel_url} (handle: {handle})")

    # ─── Step 1: Fetch long-form videos via streamers/youtube-scraper ───
    long_input = {
        "startUrls": [{"url": channel_url}],
        "maxResults": 15,
        "maxResultsShorts": 0,
        "maxResultStreams": 0,
        "sortVideosBy": "NEWEST",
    }

    logger.info("[YouTube] Fetching long-form videos...")
    long_items = _run_apify_actor("streamers/youtube-scraper", long_input)
    logger.info(f"[YouTube] Long-form: got {len(long_items)} items")

    # ─── Step 2: Fetch shorts via streamers/youtube-shorts-scraper ───
    shorts_input = {
        "channels": [handle],
        "maxResultsShorts": 15,
    }

    logger.info("[YouTube] Fetching Shorts...")
    try:
        short_items = _run_apify_actor("streamers/youtube-shorts-scraper", shorts_input)
        logger.info(f"[YouTube] Shorts: got {len(short_items)} items")
    except Exception as e:
        logger.warning(f"[YouTube] Shorts scraper failed (channel may have none): {e}")
        short_items = []

    if not long_items and not short_items:
        raise ValueError(f"No data found for YouTube channel: {channel_input}")

    # ─── Step 3: Extract channel info from first available item ───
    first = long_items[0] if long_items else short_items[0]
    about = first.get('aboutChannelInfo', {})

    channel_id = about.get('channelId', '') or first.get('channelId', '')
    channel_handle_raw = about.get('channelUsername', '') or first.get('channelUsername', '') or handle
    channel_name = about.get('channelName', '') or first.get('channelName', '')
    subscribers = about.get('numberOfSubscribers', 0) or first.get('numberOfSubscribers', 0) or 0
    total_videos = about.get('channelTotalVideos', 0) or first.get('channelTotalVideos', 0) or 0

    logger.info(f"[YouTube] Channel: {channel_name} (@{channel_handle_raw}) | {subscribers} subs")

    # ─── Step 4: Skip newest video (incomplete view data), limit to 10 ───
    if len(long_items) > 1:
        long_items = long_items[1:]
    long_items = long_items[:10]

    if len(short_items) > 1:
        short_items = short_items[1:]
    short_items = short_items[:10]

    # ─── Step 5: Calculate metrics ───
    long_metrics = _calculate_video_metrics(long_items) if long_items else {
        'avg_views': 0, 'engagement_rate': 0.0, 'avg_video_length': 0, '_views_count': 0
    }
    short_metrics = _calculate_video_metrics(short_items) if short_items else {
        'avg_views': 0, 'engagement_rate': 0.0, 'avg_video_length': 0, '_views_count': 0
    }

    has_enough_long = long_metrics['_views_count'] >= MIN_VIDEOS_FOR_METRICS
    has_enough_short = short_metrics['_views_count'] >= MIN_VIDEOS_FOR_METRICS

    if not has_enough_long and not has_enough_short:
        raise InsufficientDataError(
            f"Not enough video data for '{channel_name}'. "
            f"Found {long_metrics['_views_count']} long-form and {short_metrics['_views_count']} Shorts with views "
            f"(need ≥ {MIN_VIDEOS_FOR_METRICS} in at least one). Creator was NOT added."
        )

    # Log what we got
    if has_enough_long:
        logger.info(f"[YouTube] Long metrics: {long_metrics['avg_views']} avg views, {long_metrics['engagement_rate']}% ER, {long_metrics['avg_video_length']}s avg dur")
    if has_enough_short:
        logger.info(f"[YouTube] Short metrics: {short_metrics['avg_views']} avg views, {short_metrics['engagement_rate']}% ER, {short_metrics['avg_video_length']}s avg dur")

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "channel_id": channel_id,
        "channel_handle": channel_handle_raw.lstrip('@'),
        "channel_name": channel_name,
        "profile_link": f"https://www.youtube.com/@{channel_handle_raw.lstrip('@')}",
        "subscribers": int(subscribers),
        "total_videos": int(total_videos),
        # Long-form metrics
        "avg_long_views": int(long_metrics['avg_views']) if has_enough_long else 0,
        "long_engagement_rate": long_metrics['engagement_rate'] if has_enough_long else 0.0,
        "avg_long_duration": long_metrics['avg_video_length'] if has_enough_long else 0,
        # Shorts metrics
        "avg_short_views": int(short_metrics['avg_views']) if has_enough_short else 0,
        "short_engagement_rate": short_metrics['engagement_rate'] if has_enough_short else 0.0,
        "avg_short_duration": short_metrics['avg_video_length'] if has_enough_short else 0,
        # Timestamps
        "last_scraped_at": now_iso,
    }
