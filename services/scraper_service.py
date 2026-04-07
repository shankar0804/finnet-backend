import os
import time
import json
import requests
import urllib3
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

APIFY_TOKEN = os.environ.get('APIFY_TOKEN', '')

def fetch_influencer_data(username: str) -> dict:
    """Scrapes raw data from Instagram via Apify and generates a formatted Influencer representation."""
    if not APIFY_TOKEN:
        raise ValueError("Apify API token not configured.")

    start_url = f"https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs?token={APIFY_TOKEN}"
    run_input = {"usernames": [username], "resultsLimit": 20}
    
    logger.info(f"[Service] Starting Apify REST profile scraper for {username}...")
    
    # 1. Start the Actor Run
    resp = requests.post(start_url, json=run_input, verify=False)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to start Apify actor: {resp.text}")
        
    run_data = resp.json().get("data", {})
    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")
    
    # 2. Poll for Completion
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    while True:
        time.sleep(3)
        status_resp = requests.get(status_url, verify=False)
        status = status_resp.json().get("data", {}).get("status")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            if status != "SUCCEEDED":
                raise Exception(f"Apify run finished with status: {status}")
            break
            
    # 3. Retrieve results from dataset
    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"
    dataset_resp = requests.get(dataset_url, verify=False)
    items = dataset_resp.json()
    
    if not items:
        raise ValueError("No profile data found for this user.")
        
    profile_data = items[0]
    
    # Guard: Apify sometimes returns profile shell with no actual data
    if profile_data.get("followersCount") is None and profile_data.get("postsCount") is None:
        raise ValueError(
            f"Instagram returned an empty profile for @{username}. "
            "This usually means Instagram blocked the scrape. Please try again in a minute."
        )
        
    # Isolate reels: filter to videos/clips, exclude pinned
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
    
    # --- Avg Views with IQR Outlier Removal ---
    # Goal: Find the "typical" view range, ignoring viral spikes and dead posts
    views = [r.get("videoViewCount", 0) for r in target_reels if r.get("videoViewCount", 0) > 0]
    
    if len(views) >= 4:
        # IQR method: remove statistical outliers (both high and low)
        views_sorted = sorted(views)
        n = len(views_sorted)
        q1 = views_sorted[n // 4]           # 25th percentile
        q3 = views_sorted[(3 * n) // 4]     # 75th percentile
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        valid_views = [v for v in views_sorted if lower_bound <= v <= upper_bound]
        # Fallback: if IQR removes everything (all same value), use all
        if not valid_views:
            valid_views = views_sorted
    elif len(views) > 2:
        # Not enough for IQR, just drop highest and lowest
        views_sorted = sorted(views)
        valid_views = views_sorted[1:-1]
    else:
        valid_views = views
    
    avg_views = (sum(valid_views) / len(valid_views)) if valid_views else 0
    # Round to nearest 50K for >= 50K, nearest 10K for >= 10K, nearest 1K otherwise
    if avg_views >= 50000:
        avg_views = round(avg_views / 50000) * 50000
    elif avg_views >= 10000:
        avg_views = round(avg_views / 10000) * 10000
    elif avg_views >= 1000:
        avg_views = round(avg_views / 1000) * 1000
    else:
        avg_views = round(avg_views)
    
    # --- Engagement Rate ---
    total_views = sum(views)
    total_likes = sum(r.get("likesCount", 0) for r in target_reels)
    total_comments = sum(r.get("commentsCount", 0) for r in target_reels)
    engagement_rate = 0.0
    if total_views > 0:
        engagement_rate = round(((total_likes + total_comments) / total_views) * 100, 2)
    
    # --- Average Video Length (seconds) ---
    # Apify doesn't return videoDuration directly, but it's encoded in the videoUrl's efg param
    import base64 as b64
    from urllib.parse import urlparse, parse_qs
    
    durations = []
    for r in target_reels:
        vid_url = r.get("videoUrl", "")
        if not vid_url:
            continue
        try:
            parsed = urlparse(vid_url)
            efg_vals = parse_qs(parsed.query).get("efg", [])
            if efg_vals:
                # efg is base64 encoded JSON containing duration_s
                efg_json = json.loads(b64.b64decode(efg_vals[0] + "==").decode("utf-8", errors="ignore"))
                dur = efg_json.get("duration_s")
                if dur and isinstance(dur, (int, float)) and dur > 0:
                    durations.append(int(dur))
        except Exception:
            pass
    
    # Outlier removal for durations too
    if len(durations) > 2:
        durations_sorted = sorted(durations)
        valid_durations = durations_sorted[1:-1]
    else:
        valid_durations = durations
    avg_video_length = round(sum(valid_durations) / len(valid_durations)) if valid_durations else 0
    
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
        
    return {
        "username": profile_data.get("username", username),
        "creator_name": profile_data.get("fullName", username),
        "profile_link": f"https://instagram.com/{profile_data.get('username', username)}",
        "platform": "Instagram",
        "followers": profile_data.get("followersCount", 0),
        "avg_views": int(avg_views),
        "engagement_rate": engagement_rate,
        "avg_video_length": avg_video_length,
        # Timestamps
        "last_scraped_at": now_iso,
    }
