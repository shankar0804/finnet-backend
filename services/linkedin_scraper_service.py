"""LinkedIn Profile Scraper Service.

Uses Apify's supreme_coder/linkedin-profile-scraper (no cookies required, ~$3/1k profiles)
to fetch professional profile data.
"""

import os
import logging
from datetime import datetime, timezone
from services.scraper_service import _run_apify_actor

logger = logging.getLogger(__name__)

APIFY_TOKEN = os.environ.get('APIFY_TOKEN', '')


def _extract_linkedin_identifier(url_or_id: str) -> tuple:
    """Normalize LinkedIn input to (profile_url, public_identifier).
    Handles: linkedin.com/in/username, just 'username', full URL with query params
    Returns: (full_url, public_identifier)
    """
    url_or_id = url_or_id.strip().rstrip('/')

    # Remove query parameters
    if '?' in url_or_id:
        url_or_id = url_or_id.split('?')[0]

    # Full URL
    if 'linkedin.com/in/' in url_or_id:
        # Extract the public identifier from the URL
        parts = url_or_id.split('linkedin.com/in/')
        public_id = parts[1].strip('/').split('/')[0]
        return (f"https://www.linkedin.com/in/{public_id}/", public_id)

    # Just the username/identifier
    public_id = url_or_id.lstrip('@')
    return (f"https://www.linkedin.com/in/{public_id}/", public_id)


def fetch_linkedin_data(profile_input: str) -> dict:
    """Scrapes LinkedIn profile data via Apify and returns formatted creator data.

    Args:
        profile_input: LinkedIn profile URL or public identifier

    Returns:
        dict with profile metadata

    Raises:
        ValueError: if profile not found or scraper fails
    """
    if not APIFY_TOKEN:
        raise ValueError("Apify API token not configured.")

    profile_url, public_id = _extract_linkedin_identifier(profile_input)
    logger.info(f"[LinkedIn] Starting scrape for: {public_id} ({profile_url})")

    run_input = {
        "urls": [{"url": profile_url}],
    }

    items = _run_apify_actor("supreme_coder/linkedin-profile-scraper", run_input)

    if not items:
        raise ValueError(f"No LinkedIn profile data found for: {profile_input}")

    profile = items[0]

    # Guard: check if we got actual data
    first_name = profile.get('firstName', '') or ''
    last_name = profile.get('lastName', '') or ''
    full_name = f"{first_name} {last_name}".strip()

    if not full_name and not profile.get('headline'):
        raise ValueError(
            f"LinkedIn returned an empty profile for '{profile_input}'. "
            "This may mean the profile is private or the scraper was blocked."
        )

    # Extract the public identifier (may differ from what we parsed from URL)
    actual_public_id = profile.get('publicIdentifier', public_id) or public_id

    # Extract current position
    positions = profile.get('positions', [])
    current_company = ''
    current_title = ''
    if positions and isinstance(positions, list):
        # First position is usually current (no endDate)
        for pos in positions:
            if isinstance(pos, dict):
                time_period = pos.get('timePeriod', {})
                if not time_period.get('endDate'):  # No end date = current
                    current_title = pos.get('title', '')
                    current_company = pos.get('companyName', '') or profile.get('companyName', '')
                    break
        # Fallback: use the first position
        if not current_company and positions:
            first_pos = positions[0] if isinstance(positions[0], dict) else {}
            current_company = first_pos.get('companyName', '') or profile.get('companyName', '')
            current_title = first_pos.get('title', '') or profile.get('jobTitle', '')

    # Fallback to top-level fields
    if not current_company:
        current_company = profile.get('companyName', '')
    if not current_title:
        current_title = profile.get('jobTitle', '') or profile.get('occupation', '')

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "profile_id": actual_public_id,
        "full_name": full_name or profile.get('occupation', ''),
        "profile_link": f"https://www.linkedin.com/in/{actual_public_id}/",
        "headline": profile.get('headline', '') or profile.get('occupation', ''),
        "summary": profile.get('summary', ''),
        "current_company": current_company,
        "current_title": current_title,
        "industry": profile.get('industryName', ''),
        "location": profile.get('geoLocationName', '') or profile.get('geoCountryName', ''),
        "connections": profile.get('followerCount', 0) or profile.get('connectionCount', 0) or 0,
        # Timestamps
        "last_scraped_at": now_iso,
    }
