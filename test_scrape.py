import os
import requests
import time
import json
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
username = "ohnovinay"

print(f"Starting scrape for @{username}...")
resp = requests.post(
    f"https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs?token={APIFY_TOKEN}",
    json={"usernames": [username], "resultsLimit": 20},
    verify=False
)
print(f"Start status: {resp.status_code}")
run = resp.json().get("data", {})
run_id = run.get("id")
dataset_id = run.get("defaultDatasetId")
print(f"Run ID: {run_id}")

# Poll
while True:
    time.sleep(3)
    s = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}", verify=False)
    status = s.json().get("data", {}).get("status")
    print(f"  Status: {status}")
    if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
        break

# Get results
items = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}", verify=False).json()
print(f"\nItems returned: {len(items)}")
if items:
    p = items[0]
    print(f"Username: {p.get('username')}")
    print(f"Full Name: {p.get('fullName')}")
    print(f"Followers: {p.get('followersCount')}")
    print(f"Following: {p.get('followingCount')}")
    print(f"Posts count: {p.get('postsCount')}")
    print(f"Is Private: {p.get('private')}")
    posts = p.get("latestPosts", [])
    print(f"Latest posts returned: {len(posts)}")
    videos = [x for x in posts if x.get("type","").lower() == "video" or x.get("productType","").lower() in ["clips","igtv"]]
    print(f"Video/reels found: {len(videos)}")
    for v in videos[:3]:
        print(f"  - views: {v.get('videoViewCount')}, likes: {v.get('likesCount')}, type: {v.get('type')}, product: {v.get('productType')}")
else:
    print("NO DATA RETURNED!")
