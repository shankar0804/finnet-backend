import os
import requests
import time
import base64
import json
from io import BytesIO
from PIL import Image
from openai import OpenAI

PADDLE_URL = "https://ai.api.nvidia.com/v1/cv/baidu/paddleocr"
PADDLE_KEY = os.environ.get("PADDLE_KEY", "")
QWEN_KEY = os.environ.get("QWEN_KEY", "")


# ═══════════════════════════════════════════════════════════
# Shared pipeline helpers (Node 1 — PaddleOCR, Node 2 — Llama)
# ═══════════════════════════════════════════════════════════


def _compress_image_to_b64(image_bytes: bytes, max_len: int = 180_000) -> tuple:
    """Shrink an image until its base64 representation fits the LLM payload budget."""
    b64_str = base64.b64encode(image_bytes).decode("utf-8")
    if len(b64_str) <= max_len:
        return image_bytes, b64_str

    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    quality, ratio = 85, 0.8
    while len(b64_str) > max_len and ratio > 0.2:
        new_size = (int(img.width * ratio), int(img.height * ratio))
        resized_img = img.resize(new_size, Image.Resampling.LANCZOS)
        buffer = BytesIO()
        resized_img.save(buffer, format="JPEG", quality=quality)
        image_bytes = buffer.getvalue()
        b64_str = base64.b64encode(image_bytes).decode("utf-8")
        ratio -= 0.15
        quality -= 10

    if len(b64_str) > max_len:
        raise ValueError("Image is still too large even after maximum compression")
    return image_bytes, b64_str


def _run_paddle_ocr(b64_str: str) -> tuple:
    """Hit NVIDIA PaddleOCR. Returns (combined_text, seconds_elapsed)."""
    paddle_headers = {"Authorization": f"Bearer {PADDLE_KEY}", "Accept": "application/json"}
    paddle_payload = {"input": [{"type": "image_url", "url": f"data:image/png;base64,{b64_str}"}]}

    node1_s = time.time()
    resp = requests.post(PADDLE_URL, headers=paddle_headers, json=paddle_payload, verify=False)
    node1_duration = round(time.time() - node1_s, 2)

    if resp.status_code != 200:
        raise Exception(f"OCR failed with status {resp.status_code}: {resp.text}")

    ocr_data = resp.json()
    extracted_texts = []
    data_list = ocr_data.get("data", [])
    if data_list:
        for d in data_list[0].get("text_detections", []):
            t = d.get("text_prediction", {}).get("text", "")
            if t:
                extracted_texts.append(t)

    combined_text = "\n".join(extracted_texts)
    if not combined_text:
        raise ValueError("No text detected in the image.")
    return combined_text, node1_duration


def _run_llama_extract(system_prompt: str, combined_text: str) -> tuple:
    """Ask NVIDIA-hosted Llama to turn OCR text into a JSON dict. Returns (parsed, seconds_elapsed)."""
    node2_s = time.time()
    client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=QWEN_KEY)

    completion = client.chat.completions.create(
        model="meta/llama-3.1-8b-instruct",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"OCR Text:\n{combined_text}"},
        ],
        temperature=0.2,
        top_p=0.7,
        max_tokens=1024,
        stream=True,
    )

    llm_text = ""
    for chunk in completion:
        if chunk.choices and chunk.choices[0].delta.content is not None:
            llm_text += chunk.choices[0].delta.content

    node2_duration = round(time.time() - node2_s, 2)
    llm_text = llm_text.strip()

    if llm_text.startswith("```json"):
        llm_text = llm_text[7:]
    elif llm_text.startswith("```"):
        llm_text = llm_text[3:]
    if llm_text.endswith("```"):
        llm_text = llm_text[:-3]

    try:
        return json.loads(llm_text.strip()), node2_duration
    except Exception:
        raise ValueError(f"Failed to parse LLM JSON: {llm_text}")


def run_ocr_pipeline(image_bytes: bytes) -> dict:
    """Executes the dual-node AI pipeline to extract analytical values from raw dashboard screenshots."""

    _, b64_str = _compress_image_to_b64(image_bytes)
    combined_text, node1_duration = _run_paddle_ocr(b64_str)

    system_prompt = """
You are an AI data extractor. You receive raw OCR text from Instagram analytics dashboard screenshots.
Extract ONLY the metrics you can clearly find in the text. If a metric is NOT present in the text, return an EMPTY STRING "" for that field — never guess or make up values.

Metrics to look for:
1. Engaged views (may appear as "Engaged views", "Views", or a large number near the top)
2. Unique viewers
3. Watch time (in hours)
4. Average view duration (e.g. "0:45", "1:23")
5. Skip rate (may appear as "Typical % skipped", "Skip rate", "skipped", etc.)
6. Age demographics: percentage for each group — 13-17, 18-24, 25-34, 35-44, 45-54
7. Gender split: Male %, Female %
8. Top 5 cities: city names with percentage if available (e.g. "Mumbai 25%", "Delhi 18%")

Return ONLY a valid JSON object (no markdown, no explanation) in this exact format:
{
    "engaged_views": "",
    "unique_viewers": "",
    "watch_time_hours": "",
    "average_view_duration": "",
    "skip_rate": "",
    "age_13_17": "",
    "age_18_24": "",
    "age_25_34": "",
    "age_35_44": "",
    "age_45_54": "",
    "male_pct": "",
    "female_pct": "",
    "city_1": "",
    "city_2": "",
    "city_3": "",
    "city_4": "",
    "city_5": ""
}
Fill in ONLY the fields you find in the OCR text. Leave the rest as empty strings.
    """.strip()

    final_result, node2_duration = _run_llama_extract(system_prompt, combined_text)

    return {
        "ocr_extracted": combined_text,
        "result": final_result,
        "metrics": {
            "node1_ocr_time_sec": node1_duration,
            "node2_llm_time_sec": node2_duration,
            "total_time_sec": round(node1_duration + node2_duration, 2),
        },
    }


# ═══════════════════════════════════════════════════════════
# Post-level OCR pipeline (used by campaign entry creation)
# ═══════════════════════════════════════════════════════════


_POST_OCR_SYSTEM_PROMPT = """
You are an AI data extractor. You receive raw OCR text from screenshots of a
single social media post — typically an Instagram reel insights panel, a
YouTube Studio analytics screen, or a LinkedIn post detail view.

Extract ONLY the metrics you can clearly find in the text. If a metric is NOT
present, return an EMPTY STRING "" for that field — never guess.

Numbers may appear with suffixes like "K", "M", or "B" (e.g. "1.2M", "523K").
Keep them as the original short form — the caller will parse them.
Durations may appear as "1:23", "0:47", or "2:05" — return that raw string.

Fields to extract (post-level, i.e. for this ONE post/reel/video):
1. username       — creator handle (e.g. "@virat.kohli" -> "virat.kohli")
2. views          — total views / plays / impressions on the post
3. play_count     — plays specifically (if shown separately from views)
4. likes          — likes / reactions
5. comments       — comments count
6. shares         — shares / sends
7. saves          — saves / bookmarks
8. reach          — unique accounts reached
9. impressions    — total impressions (LinkedIn / paid posts)
10. duration      — video duration (e.g. "0:45") if shown
11. post_date     — posted/published date, any format you see
12. avd           — average view duration (YT analytics) e.g. "0:23"
13. ctr           — click-through rate percent e.g. "4.2%"
14. skip_rate     — typical % skipped (IG insights)

Return ONLY a valid JSON object (no markdown, no explanation) in this exact format:
{
    "username": "",
    "views": "",
    "play_count": "",
    "likes": "",
    "comments": "",
    "shares": "",
    "saves": "",
    "reach": "",
    "impressions": "",
    "duration": "",
    "post_date": "",
    "avd": "",
    "ctr": "",
    "skip_rate": ""
}
""".strip()


def _parse_short_number(raw) -> int:
    """Convert '1.2M', '523K', '1,234', '8.4b' to an int. Returns 0 on failure."""
    if raw is None:
        return 0
    s = str(raw).strip().replace(",", "").replace(" ", "")
    if not s:
        return 0
    mult = 1
    last = s[-1].lower()
    if last in ("k", "m", "b"):
        mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[last]
        s = s[:-1]
    try:
        return int(float(s) * mult)
    except (ValueError, TypeError):
        return 0


def _parse_duration(raw) -> int:
    """Convert 'mm:ss' or 'hh:mm:ss' to integer seconds. 0 on failure."""
    if not raw:
        return 0
    try:
        parts = [int(p) for p in str(raw).strip().split(":")]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 1:
            return parts[0]
    except (ValueError, TypeError):
        pass
    return 0


def run_post_ocr_pipeline(image_bytes: bytes) -> dict:
    """Post-level variant of run_ocr_pipeline.

    Targets the metrics a single reel / video / post screenshot exposes
    (views, likes, comments, shares, saves, AVD, CTR, etc.) and returns
    them in a shape that maps directly onto the campaign_entries columns.

    Return shape:
        {
            "ocr_extracted": "<raw text>",
            "result":  {parsed ints + strings, numbers already converted},
            "metrics": {timings},
        }

    `result` keys match the post_scraper_service output field names so
    the entry orchestrator can do a trivial gap-fill merge:
        username, video_views, play_count, likes, comments, shares,
        saves, impressions, duration_secs, post_timestamp, avd,
        ctr, skip_rate.
    """
    _, b64_str = _compress_image_to_b64(image_bytes)
    combined_text, node1_duration = _run_paddle_ocr(b64_str)
    raw, node2_duration = _run_llama_extract(_POST_OCR_SYSTEM_PROMPT, combined_text)

    def s(val) -> str:
        v = str(val or "").strip()
        if v.lower() in ("n/a", "na", "none", "-", "null"):
            return ""
        return v

    username = s(raw.get("username", "")).lstrip("@")

    result = {
        "username": username,
        "video_views": _parse_short_number(raw.get("views", "")),
        "play_count": _parse_short_number(raw.get("play_count", "")),
        "likes": _parse_short_number(raw.get("likes", "")),
        "comments": _parse_short_number(raw.get("comments", "")),
        "shares": _parse_short_number(raw.get("shares", "")),
        "saves": _parse_short_number(raw.get("saves", "")),
        "reach": _parse_short_number(raw.get("reach", "")),
        "impressions": _parse_short_number(raw.get("impressions", "")),
        "duration_secs": _parse_duration(raw.get("duration", "")),
        "post_timestamp": s(raw.get("post_date", "")),
        # YouTube / IG insights extras — kept as strings (formatted for display).
        "avd": s(raw.get("avd", "")),
        "ctr": s(raw.get("ctr", "")),
        "skip_rate": s(raw.get("skip_rate", "")),
    }

    return {
        "ocr_extracted": combined_text,
        "result": result,
        "metrics": {
            "node1_ocr_time_sec": node1_duration,
            "node2_llm_time_sec": node2_duration,
            "total_time_sec": round(node1_duration + node2_duration, 2),
        },
    }
