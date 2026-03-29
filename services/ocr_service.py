import os
import requests
import time
import base64
import json
from io import BytesIO
from PIL import Image
from openai import OpenAI

PADDLE_URL = "https://ai.api.nvidia.com/v1/cv/baidu/paddleocr"
PADDLE_KEY = "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"
QWEN_KEY = "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"

def run_ocr_pipeline(image_bytes: bytes) -> dict:
    """Executes the dual-node AI pipeline to extract analytical values from raw dashboard screenshots."""
    
    # 1. Compression
    MAX_B64_LEN = 180_000
    b64_str = base64.b64encode(image_bytes).decode('utf-8')
    
    if len(b64_str) > MAX_B64_LEN:
        img = Image.open(BytesIO(image_bytes)).convert('RGB')
        quality, ratio = 85, 0.8
        while len(b64_str) > MAX_B64_LEN and ratio > 0.2:
            new_size = (int(img.width * ratio), int(img.height * ratio))
            resized_img = img.resize(new_size, Image.Resampling.LANCZOS)
            buffer = BytesIO()
            resized_img.save(buffer, format="JPEG", quality=quality)
            image_bytes = buffer.getvalue()
            b64_str = base64.b64encode(image_bytes).decode('utf-8')
            ratio -= 0.15
            quality -= 10

    if len(b64_str) > MAX_B64_LEN:
        raise ValueError("Image is still too large even after maximum compression")
        
    # 2. Node 1: PaddleOCR
    paddle_headers = {"Authorization": f"Bearer {PADDLE_KEY}", "Accept": "application/json"}
    paddle_payload = {"input": [{"type": "image_url", "url": f"data:image/png;base64,{b64_str}"}]}
    
    node1_s = time.time()
    resp = requests.post(PADDLE_URL, headers=paddle_headers, json=paddle_payload, verify=False)
    node1_duration = round(time.time() - node1_s, 2)
    
    if resp.status_code != 200:
        raise Exception(f"OCR failed with status {resp.status_code}: {resp.text}")
        
    ocr_data = resp.json()
    extracted_texts = []
    
    data_list = ocr_data.get('data', [])
    if data_list:
        for d in data_list[0].get('text_detections', []):
            t = d.get('text_prediction', {}).get('text', '')
            if t: extracted_texts.append(t)
            
    combined_text = "\n".join(extracted_texts)
    if not combined_text:
        raise ValueError("No text detected in the image.")

    # 3. Node 2: Llama LLM Processing
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
    
    node2_s = time.time()
    client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=QWEN_KEY)
    
    completion = client.chat.completions.create(
        model="meta/llama-3.1-8b-instruct",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"OCR Text:\n{combined_text}"}
        ],
        temperature=0.2, top_p=0.7, max_tokens=1024, stream=True
    )
    
    llm_text = ""
    for chunk in completion:
        if chunk.choices and chunk.choices[0].delta.content is not None:
            llm_text += chunk.choices[0].delta.content
            
    node2_duration = round(time.time() - node2_s, 2)
    llm_text = llm_text.strip()
    
    if llm_text.startswith("```json"): llm_text = llm_text[7:]
    elif llm_text.startswith("```"): llm_text = llm_text[3:]
    if llm_text.endswith("```"): llm_text = llm_text[:-3]
    
    try:
        final_result = json.loads(llm_text.strip())
    except Exception:
        raise ValueError(f"Failed to parse LLM JSON: {llm_text}")
        
    return {
        "ocr_extracted": combined_text,
        "result": final_result,
        "metrics": {
            "node1_ocr_time_sec": node1_duration,
            "node2_llm_time_sec": node2_duration,
            "total_time_sec": round(node1_duration + node2_duration, 2)
        }
    }
