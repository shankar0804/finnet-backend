import requests
import json
import base64
import os
from dotenv import load_dotenv

load_dotenv()

PADDLE_URL = "https://ai.api.nvidia.com/v1/cv/baidu/paddleocr"
PADDLE_KEY = os.environ.get("PADDLE_KEY", "")
QWEN_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
QWEN_KEY = os.environ.get("QWEN_KEY", "")

import sys

def test_paddle(image_path="paddleocr1.png"):
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
    
    print(f"\n--- Testing Node 1 (PaddleOCR) with {image_path} ---")
    try:
        with open(image_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        print("Failed to read image:", e)
        return None
        
    paddle_headers = {
        "Authorization": f"Bearer {PADDLE_KEY}",
        "Accept": "application/json"
    }
    paddle_payload = {
        "input": [
            {
                "type": "image_url",
                "url": f"data:image/png;base64,{image_b64}"
            }
        ]
    }
    
    print("Sending request to PaddleOCR...")
    res = requests.post(PADDLE_URL, headers=paddle_headers, json=paddle_payload)
    print("Status code:", res.status_code)
    try:
        data = res.json()
        print("Got JSON response.")
        
        extracted_texts = []
        data_list = data.get('data', [])
        if len(data_list) > 0:
            detections = data_list[0].get('text_detections', [])
            for d in detections:
                t = d.get('text_prediction', {}).get('text', '')
                if t: extracted_texts.append(t)
        
        combined_text = "\n".join(extracted_texts)
        print("Extracted Length:", len(combined_text))
        print("First 50 chars:", repr(combined_text[:50]))
        return combined_text
    except Exception as e:
        print("Paddle Error:", e)
        print("Raw text:", res.text)
        return None

def test_qwen(ocr_text):
    print("\n--- Testing Node 2 (Qwen LLM) ---")
    
    qwen_headers = {
        "Authorization": f"Bearer {QWEN_KEY}",
        "Accept": "text/event-stream"
    }
    
    system_prompt = """
You are an AI data extractor. You will be provided with a raw list of text extracted from an image by an OCR.
Your task is to identify and extract the following 4 specific metrics:
1. Engaged views
2. Unique viewers
3. Watch time (hours)
4. Average view duration

Return the result EXCLUSIVELY as a valid JSON object in the exact format shown below, with no other text, code blocks, or explanations:
{
    "engaged_views": "<value or N/A>",
    "unique_viewers": "<value or N/A>",
    "watch_time_hours": "<value or N/A>",
    "average_view_duration": "<value or N/A>"
}
    """.strip()
    
    qwen_payload = {
        "model": "qwen/qwen3.5-397b-a17b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"OCR Text:\n{ocr_text}"}
        ],
        "temperature": 0.1,
        "max_tokens": 500,
        "stream": True,
        "chat_template_kwargs": {"enable_thinking": True}
    }
    
    print("Calling Qwen API...")
    try:
        llm_response = requests.post(QWEN_URL, headers=qwen_headers, json=qwen_payload, stream=True)
        print("Status code:", llm_response.status_code)
        llm_text = ""
        for line in llm_response.iter_lines():
            if line:
                line_decoded = line.decode('utf-8')
                print(f"CHUNK RECVD: {line_decoded}")
                if line_decoded.startswith("data: "):
                    data_str = line_decoded[6:]
                    if data_str == "[DONE]":
                        break
                    try:
                        data_json = json.loads(data_str)
                        content = data_json.get('choices', [{}])[0].get('delta', {}).get('content', '')
                        if content:
                            llm_text += content
                    except Exception as e:
                        pass
        print("\nFinal LLM Text output:")
        print(llm_text)
    except Exception as e:
        print("Qwen error:", e)

if __name__ == "__main__":
    ocr_result = test_paddle()
    if ocr_result:
        test_qwen(ocr_result)
