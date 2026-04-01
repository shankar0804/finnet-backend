import asyncio
import httpx
import json

async def test_stateless_mcp():
    url = "https://mcp.supabase.com/mcp?project_ref=vljfczytysvhochiirlf"
    supabase_key = "sbp_9f630ff3c1b7bbac7ba4045ee1e91314884cd34d"
    
    headers = {
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(verify=False) as client:
        # 1. Initialize
        init_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "trakr", "version": "1.0.0"}
            }
        }
        res = await client.post(url, headers=headers, json=init_payload)
        session_id = res.headers.get("mcp-session-id")
        
        print("INIT Response:", res.status_code, res.text)
        print("Session ID:", session_id)
        
        # 2. List Tools
        if session_id:
            headers["mcp-session-id"] = session_id
            tools_payload = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }
            res_tools = await client.post(url, headers=headers, json=tools_payload)
            print("\nTOOLS Response:", res_tools.status_code, res_tools.text)

if __name__ == "__main__":
    asyncio.run(test_stateless_mcp())
