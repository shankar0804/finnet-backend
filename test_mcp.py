import asyncio
import os
import httpx
from mcp.client.sse import sse_client
from mcp.client.session import ClientSession
import httpx_sse
from contextlib import asynccontextmanager

# --- NUCLEAR SSL BYPASS FOR HTTPX ---
original_transport_init = httpx.AsyncHTTPTransport.__init__
def new_transport_init(self, *args, **kwargs):
    kwargs['verify'] = False
    original_transport_init(self, *args, **kwargs)
httpx.AsyncHTTPTransport.__init__ = new_transport_init

original_sync_transport_init = httpx.HTTPTransport.__init__
def new_sync_transport_init(self, *args, **kwargs):
    kwargs['verify'] = False
    original_sync_transport_init(self, *args, **kwargs)
httpx.HTTPTransport.__init__ = new_sync_transport_init
# ------------------------------------

# --- MCP SSE HANDSHAKE OVERRIDE ---
original_aconnect_sse = httpx_sse.aconnect_sse
@asynccontextmanager
async def patched_aconnect_sse(client, method, url, **kwargs):
    if "supabase.com" in str(url):
        method = "POST" # Forcibly mutate the hardcoded GET request to POST
        
        headers = kwargs.get("headers")
        if headers is None:
            headers = {}
        elif isinstance(headers, httpx.Headers):
            headers = dict(headers.items())
            
        headers["Accept"] = "application/json, text/event-stream"
        kwargs["headers"] = headers

    async with original_aconnect_sse(client, method, url, **kwargs) as event_source:
        yield event_source
        
import mcp.client.sse
mcp.client.sse.aconnect_sse = patched_aconnect_sse
# -----------------------------------

async def test_supabase_mcp():
    url = "https://mcp.supabase.com/mcp?project_ref=vljfczytysvhochiirlf"
    supabase_key = "sbp_9f630ff3c1b7bbac7ba4045ee1e91314884cd34d"
    headers = {"Authorization": f"Bearer {supabase_key}"}

    print(f"Connecting to Supabase MCP at {url}...")
    try:
        async with sse_client(url, headers=headers) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                print("✅ MCP Session Successfully Initialized!")
                tools_response = await session.list_tools()
                print("\n🛠️ Available Supabase MCP Tools:")
                for tool in tools_response.tools:
                    print(f"- {tool.name}")
    except Exception as e:
        import traceback
        print("❌ MCP Connection Error:", e)
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_supabase_mcp())
