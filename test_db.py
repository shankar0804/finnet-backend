import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv
import httpx

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

try:
    # Attempting to monkeypatch httpx default verify if options aren't available
    from httpx import Client as HTTPXClient
    from postgrest import APIResponse
    print("Testing create_client...")
    client = httpx.Client(verify=False)
    # The modern option is just let httpx trust the proxy. But maybe we can just patch Client.__init__
    original_init = httpx.Client.__init__
    def new_init(self, *args, **kwargs):
        kwargs['verify'] = False
        original_init(self, *args, **kwargs)
    httpx.Client.__init__ = new_init

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    resp = supabase.table('influencers').select('*').limit(1).execute()
    print("Success!", resp.data)
except Exception as e:
    print("Error:", str(e))
