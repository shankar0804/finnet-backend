import os
import httpx
from supabase import create_client, Client

# --- SSL Circumvention Patch ---
original_init = httpx.Client.__init__
def new_init(self, *args, **kwargs):
    kwargs['verify'] = False
    original_init(self, *args, **kwargs)
httpx.Client.__init__ = new_init
# -------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Initialize global Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
