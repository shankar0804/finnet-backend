# 🚀 Deploy Reminders — TRAKRX

## Render: Update Service Account (PENDING)

The service account has been moved to `operations@finnetmedia.com` (project: `trakrx-492818`).
You need to update the base64-encoded service account on Render.

### Steps:
1. Run this in PowerShell to get the base64 string:
   ```powershell
   [Convert]::ToBase64String([IO.File]::ReadAllBytes("c:\Users\SHANKAM\Desktop\finnet\trakr-revamp\service_account.json"))
   ```
2. Go to **Render Dashboard → Your Service → Environment**
3. Update (or create) the env var: `GOOGLE_SA_BASE64`
4. Paste the base64 string as the value
5. Click **Save** → Render will auto-redeploy

## Vercel: Add Authorized JavaScript Origins (PENDING)

In Google Cloud Console ([Clients page](https://console.cloud.google.com/auth/clients)):
1. Click on the OAuth client `851315925686-...`
2. Add your **Vercel production URL** to **Authorized JavaScript origins**
   - e.g. `https://finnet-frontend.vercel.app`
3. Add it to **Authorized redirect URIs** too:
   - e.g. `https://finnet-frontend.vercel.app`
   - `https://finnet-backend.onrender.com/oauth2callback`
4. Save

## Render: Add JWT_SECRET env var (PENDING)

Add this env var on Render so JWT tokens work in production:
- Key: `JWT_SECRET`
- Value: `trakrx-finnet-2026-secure-jwt-key-d9f8a2c1`

## Supabase: Create app_users table (CHECK)

Run this SQL in **Supabase SQL Editor** if the table doesn't exist:
```sql
-- If app_users already exists, add the new columns:
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS auth_method VARCHAR(20) DEFAULT 'google';
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS password_hash TEXT DEFAULT '';

-- Update role constraint to include 'brand':
ALTER TABLE app_users DROP CONSTRAINT IF EXISTS app_users_role_check;
ALTER TABLE app_users ADD CONSTRAINT app_users_role_check CHECK (role IN ('admin', 'senior', 'junior', 'brand'));

-- If it doesn't exist yet, full creation:
CREATE TABLE IF NOT EXISTS app_users (
    email VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) DEFAULT '',
    picture TEXT DEFAULT '',
    role VARCHAR(20) DEFAULT 'junior' CHECK (role IN ('admin', 'senior', 'junior')),
    auth_method VARCHAR(20) DEFAULT 'google',
    password_hash TEXT DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
ALTER TABLE app_users ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_read" ON app_users FOR SELECT USING (true);
CREATE POLICY "anon_insert" ON app_users FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_update" ON app_users FOR UPDATE USING (true);
CREATE POLICY "anon_delete" ON app_users FOR DELETE USING (true);
```

## Supabase: Create audit_logs table (PENDING)

Run this SQL in **Supabase SQL Editor**:
```sql
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    operation VARCHAR(20) NOT NULL,
    performed_by VARCHAR(255) DEFAULT 'system',
    target_table VARCHAR(100) DEFAULT '',
    target_id VARCHAR(255) DEFAULT '',
    details JSONB DEFAULT '{}',
    source VARCHAR(50) DEFAULT 'dashboard',
    ip_address VARCHAR(50) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs (performed_by);
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_read" ON audit_logs FOR SELECT USING (true);
CREATE POLICY "anon_insert" ON audit_logs FOR INSERT WITH CHECK (true);
```

---

## Supabase: Brand Management Tables (NEW)

Run this SQL to create the partnership/campaign/entry tables:

```sql
-- Partnerships
CREATE TABLE IF NOT EXISTS partnerships (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) DEFAULT '',
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'completed', 'paused')),
    notes TEXT DEFAULT '',
    created_by VARCHAR(255) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
ALTER TABLE partnerships ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all_partnerships" ON partnerships FOR ALL USING (true) WITH CHECK (true);

-- Campaigns
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    partnership_id UUID NOT NULL REFERENCES partnerships(id) ON DELETE CASCADE,
    campaign_name VARCHAR(255) NOT NULL,
    platform VARCHAR(50) DEFAULT 'Instagram',
    status VARCHAR(20) DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'completed')),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_campaigns_partnership ON campaigns (partnership_id);
ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all_campaigns" ON campaigns FOR ALL USING (true) WITH CHECK (true);

-- Campaign Entries
CREATE TABLE IF NOT EXISTS campaign_entries (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    creator_username VARCHAR(255) NOT NULL,
    deliverable_type VARCHAR(50) DEFAULT 'Reel' CHECK (deliverable_type IN ('Reel', 'Story', 'Post', 'Video', 'Other')),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'delivered', 'approved')),
    content_link TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    amount DECIMAL(10,2) DEFAULT 0,
    delivery_date DATE,
    poc VARCHAR(255) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_entries_campaign ON campaign_entries (campaign_id);
ALTER TABLE campaign_entries ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all_entries" ON campaign_entries FOR ALL USING (true) WITH CHECK (true);
```

---

> Delete this file once all steps are completed.
