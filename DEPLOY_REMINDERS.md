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

> Delete this file once all steps are completed.
