-- Create the highly structured Influencer Agency Database
CREATE TABLE IF NOT EXISTS influencers (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    creator_name VARCHAR(255),
    profile_link TEXT,
    platform VARCHAR(50) DEFAULT 'Instagram',
    niche VARCHAR(255) DEFAULT '',
    language VARCHAR(100) DEFAULT '',
    location VARCHAR(255) DEFAULT '',
    followers BIGINT DEFAULT 0,
    avg_views BIGINT DEFAULT 0,
    engagement_rate DECIMAL(5,2) DEFAULT 0.0,
    avg_video_length INT DEFAULT 0,

    -- OCR Locked Columns
    avd VARCHAR(20) DEFAULT '',
    skip_rate VARCHAR(20) DEFAULT '',

    -- Age Demographics (OCR)
    age_13_17 VARCHAR(20) DEFAULT '',
    age_18_24 VARCHAR(20) DEFAULT '',
    age_25_34 VARCHAR(20) DEFAULT '',
    age_35_44 VARCHAR(20) DEFAULT '',
    age_45_54 VARCHAR(20) DEFAULT '',

    -- Gender (OCR)
    male_pct VARCHAR(20) DEFAULT '',
    female_pct VARCHAR(20) DEFAULT '',

    -- Creator Gender (user-provided mandatory field)
    gender VARCHAR(20) DEFAULT '',

    -- Top Cities (OCR)
    city_1 VARCHAR(100) DEFAULT '',
    city_2 VARCHAR(100) DEFAULT '',
    city_3 VARCHAR(100) DEFAULT '',
    city_4 VARCHAR(100) DEFAULT '',
    city_5 VARCHAR(100) DEFAULT '',

    -- Manual Fields
    contact_numbers VARCHAR(255) DEFAULT '',
    mail_id VARCHAR(255) DEFAULT '',
    managed_by VARCHAR(255) DEFAULT '',

    -- Timestamp Tracking
    last_scraped_at TIMESTAMP WITH TIME ZONE,
    last_ocr_at TIMESTAMP WITH TIME ZONE,
    last_manual_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Enable Row Level Security
ALTER TABLE influencers ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anonymous read access" ON influencers FOR SELECT USING (true);
CREATE POLICY "Allow anonymous insert access" ON influencers FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow anonymous update access" ON influencers FOR UPDATE USING (true);
CREATE POLICY "Allow anonymous delete access" ON influencers FOR DELETE USING (true);

-- ═══ App Users (RBAC) ═══
-- Stores signed-in users and their roles.
-- operations@finnetmedia.com = admin (hardcoded in app logic)
-- auth_method: 'google' for @finnetmedia.com, 'password' for external employees
-- Default role for new users = junior
CREATE TABLE IF NOT EXISTS app_users (
    email VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) DEFAULT '',
    picture TEXT DEFAULT '',
    role VARCHAR(20) DEFAULT 'junior' CHECK (role IN ('admin', 'senior', 'junior', 'brand')),
    auth_method VARCHAR(20) DEFAULT 'google' CHECK (auth_method IN ('google', 'password')),
    password_hash TEXT DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE app_users ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anonymous read access" ON app_users FOR SELECT USING (true);
CREATE POLICY "Allow anonymous insert access" ON app_users FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow anonymous update access" ON app_users FOR UPDATE USING (true);
CREATE POLICY "Allow anonymous delete access" ON app_users FOR DELETE USING (true);

-- ═══ Audit Logs ═══
-- Tracks all INSERT, UPDATE, DELETE operations across the system.
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    operation VARCHAR(20) NOT NULL,              -- INSERT, UPDATE, DELETE, LOGIN, EXPORT
    performed_by VARCHAR(255) DEFAULT 'system',  -- email of the user who performed the action
    target_table VARCHAR(100) DEFAULT '',         -- influencers, app_users, etc.
    target_id VARCHAR(255) DEFAULT '',            -- username or email of the affected record
    details JSONB DEFAULT '{}',                   -- what changed: {field: value} or description
    source VARCHAR(50) DEFAULT 'dashboard',       -- dashboard, whatsapp_bot, system, bulk_import
    ip_address VARCHAR(50) DEFAULT '',            -- request IP
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for fast admin queries (most recent first)
CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs (performed_by);

ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anonymous read access" ON audit_logs FOR SELECT USING (true);
CREATE POLICY "Allow anonymous insert access" ON audit_logs FOR INSERT WITH CHECK (true);

-- ═══ Partnerships (Brand Deals) ═══
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

-- ═══ Campaigns (under Partnerships) ═══
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

-- ═══ Campaign Entries (Creator assignments) ═══
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
