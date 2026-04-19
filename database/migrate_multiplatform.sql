-- ═══ Multi-Platform Migration ═══
-- Run this in Supabase Dashboard > SQL Editor

-- 1. Add creator_group_id to existing influencers table
ALTER TABLE influencers ADD COLUMN IF NOT EXISTS creator_group_id UUID DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_influencers_group ON influencers (creator_group_id);

-- 2. YouTube Creators table
CREATE TABLE IF NOT EXISTS youtube_creators (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    channel_id VARCHAR(255) UNIQUE NOT NULL,
    channel_handle VARCHAR(255) DEFAULT '',
    channel_name VARCHAR(255),
    profile_link TEXT,
    niche VARCHAR(255) DEFAULT '',
    language VARCHAR(100) DEFAULT '',
    gender VARCHAR(20) DEFAULT '',
    location VARCHAR(255) DEFAULT '',
    subscribers BIGINT DEFAULT 0,
    total_videos INT DEFAULT 0,
    avg_long_views BIGINT DEFAULT 0,
    long_engagement_rate DECIMAL(5,2) DEFAULT 0.0,
    avg_long_duration INT DEFAULT 0,
    avg_short_views BIGINT DEFAULT 0,
    short_engagement_rate DECIMAL(5,2) DEFAULT 0.0,
    avg_short_duration INT DEFAULT 0,
    avd VARCHAR(20) DEFAULT '',
    skip_rate VARCHAR(20) DEFAULT '',
    age_13_17 VARCHAR(20) DEFAULT '',
    age_18_24 VARCHAR(20) DEFAULT '',
    age_25_34 VARCHAR(20) DEFAULT '',
    age_35_44 VARCHAR(20) DEFAULT '',
    age_45_54 VARCHAR(20) DEFAULT '',
    male_pct VARCHAR(20) DEFAULT '',
    female_pct VARCHAR(20) DEFAULT '',
    city_1 VARCHAR(100) DEFAULT '',
    city_2 VARCHAR(100) DEFAULT '',
    city_3 VARCHAR(100) DEFAULT '',
    city_4 VARCHAR(100) DEFAULT '',
    city_5 VARCHAR(100) DEFAULT '',
    contact_numbers VARCHAR(255) DEFAULT '',
    mail_id VARCHAR(255) DEFAULT '',
    managed_by VARCHAR(255) DEFAULT '',
    creator_group_id UUID DEFAULT NULL,
    last_scraped_at TIMESTAMP WITH TIME ZONE,
    last_ocr_at TIMESTAMP WITH TIME ZONE,
    last_manual_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_yt_creators_group ON youtube_creators (creator_group_id);
ALTER TABLE youtube_creators ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all_youtube_creators" ON youtube_creators FOR ALL USING (true) WITH CHECK (true);

-- 3. LinkedIn Creators table
CREATE TABLE IF NOT EXISTS linkedin_creators (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    profile_id VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    profile_link TEXT,
    headline VARCHAR(500) DEFAULT '',
    summary TEXT DEFAULT '',
    current_company VARCHAR(255) DEFAULT '',
    current_title VARCHAR(255) DEFAULT '',
    industry VARCHAR(255) DEFAULT '',
    niche VARCHAR(255) DEFAULT '',
    language VARCHAR(100) DEFAULT '',
    gender VARCHAR(20) DEFAULT '',
    location VARCHAR(255) DEFAULT '',
    connections INT DEFAULT 0,
    contact_numbers VARCHAR(255) DEFAULT '',
    mail_id VARCHAR(255) DEFAULT '',
    managed_by VARCHAR(255) DEFAULT '',
    creator_group_id UUID DEFAULT NULL,
    last_scraped_at TIMESTAMP WITH TIME ZONE,
    last_manual_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_li_creators_group ON linkedin_creators (creator_group_id);
ALTER TABLE linkedin_creators ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all_linkedin_creators" ON linkedin_creators FOR ALL USING (true) WITH CHECK (true);
