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
