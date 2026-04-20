-- ═══════════════════════════════════════════════════════════
-- TRAKR — Validation & Whitelist Migration
-- ═══════════════════════════════════════════════════════════
-- Adds:
--   1. allowed_values       — controlled vocabulary for niche + language
--   2. whatsapp_whitelist   — phone numbers allowed to DM/mention the bot
--   3. bot_settings         — feature flags (whitelist_enabled, etc.)
-- Also seeds starter niches + languages so the bot works out of the box.
-- Run this once in the Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════

-- ─── 1. allowed_values (niche + language taxonomy) ─────────
CREATE TABLE IF NOT EXISTS allowed_values (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    category VARCHAR(20) NOT NULL CHECK (category IN ('niche', 'language')),
    value VARCHAR(100) NOT NULL,
    created_by VARCHAR(255) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uniq_category_value UNIQUE (category, value)
);
CREATE INDEX IF NOT EXISTS idx_allowed_values_cat ON allowed_values (category);

ALTER TABLE allowed_values ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "anon_all_allowed_values" ON allowed_values;
CREATE POLICY "anon_all_allowed_values" ON allowed_values FOR ALL USING (true) WITH CHECK (true);

-- ─── 2. whatsapp_whitelist ─────────────────────────────────
-- phone_number stored WITHOUT '+' or spaces (e.g. "919876543210")
CREATE TABLE IF NOT EXISTS whatsapp_whitelist (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    phone_number VARCHAR(30) NOT NULL UNIQUE,
    label VARCHAR(100) DEFAULT '',
    scope VARCHAR(10) DEFAULT 'both' CHECK (scope IN ('dm', 'group', 'both')),
    enabled BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(255) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whatsapp_whitelist_phone ON whatsapp_whitelist (phone_number);

ALTER TABLE whatsapp_whitelist ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "anon_all_whitelist" ON whatsapp_whitelist;
CREATE POLICY "anon_all_whitelist" ON whatsapp_whitelist FOR ALL USING (true) WITH CHECK (true);

-- ─── 3. bot_settings (key-value) ────────────────────────────
CREATE TABLE IF NOT EXISTS bot_settings (
    key VARCHAR(64) PRIMARY KEY,
    value TEXT DEFAULT '',
    updated_by VARCHAR(255) DEFAULT '',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE bot_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "anon_all_bot_settings" ON bot_settings;
CREATE POLICY "anon_all_bot_settings" ON bot_settings FOR ALL USING (true) WITH CHECK (true);

-- Default: whitelist is OFF so nothing breaks on first deploy.
-- Admin flips it ON from the WhatsApp tab when ready.
INSERT INTO bot_settings (key, value) VALUES ('whitelist_enabled', 'false')
ON CONFLICT (key) DO NOTHING;

-- ─── Seed: basic niches ────────────────────────────────────
INSERT INTO allowed_values (category, value) VALUES
  ('niche', 'Finance'),
  ('niche', 'Beauty'),
  ('niche', 'Lifestyle'),
  ('niche', 'Fitness'),
  ('niche', 'Comedy'),
  ('niche', 'Food'),
  ('niche', 'Travel'),
  ('niche', 'Tech'),
  ('niche', 'Gaming'),
  ('niche', 'Fashion'),
  ('niche', 'Education'),
  ('niche', 'Entertainment'),
  ('niche', 'Business'),
  ('niche', 'Health'),
  ('niche', 'Parenting'),
  ('niche', 'Music'),
  ('niche', 'Sports')
ON CONFLICT (category, value) DO NOTHING;

-- ─── Seed: basic languages ─────────────────────────────────
INSERT INTO allowed_values (category, value) VALUES
  ('language', 'Hindi'),
  ('language', 'English'),
  ('language', 'Tamil'),
  ('language', 'Telugu'),
  ('language', 'Kannada'),
  ('language', 'Malayalam'),
  ('language', 'Marathi'),
  ('language', 'Bengali'),
  ('language', 'Gujarati'),
  ('language', 'Punjabi'),
  ('language', 'Urdu'),
  ('language', 'Odia'),
  ('language', 'Assamese')
ON CONFLICT (category, value) DO NOTHING;
