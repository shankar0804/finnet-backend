-- ═══════════════════════════════════════════════════════════
-- TRAKR — Campaign Entry Enrichment Migration
-- ═══════════════════════════════════════════════════════════
-- Adds per-post metric columns to campaign_entries so a single
-- entry can carry the scraped/OCR stats (views, likes, comments
-- etc.) alongside the base booking info.
--
-- Populated by the /api/entries flow after scraping a reel/video
-- URL or OCRing a screenshot. Platform is stamped so the dashboard
-- can group entries by IG / YouTube / LinkedIn.
--
-- Run once in the Supabase SQL editor.
-- ═══════════════════════════════════════════════════════════

ALTER TABLE campaign_entries
  ADD COLUMN IF NOT EXISTS platform          VARCHAR(20)  DEFAULT 'instagram',
  ADD COLUMN IF NOT EXISTS video_views       BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS play_count        BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS likes             BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS comments          BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS shares            BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS saves             BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS impressions       BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reacts            BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reshares          BIGINT       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS duration_secs     INT          DEFAULT 0,
  ADD COLUMN IF NOT EXISTS engagement_rate   DECIMAL(5,2) DEFAULT 0.0,
  ADD COLUMN IF NOT EXISTS post_timestamp    TIMESTAMPTZ,
  -- data_source: 'scrape' (URL) | 'ocr' (screenshot) | 'manual' (user form)
  ADD COLUMN IF NOT EXISTS data_source       VARCHAR(20)  DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS last_enriched_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_entries_platform ON campaign_entries (platform);
