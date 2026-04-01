/**
 * TRAKR WhatsApp Bot Configuration
 * Uses env vars in production, falls back to local defaults for development.
 */
// Load environment variables from the root .env file in the parent directory
require('dotenv').config({ path: '../.env' });

module.exports = {
    // The bot's @mention trigger (without @). Users tag @finbot in groups.
    BOT_NAME: process.env.BOT_NAME || 'finbot',

    // Flask API base URL (both local dev and production use localhost)
    TRAKR_API_URL: process.env.TRAKR_API_URL || 'http://127.0.0.1:5000',

    // Max data rows to include in a WhatsApp reply (keeps messages readable)
    MAX_ROWS_IN_REPLY: 10,

    // Max characters per WhatsApp message (WhatsApp limit is ~65536)
    MAX_MESSAGE_LENGTH: 4000,

    // Path to store WhatsApp session credentials (for file-based auth fallback)
    AUTH_DIR: './auth_info',

    // NVIDIA AI settings for intent classification
    NVIDIA_API_URL: process.env.NVIDIA_API_URL || 'https://integrate.api.nvidia.com/v1',
    NVIDIA_KEY: (process.env.NVIDIA_KEY || '').trim(),

    // Supabase credentials for persistent WhatsApp session auth (production)
    USE_SUPABASE_AUTH: process.env.USE_SUPABASE_AUTH === 'true',
    SUPABASE_URL: process.env.SUPABASE_URL || '',
    SUPABASE_KEY: process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY || '',
};
