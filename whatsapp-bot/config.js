/**
 * TRAKR WhatsApp Bot Configuration
 * Change these values to customize bot behavior.
 */
module.exports = {
    // The bot's @mention trigger (without @). Users tag @finbot in groups.
    BOT_NAME: 'finbot',

    // Your Flask API base URL (the Trakr server)
    // In production (Render), both run in the same container so use localhost
    // In development, also localhost
    TRAKR_API_URL: process.env.TRAKR_API_URL || 'http://127.0.0.1:5000',

    // Max data rows to include in a WhatsApp reply (keeps messages readable)
    MAX_ROWS_IN_REPLY: 10,

    // Max characters per WhatsApp message (WhatsApp limit is ~65536)
    MAX_MESSAGE_LENGTH: 4000,

    // Path to store WhatsApp session credentials (for auto-reconnect)
    AUTH_DIR: './auth_info',

    // NVIDIA AI settings for intent classification
    NVIDIA_API_URL: process.env.NVIDIA_API_URL || 'https://integrate.api.nvidia.com/v1',
    NVIDIA_KEY: (process.env.NVIDIA_KEY || '').trim()
};
