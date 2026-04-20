/**
 * TRAKR WhatsApp Bot — Baileys
 *
 * Two modes:
 * 1. @finbot <query>      — AI search against the influencer database
 * 2. @finbot update       — Scrape Instagram link from chat + OCR screenshots if present
 *
 * Rules:
 * - Instagram link is ALWAYS required for update commands
 * - If screenshots present: Scrape + OCR
 * - If no screenshots: Scrape only
 */

const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    downloadMediaMessage,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const qrcode = require('qrcode-terminal');
const qrcodeLib = require('qrcode');
const FormData = require('form-data');
const http = require('http');
const { BOT_NAME, TRAKR_API_URL, MAX_ROWS_IN_REPLY, MAX_MESSAGE_LENGTH, AUTH_DIR, SUPABASE_URL, SUPABASE_KEY, USE_SUPABASE_AUTH } = require('./config');
const { classifyIntent } = require('./agent');
const {
    getAllowed,
    canonicalValue,
    formatAllowedList,
    isSenderAllowed,
    parseInlineUpdates,
    refreshAllowed,
    refreshWhitelist,
    extractCreatorTargets,
} = require('./validator');

// ─── Platform helpers (Instagram / YouTube / LinkedIn) ─────────
const PLATFORM_LABELS = {
    instagram: 'Instagram',
    youtube: 'YouTube',
    linkedin: 'LinkedIn',
};

function platformPrefix(platform) {
    if (platform === 'youtube') return 'YT @';
    if (platform === 'linkedin') return 'LinkedIn ';
    return '@';
}

function prettyTarget(platform, handle) {
    return `${platformPrefix(platform)}${handle}`;
}

// ─── Awaiting-platform clarification state (AI search) ─────────
// jid -> { originalQuery, expiresAt }
const pendingClarifications = new Map();
const CLARIFY_TTL_MS = 5 * 60 * 1000;

function parsePlatformReply(text) {
    if (!text) return null;
    const t = text.trim().toLowerCase();
    if (/\b(instagram|insta|ig)\b/.test(t)) return 'instagram';
    if (/\b(youtube|yt)\b/.test(t)) return 'youtube';
    if (/\b(linkedin|li)\b/.test(t)) return 'linkedin';
    return null;
}

// Keywords that abort any ongoing multi-step flow (scrape/OCR/bulk import) for this chat
const QUIT_KEYWORDS = new Set(['quit', 'cancel', 'stop', 'abort', 'exit', 'nevermind', 'never mind', 'new query']);
function isQuitMessage(text) {
    if (!text) return false;
    const t = text.trim().toLowerCase().replace(/[.!?]+$/, '');
    if (QUIT_KEYWORDS.has(t)) return true;
    // Also catch "@finbot quit" forms
    const stripped = t.replace(new RegExp(`^@?${BOT_NAME}\\s+`, 'i'), '').trim();
    return QUIT_KEYWORDS.has(stripped);
}

async function resetChatState(jid, sock, quoted) {
    // 1) Pending scrape / OCR queue
    let hadState = false;
    if (pendingScrapes.has(jid)) {
        pendingScrapes.delete(jid);
        hadState = true;
    }
    // 2) In-flight bulk imports for this chat
    const toCancel = [];
    for (const [jobId, info] of bulkImportJobs.entries()) {
        if (info.jid === jid) toCancel.push(jobId);
    }
    for (const jobId of toCancel) {
        try {
            await fetch(`${TRAKR_API_URL}/api/bulk-import/${jobId}/cancel`, { method: 'POST' });
            bulkImportJobs.delete(jobId);
            hadState = true;
        } catch (e) { /* ignore */ }
    }
    // 3) Clear chat history so old screenshots/links don't leak into the next query
    chatHistory.delete(jid);

    // 4) Drop any pending AI-search platform clarification
    if (pendingClarifications.has(jid)) {
        pendingClarifications.delete(jid);
        hadState = true;
    }

    if (sock) {
        const text = hadState
            ? '🛑 Cancelled — previous session cleared. Ask me anything to start fresh.'
            : '✅ Nothing in progress. Ready for your next query.';
        try {
            if (quoted) {
                await sock.sendMessage(jid, { text }, { quoted });
            } else {
                await sock.sendMessage(jid, { text });
            }
        } catch (e) { /* ignore */ }
    }
}

// Supabase auth (only loaded when SUPABASE_URL is configured)
let createClient, useSupabaseAuthState;
if (USE_SUPABASE_AUTH && SUPABASE_URL && SUPABASE_KEY) {
    createClient = require('@supabase/supabase-js').createClient;
    useSupabaseAuthState = require('./supabaseAuth');
    console.log('🔐 Auth mode: Supabase (persistent)');
} else {
    console.log('🔐 Auth mode: Local file system');
}

const logger = pino({ level: 'warn' });

// ─── Internal Status Server (port 3001) ───
// Flask proxy hits this to report status/QR to the frontend UI
let botState = { state: 'offline', qr: null, qrBase64: null, phone: null };

const STATUS_PORT = process.env.BOT_STATUS_PORT || 3001;
const statusServer = http.createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Access-Control-Allow-Origin', '*');

    if (req.url === '/status' || req.url === '/') {
        res.writeHead(200);
        res.end(JSON.stringify(botState));
    } else if (req.url === '/qr') {
        res.writeHead(200);
        res.end(JSON.stringify({ qr: botState.qr, qrBase64: botState.qrBase64, state: botState.state }));
    } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not found' }));
    }
});

const BIND_HOST = process.env.BIND_HOST || '0.0.0.0';
statusServer.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.log(`⚠️ Port ${STATUS_PORT} in use — status server skipped (bot still works)`);
    } else {
        console.error('Status server error:', err);
    }
});
statusServer.listen(STATUS_PORT, BIND_HOST, () => {
    console.log(`📡 Bot status server running on http://${BIND_HOST}:${STATUS_PORT}`);
});

// ─── Personality ───
const REACTIONS = {
    thinking: ['🧠', '⚡', '🔮', '🤖', '👀', '🔍'],
    success: ['🔥', '✨', '💪', '🚀', '🎯', '⭐'],
    fail: ['💀', '😬', '🫠'],
};
function randomFrom(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

const QUIPS = {
    scrapeStart: [
        '🕵️ On it boss! Stalking their profile (legally)...',
        '⚡ Firing up the scraper... time to do some recon.',
        '🔥 Let me work my magic on this one...',
        '🤖 *cracks knuckles* Let\'s see what we got...',
    ],
    scrapeSuccess: [
        '✅ Boom! Got \'em locked in the database.',
        '✅ Profile secured! Another one for the roster.',
        '✅ Done and dusted. They\'re in the system now.',
    ],
    ocrStart: [
        '📸 Reading those screenshots like a hawk...',
        '🧠 Engaging my AI eyes on those screenshots...',
        '📸 Analyzing the screenshots... my OCR game is strong.',
    ],
    exportStart: [
        '📊 Cooking up a fresh spreadsheet for you...',
        '📊 Say no more, generating that sheet...',
        '📊 One Google Sheet, coming right up chef! 🧑‍🍳',
    ],
    searchStart: [
        '🔍', '🧠', '⚡',
    ],
    noLink: [
        '🤔 Hold up — I need an Instagram link to work with! Drop the link first, then tag me.',
        '😅 I\'m good but not psychic! Share the Instagram link first, then hit me up.',
        '🔗 Missing the Instagram link! Send it in the chat, then tag me again.',
    ],
    noScreenshots: [
        '_No screenshots spotted — went with scrape only. Drop some analytics screenshots next time for the full treatment!_',
        '_Only scraped the profile since I didn\'t see any screenshots. Want the OCR data too? Send those screenshots!_',
    ],
};
function quip(key) { return randomFrom(QUIPS[key]); }

// Store recent messages per chat (last 50) — works for both groups and DMs
const chatHistory = new Map();
const MAX_HISTORY = 50;

function addToHistory(jid, msg) {
    if (!chatHistory.has(jid)) chatHistory.set(jid, []);
    const history = chatHistory.get(jid);
    history.push(msg);
    if (history.length > MAX_HISTORY) history.shift();
}

// ─── Pending Scrape State Machine ───
// Tracks per-chat scrape flows that need mandatory field input
const pendingScrapes = new Map();

// Track bulk import jobs: job_id -> { jid, msgKey }
const bulkImportJobs = new Map();
// Key: jid, Value: { queue: [{username, scraped}], current: {username, step, data}, sock }

// ─── Per-JID message serialization ───
// Messages from the SAME user are handled strictly in order (prevents
// double-scrapes, out-of-order OCR, and interleaved state-machine bugs
// when a user taps send multiple times rapidly). Messages from DIFFERENT
// users still process in parallel.
const perJidChain = new Map();
function enqueueForJid(jid, task) {
    const prev = perJidChain.get(jid) || Promise.resolve();
    const next = prev.then(() => task()).catch((err) => {
        console.error(`[PER-JID ${jid}] task error:`, err);
    });
    perJidChain.set(jid, next);
    next.finally(() => {
        if (perJidChain.get(jid) === next) perJidChain.delete(jid);
    });
    return next;
}

/**
 * Extract first Instagram username from text.
 */
function extractInstagramUsername(text) {
    if (!text) return null;
    const urlMatch = text.match(/instagram\.com\/(?:reel\/|p\/)?([A-Za-z0-9_.]+)/i);
    if (urlMatch) return urlMatch[1].split('?')[0].split('/')[0];
    return null;
}

/**
 * Extract ALL Instagram usernames from text (for bulk link dumps).
 */
function extractAllInstagramUsernames(text) {
    if (!text) return [];
    const matches = text.matchAll(/instagram\.com\/(?:reel\/|p\/)?([A-Za-z0-9_.]+)/gi);
    const usernames = [];
    for (const m of matches) {
        const u = m[1].split('?')[0].split('/')[0];
        if (u && !usernames.includes(u)) usernames.push(u);
    }
    return usernames;
}

/**
 * Find screenshots from recent chat history.
 *
 * Policy: only pick up images that were sent as a CONTIGUOUS batch IMMEDIATELY
 * before the current trigger message. As soon as we hit a text/non-image
 * message, we stop. This prevents screenshots from older, unrelated flows
 * from leaking into the current one.
 *
 * Also caps to the last 10 to avoid runaway batches.
 */
function findRecentScreenshots(jid) {
    const history = chatHistory.get(jid) || [];
    const images = [];
    // Start just before the current trigger message (last entry in history)
    for (let i = history.length - 2; i >= 0 && images.length < 10; i--) {
        const m = history[i]?.message;
        if (!m) break;
        const isImage = !!(m.imageMessage || m.ephemeralMessage?.message?.imageMessage);
        if (!isImage) break;           // stop at first non-image — no "gap skipping"
        images.unshift(history[i]);
    }
    return images;
}

/**
 * Find first IG username from recent history.
 */
function findUsernameFromHistory(jid) {
    const history = chatHistory.get(jid) || [];
    for (let i = history.length - 1; i >= 0; i--) {
        const msg = history[i];
        const text = msg.message?.conversation
            || msg.message?.extendedTextMessage?.text
            || msg.message?.imageMessage?.caption
            || '';
        if (text) {
            const username = extractInstagramUsername(text);
            if (username) return username;
        }
        if ((history.length - 1 - i) > 10) break;
    }
    return null;
}

/**
 * Call the Trakr scraper API for the correct platform (Apify under the hood).
 * Accepts either a plain string (legacy; treated as Instagram) or a {platform, handle}.
 */
async function callScraper(target) {
    const platform = (typeof target === 'object' && target?.platform) || 'instagram';
    const handle = typeof target === 'string' ? target : (target?.handle || '');

    let url, body;
    if (platform === 'youtube') {
        url = `${TRAKR_API_URL}/api/scrape-youtube`;
        body = { channel: handle };
    } else if (platform === 'linkedin') {
        url = `${TRAKR_API_URL}/api/scrape-linkedin`;
        body = { profile: handle };
    } else {
        url = `${TRAKR_API_URL}/api/scrape-instagram`;
        body = { username: handle };
    }

    try {
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!res.ok) {
            return {
                success: false,
                platform,
                error: data.error || data.details || 'Scrape failed',
                details: data.details || '',
            };
        }
        return { success: true, platform, data };
    } catch (err) {
        return { success: false, platform, error: err.message };
    }
}

/**
 * Download a WhatsApp image and send it to the Trakr OCR API.
 */
async function processImage(msg, targetUsername) {
    try {
        console.log(`📸 [OCR] Downloading image for @${targetUsername}...`);
        const buffer = await downloadMediaMessage(msg, 'buffer', {});
        if (!buffer || buffer.length === 0) {
            console.error(`📸 [OCR] Download failed: empty buffer`);
            return { success: false, error: 'Failed to download image (empty buffer)' };
        }
        console.log(`📸 [OCR] Downloaded ${(buffer.length / 1024).toFixed(1)}KB, sending to OCR...`);

        const form = new FormData();
        form.append('image', buffer, {
            filename: 'screenshot.jpg',
            contentType: msg.message?.imageMessage?.mimetype || 'image/jpeg',
        });
        form.append('target_username', targetUsername);

        return new Promise((resolve) => {
            const req = form.submit(`${TRAKR_API_URL}/api/upload`, (err, res) => {
                if (err) {
                    console.error(`📸 [OCR] Upload error: ${err.message}`);
                    resolve({ success: false, error: `Upload failed: ${err.message}` });
                    return;
                }
                let body = '';
                res.on('data', (chunk) => body += chunk);
                res.on('end', () => {
                    console.log(`📸 [OCR] Server responded: ${res.statusCode} (${body.length} bytes)`);
                    try {
                        const data = JSON.parse(body);
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve({ success: true, result: data.result });
                        } else {
                            resolve({ success: false, error: data.error || `OCR failed (HTTP ${res.statusCode})` });
                        }
                    } catch (e) {
                        console.error(`📸 [OCR] Invalid server response: ${body.slice(0, 200)}`);
                        resolve({ success: false, error: 'Invalid server response' });
                    }
                });
                res.on('error', (e) => {
                    console.error(`📸 [OCR] Response error: ${e.message}`);
                    resolve({ success: false, error: `Response error: ${e.message}` });
                });
            });
            req.on('error', (e) => {
                console.error(`📸 [OCR] Request error: ${e.message}`);
                resolve({ success: false, error: `Request failed: ${e.message}` });
            });
        });
    } catch (err) {
        console.error(`📸 [OCR] Exception: ${err.message}`);
        return { success: false, error: err.message };
    }
}

/**
 * Fetch all roster data and export to Google Sheet.
 */
async function exportToSheet(searchQuery) {
    const MAX_RETRIES = 2;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            let rows;
            let title;

            if (searchQuery) {
                const searchResult = await queryTrakr(searchQuery);
                if (!searchResult || searchResult.type === 'error' || !searchResult.data?.length) {
                    return { success: false, error: 'No data found for that query.' };
                }
                rows = searchResult.data;
                title = `TRAKR Export: ${searchQuery}`;
            } else {
                const controller1 = new AbortController();
                const timeout1 = setTimeout(() => controller1.abort(), 30000);
                const res = await fetch(`${TRAKR_API_URL}/api/roster`, { signal: controller1.signal });
                clearTimeout(timeout1);
                if (!res.ok) return { success: false, error: 'Failed to fetch roster data' };
                const data = await res.json();
                rows = data;
                title = `TRAKR Full Roster Export`;
            }

            if (!rows || rows.length === 0) {
                return { success: false, error: 'No data to export.' };
            }

            // Export with 60s timeout (Google Sheets API can be slow)
            const controller2 = new AbortController();
            const timeout2 = setTimeout(() => controller2.abort(), 60000);
            const exportRes = await fetch(`${TRAKR_API_URL}/api/export-to-sheet`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ data: rows, title }),
                signal: controller2.signal,
            });
            clearTimeout(timeout2);

            const exportData = await exportRes.json();
            if (!exportRes.ok) {
                throw new Error(exportData.error || 'Export failed');
            }

            return { success: true, url: exportData.sheet_url, count: rows.length };
        } catch (err) {
            console.error(`[EXPORT] Attempt ${attempt}/${MAX_RETRIES} failed: ${err.message}`);
            if (attempt === MAX_RETRIES) {
                return { success: false, error: `Export failed after ${MAX_RETRIES} attempts: ${err.message}` };
            }
            // Wait 2s before retry
            await new Promise(r => setTimeout(r, 2000));
        }
    }
}

// ─── AI Search Formatting ───

// Default columns to show in search results
const DEFAULT_DISPLAY_COLS = ['username', 'niche', 'followers', 'avg_views'];

function formatNumber(n) {
    if (isNaN(n) || n === null || n === undefined || n === '') return '-';
    n = parseInt(n);
    if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
    if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
    return `${n}`;
}

function formatReply(data) {
    if (!data || data.type === 'error') {
        return `❌ *Error:* ${data?.message || 'Something went wrong.'}`;
    }

    if (data.type === 'clarify') {
        return `🤔 ${data.message || 'Which platform did you mean — Instagram, YouTube, or LinkedIn?'}\n\n_Reply with one of:_ *instagram* / *youtube* / *linkedin*`;
    }

    let msg = '';

    if (data.data && data.data.length > 0) {
        const allRows = data.data;
        const platform = data.platform || 'instagram';
        const platformLabel = platform === 'youtube' ? 'YouTube' : platform === 'linkedin' ? 'LinkedIn' : 'Instagram';

        msg += `📊 *${platformLabel} Results* (${allRows.length} found):\n\n`;

        allRows.forEach((row, i) => {
            let line;
            if (platform === 'youtube') {
                const name = row.channel_name || row.channel_handle || 'Unknown';
                const handle = row.channel_handle ? `@${row.channel_handle}` : '';
                const niche = row.niche || '-';
                const subs = formatNumber(row.subscribers);
                const views = formatNumber(row.avg_long_views);
                line = `*${i + 1}. ${name}* ${handle}\n   Niche: ${niche} | Subs: ${subs} | Avg Long Views: ${views}\n\n`;
            } else if (platform === 'linkedin') {
                const name = row.full_name || row.profile_id || 'Unknown';
                const handle = row.profile_id ? `(${row.profile_id})` : '';
                const headline = row.headline ? `\n   _${row.headline}_` : '';
                const company = row.current_company ? `\n   🏢 ${row.current_company}` : '';
                const conns = formatNumber(row.connections);
                line = `*${i + 1}. ${name}* ${handle}${headline}${company}\n   🔗 Connections: ${conns}\n\n`;
            } else {
                const name = row.creator_name || row.username || 'Unknown';
                const username = row.username ? `@${row.username}` : '';
                const niche = row.niche || '-';
                const followers = formatNumber(row.followers);
                const avgViews = formatNumber(row.avg_views);
                line = `*${i + 1}. ${name}* ${username}\n   Niche: ${niche} | Followers: ${followers} | Avg Views: ${avgViews}\n\n`;
            }

            // Safety: if message is getting too long, stop adding rows
            if (msg.length + line.length > MAX_MESSAGE_LENGTH - 200) {
                msg += `\n... _(${allRows.length - i} more — use dashboard for full list)_\n`;
                return;
            }
            msg += line;
        });

        // Prepend insight if present and concise
        if (data.insight && data.insight.length > 0 && data.insight.length < 600) {
            msg = `💡 ${data.insight}\n\n` + msg;
        }
    } else {
        msg += data.insight || '📭 No results found.';
    }

    if (msg.length > MAX_MESSAGE_LENGTH) {
        msg = msg.slice(0, MAX_MESSAGE_LENGTH - 50) + '\n\n... _(use dashboard for full results)_';
    }
    return msg.trim();
}

async function queryTrakr(query, platform = null) {
    try {
        const body = { query };
        if (platform) body.platform = platform;
        const res = await fetch(`${TRAKR_API_URL}/api/custom-search`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            return { type: 'error', message: err.details || err.error || `API ${res.status}` };
        }
        const data = await res.json();
        return data.answer || { type: 'error', message: 'No answer' };
    } catch (err) {
        return { type: 'error', message: `Server unreachable: ${err.message}` };
    }
}

// ─── Bot Core ───

async function startBot() {
    // ─── AUTO-DETECT AUTH MODE ───
    let state, saveCreds, supabase;

    if (USE_SUPABASE_AUTH) {
        // Production: Supabase-backed session persistence
        supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
        const authResult = await useSupabaseAuthState(supabase, 'wa_');
        state = authResult.state;
        saveCreds = authResult.saveCreds;
        console.log('✅ Supabase auth state loaded');
    } else {
        // Local dev: file-based auth
        const authResult = await useMultiFileAuthState(AUTH_DIR);
        state = authResult.state;
        saveCreds = authResult.saveCreds;
        console.log('✅ File auth state loaded from', AUTH_DIR);
    }

    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
        version,
        auth: {
            creds: state.creds,
            keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        logger,
        printQRInTerminal: false,
        generateHighQualityLinkPreview: false,
    });

    let reconnectAttempt = 0;
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            console.log('\n📱 Scan this QR code with WhatsApp:\n');
            qrcode.generate(qr, { small: true });
            console.log('\nWaiting for scan...\n');

            // Generate base64 QR image for the web UI
            try {
                const qrBase64 = await qrcodeLib.toDataURL(qr, { width: 280, margin: 2 });
                botState = { state: 'qr', qr, qrBase64, phone: null };
            } catch (e) {
                console.error('QR base64 generation failed:', e.message);
                botState = { state: 'qr', qr, qrBase64: null, phone: null };
            }
        }
        if (connection === 'close') {
            const code = lastDisconnect?.error?.output?.statusCode;
            const retry = code !== DisconnectReason.loggedOut;
            console.log(`❌ Closed (${code}). ${retry ? 'Reconnecting...' : 'Logged out. Clearing session...'}`);
            if (retry) {
                reconnectAttempt++;
                // Exponential backoff: conflict/timeout waits longer
                const isConflict = code === 440 || code === 408 || code === 500 || code === 503;
                const baseDelay = isConflict ? 5000 : 2000;
                const delay = Math.min(baseDelay * Math.pow(1.5, reconnectAttempt - 1), 30000);
                console.log(`Waiting ${(delay/1000).toFixed(1)}s before reconnect (attempt ${reconnectAttempt})...`);
                botState = { state: 'reconnecting', qr: null, qrBase64: null, phone: null };
                setTimeout(() => startBot(), delay);
            } else {
                botState = { state: 'logged_out', qr: null, qrBase64: null, phone: null };
                // Clear session based on auth mode
                if (USE_SUPABASE_AUTH && supabase) {
                    try {
                        await supabase.from('whatsapp_auth').delete().like('file_name', 'wa_%');
                    } catch (e) {
                        console.error('Failed to clear DB auth session', e);
                    }
                } else {
                    const fs = require('fs');
                    if (fs.existsSync(AUTH_DIR)) {
                        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
                    }
                }
                console.log('🔄 Session cleared. Restarting bot for new QR scan...');
                startBot();
            }
        }
        if (connection === 'open') {
            reconnectAttempt = 0;
            const phoneNumber = sock.user?.id?.split(':')[0] || sock.user?.id?.split('@')[0] || null;
            botState = { state: 'connected', qr: null, qrBase64: null, phone: phoneNumber };
            console.log('✅ WhatsApp connected! Bot is live.');
            console.log(`🤖 Listening for @${BOT_NAME} in groups...\n`);
        }
    });

    sock.ev.on('creds.update', saveCreds);

    _botSocket = sock;
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        // Fire each message into its per-JID queue without awaiting — different
        // users process in parallel, same user processes strictly in order.
        for (const msg of messages) {
            const _jid = msg?.key?.remoteJid;
            if (!_jid) continue;
            enqueueForJid(_jid, () => handleIncomingMessage(sock, msg));
        }
    });

    async function handleIncomingMessage(sock, msg) {
        // Single-iteration inner loop: lets the legacy `continue;` statements
        // inside the handler act as early-exits from this message without
        // us having to rewrite 30+ call sites.
        for (const _ of [0]) {
            try {
                console.log(`\n🔍 [DEBUG] RAW MSG received from: ${msg.key.remoteJid} | fromMe: ${msg.key.fromMe}`);

                const jid = msg.key.remoteJid;
                const isGroup = jid?.endsWith('@g.us');
                const isDM = jid?.endsWith('@s.whatsapp.net') || jid?.endsWith('@lid');
                
                if (!isGroup && !isDM) {
                    console.log(`🚫 [DEBUG] Skipped: Not a Group or DM (JID: ${jid})`);
                    continue;
                }

                // Store ALL messages in history
                addToHistory(jid, msg);

                if (msg.key.fromMe) {
                    console.log(`🚫 [DEBUG] Skipped: fromMe is true (Bot won't reply to itself)`);
                    continue;
                }

                // ─── WHITELIST GATE ───
                // If enforcement is ON and sender isn't in the whitelist, silently ignore.
                try {
                    const gate = await isSenderAllowed(msg);
                    if (gate.enabled && !gate.allowed) {
                        console.log(`🚫 [WHITELIST] Blocked sender ${gate.phone || '(unknown)'} for jid ${jid}`);
                        continue;
                    }
                } catch (e) {
                    console.warn('[WHITELIST] Gate check failed, allowing message:', e.message);
                }

                // Extract text from ALL possible WhatsApp message types (including expiring/ephemeral messages)
                const text = msg.message?.conversation
                    || msg.message?.extendedTextMessage?.text
                    || msg.message?.imageMessage?.caption
                    || msg.message?.videoMessage?.caption
                    || msg.message?.ephemeralMessage?.message?.extendedTextMessage?.text
                    || msg.message?.ephemeralMessage?.message?.conversation
                    || msg.message?.ephemeralMessage?.message?.imageMessage?.caption
                    || '';

                const hasImage = !!(msg.message?.imageMessage || msg.message?.ephemeralMessage?.message?.imageMessage);
                console.log(`💬 [DEBUG] Extracted Text: "${text}" | Has Image: ${hasImage}`);

                // ─── QUIT / CANCEL KEYWORD ───
                // Fully resets any pending state (scrape queue, bulk import, history).
                if (text && isQuitMessage(text)) {
                    console.log(`🛑 [QUIT] Resetting state for ${jid}`);
                    pendingClarifications.delete(jid);
                    await resetChatState(jid, sock, msg);
                    continue;
                }

                // ─── PENDING PLATFORM CLARIFICATION ───
                // If we recently asked "which platform?", treat this reply as the answer
                // and re-run the original query with the chosen platform.
                if (text && pendingClarifications.has(jid)) {
                    const pc = pendingClarifications.get(jid);
                    if (pc.expiresAt && Date.now() > pc.expiresAt) {
                        pendingClarifications.delete(jid);
                    } else {
                        const chosen = parsePlatformReply(text);
                        if (chosen) {
                            pendingClarifications.delete(jid);
                            await sock.sendMessage(jid, { react: { text: '🔎', key: msg.key } });
                            const result = await queryTrakr(pc.originalQuery, chosen);
                            await sock.sendMessage(jid, { text: formatReply(result) }, { quoted: msg });
                            await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                            continue;
                        }
                        // Not a platform keyword — let the normal pipeline handle it
                        // but clear the pending state so we don't keep asking.
                        pendingClarifications.delete(jid);
                    }
                }

                // ─── HANDLE SCREENSHOTS DURING PENDING SCRAPE ───
                // If we're awaiting screenshots and user sends an image, process it immediately
                if (hasImage && pendingScrapes.has(jid)) {
                    const pending = pendingScrapes.get(jid);
                    if (pending.current && pending.current.step === 'awaiting_screenshots') {
                        const username = pending.current.username;
                        console.log(`📸 [PENDING] Screenshot received for @${username}, processing...`);
                        
                        // Get the actual image message (could be in ephemeral wrapper)
                        const imgMsg = msg.message?.imageMessage 
                            ? msg 
                            : { ...msg, message: { imageMessage: msg.message?.ephemeralMessage?.message?.imageMessage } };
                        
                        const result = await processImage(imgMsg, username);
                        if (result.success) {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.success)} Screenshot processed for *@${username}*! Send more screenshots or type "done" to finish.` }, { quoted: msg });
                        } else {
                            console.error(`  OCR failed: ${result.error}`);
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} Couldn't read that screenshot. Try a clearer one, or type "done" to move on.` }, { quoted: msg });
                        }
                        continue;
                    }
                }

                // Store images even without text (for screenshot collection)
                if (!text && hasImage) {
                    console.log(`📸 [DEBUG] Image without text — storing in history and skipping reply.`);
                    continue;
                }
                
                if (!text) {
                    console.log(`🚫 [DEBUG] Skipped: No readable text found. Available message keys:`, msg.message ? Object.keys(msg.message) : 'none');
                    continue;
                }

                // ─── CHECK PENDING SCRAPE STATE ───
                // If we're waiting for mandatory fields from this chat, handle that first
                if (pendingScrapes.has(jid)) {
                    const pending = pendingScrapes.get(jid);
                    if (pending.current && pending.current.step === 'awaiting_mandatory') {
                        // Parse the user's response for language, niche, gender
                        const lines = text.split('\n').map(l => l.trim()).filter(l => l);
                        let language = '', niche = '', gender = '';

                        for (const line of lines) {
                            const lower = line.toLowerCase();
                            if (lower.startsWith('language') || lower.startsWith('lang')) {
                                language = line.split(/[:\-]\s*/)[1]?.trim() || line.replace(/language\s*/i, '').trim();
                            } else if (lower.startsWith('niche') || lower.startsWith('nich')) {
                                niche = line.split(/[:\-]\s*/)[1]?.trim() || line.replace(/niche\s*/i, '').trim();
                            } else if (lower.startsWith('gender') || lower.startsWith('gen')) {
                                gender = line.split(/[:\-]\s*/)[1]?.trim() || line.replace(/gender\s*/i, '').trim();
                            }
                        }

                        // If couldn't parse structured input, try single-line "Hindi, Finance, Male" format
                        if (!language && !niche && !gender && lines.length === 1) {
                            const parts = text.split(/[,\/|]+/).map(p => p.trim());
                            if (parts.length >= 3) {
                                language = parts[0];
                                niche = parts[1];
                                gender = parts[2];
                            }
                        }

                        // Merge in any fields the user already gave us earlier (extra_updates)
                        const pre = pending.current.prefilled || {};
                        if (!language && pre.language) language = pre.language;
                        if (!niche && pre.niche) niche = pre.niche;
                        if (!gender && pre.gender) gender = pre.gender;

                        if (!language || !niche || !gender) {
                            const missing = [];
                            if (!language) missing.push('Language');
                            if (!niche) missing.push('Niche');
                            if (!gender) missing.push('Gender');
                            await sock.sendMessage(jid, {
                                text: `⚠️ Still need: *${missing.join(', ')}*.\n\nReply like this:\nLanguage: Hindi\nNiche: Finance\nGender: Male\n\n_Or one line:_ Hindi, Finance, Male`
                            }, { quoted: msg });
                            continue;
                        }

                        // ── VALIDATE against allowed lists ──
                        const [canonLang, canonNiche, canonGender] = await Promise.all([
                            canonicalValue('language', language),
                            canonicalValue('niche', niche),
                            canonicalValue('gender', gender),
                        ]);
                        const invalid = [];
                        if (!canonLang) invalid.push({ f: 'Language', given: language, allowed: await getAllowed('language') });
                        if (!canonNiche) invalid.push({ f: 'Niche', given: niche, allowed: await getAllowed('niche') });
                        if (!canonGender) invalid.push({ f: 'Gender', given: gender, allowed: await getAllowed('gender') });

                        if (invalid.length) {
                            let txt = `❌ These values aren't allowed:\n`;
                            for (const inv of invalid) {
                                txt += `\n• *${inv.f}:* "${inv.given}"\n   _Allowed:_ ${formatAllowedList(inv.allowed)}`;
                            }
                            txt += `\n\n🔁 Please reply again with valid values.`;
                            await sock.sendMessage(jid, { text: txt }, { quoted: msg });
                            continue;
                        }

                        const username = pending.current.username;
                        const platform = pending.current.platform || 'instagram';
                        const label = prettyTarget(platform, username);
                        try {
                            const res = await fetch(`${TRAKR_API_URL}/api/update-fields`, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    username,
                                    platform,
                                    updates: [
                                        { field: 'language', value: canonLang },
                                        { field: 'niche',    value: canonNiche },
                                        { field: 'gender',   value: canonGender },
                                    ]
                                })
                            });
                            const data = await res.json();
                            if (!res.ok || !data.success) {
                                throw new Error(data.error || data.details || 'update failed');
                            }
                            // OCR/screenshot step only applies to Instagram
                            if (platform === 'instagram') {
                                await sock.sendMessage(jid, {
                                    text: `${randomFrom(REACTIONS.success)} *${label}* updated!\n   Language: ${canonLang}\n   Niche: ${canonNiche}\n   Gender: ${canonGender}\n\n📸 _Want to share analytics screenshots for more data? Send them now, or type "skip" to move on. Type "quit" to start over._`
                                }, { quoted: msg });
                                pending.current.step = 'awaiting_screenshots';
                            } else {
                                await sock.sendMessage(jid, {
                                    text: `${randomFrom(REACTIONS.success)} *${label}* updated!\n   Language: ${canonLang}\n   Niche: ${canonNiche}\n   Gender: ${canonGender}`
                                }, { quoted: msg });
                                // Move on — no OCR step for YT/LI
                                const next = pending.queue.shift();
                                if (next) {
                                    await processScrapeForUser(sock, jid, msg, next, pending);
                                } else {
                                    pendingScrapes.delete(jid);
                                }
                            }
                        } catch (e) {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} Failed to update fields: ${e.message}` }, { quoted: msg });
                            pendingScrapes.delete(jid);
                        }
                        continue;
                    }

                    if (pending.current && pending.current.step === 'awaiting_screenshots') {
                        const lower = text.toLowerCase().trim();
                        if (lower === 'skip' || lower === 'no' || lower === 'next' || lower === 'done') {
                            const next = pending.queue.shift();
                            if (next) {
                                await processScrapeForUser(sock, jid, msg, next, pending);
                            } else {
                                await sock.sendMessage(jid, { text: `✅ *All done!* All creators have been processed. 🎉` }, { quoted: msg });
                                pendingScrapes.delete(jid);
                            }
                            continue;
                        }

                        const curLabel = prettyTarget(pending.current.platform || 'instagram', pending.current.username);
                        await sock.sendMessage(jid, {
                            text: `📸 I'm waiting for analytics screenshots for *${curLabel}*. Send screenshot images, or type "done" to move on.`
                        }, { quoted: msg });
                        continue;
                    }
                }

                // ─── ACTIVATION CHECK ───
                const mentionRe = new RegExp(`\\b${BOT_NAME}\\b`, 'i');
                if (isGroup && !mentionRe.test(text)) continue;
                // In DMs, always activate

                if (isGroup) {
                    console.log(`📩 [GROUP] @${BOT_NAME} mention detected`);
                } else {
                    console.log(`📩 [DM] Message received`);
                }

                // ─── BUILD QUERY & CONTEXT ───
                let query = text
                    .replace(/@\S+/g, '')
                    .replace(new RegExp(`\\b${BOT_NAME}\\b`, 'gi'), '')
                    .trim();

                let agentContext = null;

                // Extract quoted/tagged message context (works for both ephemeral and regular messages)
                const contextInfo = msg.message?.extendedTextMessage?.contextInfo
                    || msg.message?.ephemeralMessage?.message?.extendedTextMessage?.contextInfo;
                if (contextInfo?.quotedMessage) {
                    const quoted = contextInfo.quotedMessage;
                    const quotedText = quoted.conversation
                        || quoted.extendedTextMessage?.text
                        || quoted.imageMessage?.caption
                        || quoted.videoMessage?.caption
                        || quoted.ephemeralMessage?.message?.conversation
                        || quoted.ephemeralMessage?.message?.extendedTextMessage?.text
                        || '';
                    if (quotedText) {
                        agentContext = quotedText.length > 2000
                            ? quotedText.slice(0, 2000) + '...'
                            : quotedText;
                        console.log(`📎 [CONTEXT] Quoted message found (${quotedText.length} chars)`);
                    }
                }

                // Check for creator links (IG + YT + LI)
                const targetsFromText = extractCreatorTargets(text);
                const igFromText = extractInstagramUsername(text);
                const allIgUsernames = extractAllInstagramUsernames(text);
                const historyIgUsername = findUsernameFromHistory(jid);

                console.log(`🧠 [PRE-AGENT] Query: "${query}" | targets: ${targetsFromText.length} (${targetsFromText.map(t => `${t.platform}:${t.handle}`).join(', ')}) | Context: ${agentContext ? 'yes' : 'none'}`);

                // ─── AGENT: CLASSIFY INTENT ───
                await sock.sendMessage(jid, { react: { text: randomFrom(REACTIONS.thinking), key: msg.key } });
                const intent = await classifyIntent(query, agentContext);

                console.log(`🧠 [AGENT] Action: ${intent.action}`);

                // ═══════════════════════════════════════════
                // ACTION: GREETING
                // ═══════════════════════════════════════════
                if (intent.action === 'greeting') {
                    const prefix = isDM ? '' : `@${BOT_NAME} `;
                    const greetingText = 
                        `🤖 *Yo! I'm FinBot — your influencer intel assistant.*\n\n` +
                        `🔍 *Search Database:*\n` +
                        `• _${prefix}show all creators_\n` +
                        `• _${prefix}who has the most followers?_\n` +
                        `• _${prefix}beauty creators in Mumbai_\n\n` +
                        `📸 *Add Creators:*\n` +
                        `• _${prefix}add_ + IG link(s)\n` +
                        `• _${prefix}put in the db_ + IG link(s)\n\n` +
                        `✏️ *Update Data:*\n` +
                        `• IG link + _managed by Rahul_\n` +
                        `• IG link + _update details_\n\n` +
                        `📥 *Bulk Import:*\n` +
                        `• Google Sheets link + _import_\n\n` +
                        `📊 *Export:*\n` +
                        `• _${prefix}export to sheet_\n\n` +
                        `_Reply to any of my messages to follow up!_ 💬`;
                    await sock.sendMessage(jid, { text: greetingText }, { quoted: msg });
                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: EXPORT (or SEARCH + EXPORT)
                // ═══════════════════════════════════════════
                if (intent.action === 'export' || intent.action === 'search_and_export') {
                    await sock.sendMessage(jid, { text: quip('exportStart') }, { quoted: msg });

                    const searchQuery = intent.query || null;
                    const result = await exportToSheet(searchQuery);

                    if (result.success) {
                        let replyText = `${randomFrom(REACTIONS.success)} *Sheet is ready!*\n\n` +
                            `📋 *${result.count}* records exported\n` +
                            `🔗 ${result.url}\n\n` +
                            `_Anyone with the link can view & edit._ 😎`;

                        if (intent.action === 'search_and_export' && searchQuery) {
                            const searchResult = await queryTrakr(searchQuery);
                            replyText = formatReply(searchResult) + '\n\n' + replyText;
                        }

                        await sock.sendMessage(jid, { text: replyText }, { quoted: msg });
                    } else {
                        await sock.sendMessage(jid, {
                            text: `${randomFrom(REACTIONS.fail)} Export didn't make it: ${result.error}`,
                        }, { quoted: msg });
                    }

                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: UPDATE_MULTI (multiple fields in one go)
                // ═══════════════════════════════════════════
                if (intent.action === 'update_multi') {
                    // Resolve platform + handle: prefer explicit target from text, then LLM-provided
                    let platform, username;
                    if (targetsFromText.length) {
                        platform = targetsFromText[0].platform;
                        username = targetsFromText[0].handle;
                    } else {
                        platform = intent.platform || 'instagram';
                        username = intent.instagram_username || historyIgUsername;
                    }
                    if (!username && agentContext) {
                        const ctxTargets = extractCreatorTargets(agentContext);
                        if (ctxTargets.length) { platform = ctxTargets[0].platform; username = ctxTargets[0].handle; }
                    }

                    let updates = Array.isArray(intent.updates) ? intent.updates : [];
                    const parsed = parseInlineUpdates(text);
                    for (const p of parsed) {
                        if (!updates.find(u => u.field === p.field)) updates.push(p);
                    }

                    if (!username) {
                        await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} I need a creator link (Instagram / YouTube / LinkedIn) to update multiple fields.` }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }
                    if (!updates.length) {
                        await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} I couldn't find any fields to update. Try: "managed by X, email Y, number Z".` }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    await sock.sendMessage(jid, { react: { text: '✍️', key: msg.key } });
                    const result = await applyBulkUpdates(username, updates, platform);
                    await sock.sendMessage(jid, { text: result.ok ? result.appliedLabel : result.errorText }, { quoted: msg });
                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: UPDATE_FIELD (modify DB column)
                // ═══════════════════════════════════════════
                if (intent.action === 'update_field') {
                    // Resolve platform + handle
                    let platform, username;
                    if (targetsFromText.length) {
                        platform = targetsFromText[0].platform;
                        username = targetsFromText[0].handle;
                    } else {
                        platform = intent.platform || 'instagram';
                        username = intent.instagram_username || historyIgUsername;
                    }
                    if (!username && agentContext) {
                        const ctxTargets = extractCreatorTargets(agentContext);
                        if (ctxTargets.length) { platform = ctxTargets[0].platform; username = ctxTargets[0].handle; }
                    }

                    const field = intent.update_field_name;
                    const value = intent.update_field_value;

                    // Inline-parser safety net: 2+ fields in one message → bulk apply
                    const parsedExtras = parseInlineUpdates(text);
                    if (username && parsedExtras.length >= 2) {
                        if (field && value && !parsedExtras.find(p => p.field === field)) {
                            parsedExtras.push({ field, value });
                        }
                        await sock.sendMessage(jid, { react: { text: '✍️', key: msg.key } });
                        const result = await applyBulkUpdates(username, parsedExtras, platform);
                        await sock.sendMessage(jid, { text: result.ok ? result.appliedLabel : result.errorText }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    // Link + "update this person details" with no explicit field → scrape first
                    if (username && (!field || value === undefined || value === null)) {
                        const pending = { queue: [], current: null };
                        pendingScrapes.set(jid, pending);
                        await processScrapeForUser(sock, jid, msg, { platform, handle: username }, pending);
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    if (!username || !field || value === undefined || value === null) {
                        await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} I need a creator link (IG/YT/LI), the field, and the new value to update correctly.` }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    await sock.sendMessage(jid, { react: { text: '✍️', key: msg.key } });

                    if (field === 'niche' || field === 'language' || field === 'gender') {
                        const canon = await canonicalValue(field, value);
                        if (!canon) {
                            const allowed = await getAllowed(field);
                            await sock.sendMessage(jid, {
                                text: `❌ "${value}" is not a valid ${field}.\n_Allowed:_ ${formatAllowedList(allowed)}\n\n🔁 Please send again with an allowed value.`
                            }, { quoted: msg });
                            await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                            continue;
                        }
                    }

                    try {
                        const res = await fetch(`${TRAKR_API_URL}/api/update-field`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ username, field, value, platform })
                        });
                        const data = await res.json();

                        if (res.ok && data.success) {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.success)} ${data.message}` }, { quoted: msg });
                        } else if (data.allowed) {
                            await sock.sendMessage(jid, {
                                text: `❌ "${data.invalid_value}" is not a valid ${data.field}.\n_Allowed:_ ${formatAllowedList(data.allowed)}`
                            }, { quoted: msg });
                        } else {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} Field update failed: ${data.error}` }, { quoted: msg });
                        }
                    } catch (e) {
                         await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} Error saving to database: ${e.message}` }, { quoted: msg });
                    }

                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: SCRAPE (fetch from Instagram / YouTube / LinkedIn)
                // ═══════════════════════════════════════════
                if (intent.action === 'scrape') {
                    // Collect all targets from text first (supports multi-platform batches)
                    let targets = targetsFromText.slice();
                    if (!targets.length && intent.instagram_username) {
                        targets = [{ platform: intent.platform || 'instagram', handle: intent.instagram_username }];
                    }

                    if (!targets.length) {
                        await sock.sendMessage(jid, { text: quip('noLink') }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    if (targets.length > 1) {
                        const list = targets.map((t, i) => `${i + 1}. ${prettyTarget(t.platform, t.handle)} _(${PLATFORM_LABELS[t.platform]})_`).join('\n');
                        await sock.sendMessage(jid, {
                            text: `📋 *Found ${targets.length} creator link${targets.length === 1 ? '' : 's'}!* I'll process them one by one.\n\n${list}`
                        }, { quoted: msg });
                    }

                    let extraUpdates = Array.isArray(intent.extra_updates) ? intent.extra_updates.slice() : [];
                    const parsedFromText = parseInlineUpdates(text);
                    for (const p of parsedFromText) {
                        if (!extraUpdates.find(u => u.field === p.field)) extraUpdates.push(p);
                    }

                    const pending = {
                        queue: targets.slice(1),
                        current: null,
                        extraUpdates,
                    };
                    pendingScrapes.set(jid, pending);

                    await processScrapeForUser(sock, jid, msg, targets[0], pending);

                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: BULK_IMPORT (async background processing)
                // ═══════════════════════════════════════════
                if (intent.action === 'bulk_import') {
                    let sheetUrl = intent.sheet_url;
                    if (!sheetUrl) {
                        const sheetMatch = text.match(/https?:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9_-]+[^\s]*/i);
                        if (sheetMatch) sheetUrl = sheetMatch[0];
                    }

                    if (!sheetUrl) {
                        await sock.sendMessage(jid, {
                            text: '\u274c I could not find a valid Google Sheet link. Please send a link like:\nhttps://docs.google.com/spreadsheets/d/...'
                        }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    // ── Parse any inline field updates from the same message ──
                    // These will be applied as DEFAULTS to every imported row.
                    // e.g. "<sheet url> managed by Finnet Media"
                    let applyToAll = Array.isArray(intent.extra_updates) ? intent.extra_updates.slice() : [];
                    const parsedFromSheetMsg = parseInlineUpdates(text);
                    for (const p of parsedFromSheetMsg) {
                        if (!applyToAll.find(u => u.field === p.field)) applyToAll.push(p);
                    }

                    // Validate controlled-vocab values client-side for clean errors
                    const { valid: validDefaults, invalid: invalidDefaults } = await prevalidateUpdates(applyToAll);
                    if (invalidDefaults.length) {
                        await sock.sendMessage(jid, {
                            text: `❌ Can't start import — these values aren't allowed:${formatInvalidList(invalidDefaults)}\n\n🔁 Please send again with valid values.`
                        }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    if (validDefaults.length) {
                        const summary = validDefaults.map(d => `${d.field} → ${d.value}`).join(', ');
                        await sock.sendMessage(jid, {
                            text: `🧩 *Applying to every row:* ${summary}\n_(sheet values will win where both are present)_`
                        }, { quoted: msg });
                    }

                    try {
                        const importRes = await fetch(`${TRAKR_API_URL}/api/bulk-import`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                sheet_url: sheetUrl,
                                callback_url: 'http://127.0.0.1:3002/bulk-callback',
                                apply_to_all: validDefaults,
                            }),
                        });
                        const startResult = await importRes.json();

                        if (startResult.error) {
                            await sock.sendMessage(jid, {
                                text: `\u274c *Import Failed:* ${startResult.error}`
                            }, { quoted: msg });
                        } else {
                            bulkImportJobs.set(startResult.job_id, { jid, msgKey: msg.key });
                            await sock.sendMessage(jid, {
                                text: `\ud83d\udccb *Bulk Import Started!*\n\n\ud83d\udd17 Processing sheet in background...\n\u23f3 I will send progress updates. Other queries keep working!\n\n_Job: ${startResult.job_id}_`
                            }, { quoted: msg });
                        }
                    } catch (importErr) {
                        await sock.sendMessage(jid, {
                            text: `\u274c Import failed: ${importErr.message}`
                        }, { quoted: msg });
                    }

                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ─── SAFETY NET: Override search to scrape if creator links + action words present ───
                if (intent.action === 'search' && targetsFromText.length > 0) {
                    const scrapeKeywords = /\b(add|put|store|save|scrape|fetch|database|db)\b/i;
                    if (scrapeKeywords.test(text)) {
                        console.log('[OVERRIDE] LLM said search but text has creator links + action words -> scrape');
                        intent.action = 'scrape';
                        const targets = targetsFromText.slice();
                        if (targets.length > 1) {
                            const list = targets.map((t, i) => `${i + 1}. ${prettyTarget(t.platform, t.handle)} _(${PLATFORM_LABELS[t.platform]})_`).join('\n');
                            await sock.sendMessage(jid, {
                                text: `📋 *Found ${targets.length} creator links!* Processing one by one.\n\n${list}`
                            }, { quoted: msg });
                        }
                        const extraUpdates = parseInlineUpdates(text);
                        const pending = { queue: targets.slice(1), current: null, extraUpdates };
                        pendingScrapes.set(jid, pending);
                        await processScrapeForUser(sock, jid, msg, targets[0], pending);
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }
                }

                // ═══════════════════════════════════════════
                // ACTION: SEARCH (default)
                // ═══════════════════════════════════════════
                const searchQuery = intent.query || query;
                const result = await queryTrakr(searchQuery);

                // If the backend needs platform disambiguation, store state + ask
                if (result && result.type === 'clarify') {
                    pendingClarifications.set(jid, {
                        originalQuery: searchQuery,
                        expiresAt: Date.now() + CLARIFY_TTL_MS,
                    });
                    await sock.sendMessage(jid, { text: formatReply(result) }, { quoted: msg });
                } else {
                    await sock.sendMessage(jid, { text: formatReply(result) }, { quoted: msg });
                }
                console.log(`✅ Search: "${searchQuery}"${result?.type === 'clarify' ? ' (awaiting platform)' : ''}`);

                await sock.sendMessage(jid, { react: { text: '', key: msg.key } });

            } catch (err) {
                console.error('Bot error:', err);
                try {
                    await sock.sendMessage(msg.key.remoteJid, {
                        text: `${randomFrom(REACTIONS.fail)} Something broke: ${err.message}`,
                    }, { quoted: msg });
                } catch (e) { /* ignore send error */ }
            }
        }
    }
}

/**
 * Pre-validate a list of updates against the allowed vocabulary.
 * Returns { valid: [...], invalid: [...] }
 */
async function prevalidateUpdates(updates) {
    const valid = [], invalid = [];
    for (const u of (updates || [])) {
        const field = (u.field || '').trim();
        const value = u.value;
        if (!field || value === undefined || value === null || String(value).trim() === '') continue;
        if (field === 'niche' || field === 'language' || field === 'gender') {
            const canon = await canonicalValue(field, value);
            if (!canon) {
                invalid.push({ field, value, allowed: await getAllowed(field) });
                continue;
            }
            valid.push({ field, value: canon });
        } else {
            valid.push({ field, value: String(value).trim() });
        }
    }
    return { valid, invalid };
}

function formatInvalidList(invalid) {
    let txt = '';
    for (const inv of invalid) {
        txt += `\n• *${inv.field}:* "${inv.value}"\n   _Allowed:_ ${formatAllowedList(inv.allowed)}`;
    }
    return txt;
}

/**
 * Apply a set of field updates for a creator via /api/update-fields.
 * Handles validation + per-field rejection reporting.
 * Returns { ok, appliedLabel, errorText }.
 */
async function applyBulkUpdates(username, updates, platform = 'instagram') {
    const { valid, invalid } = await prevalidateUpdates(updates);
    if (!valid.length) {
        return {
            ok: false,
            errorText: invalid.length
                ? `❌ No valid updates. Invalid values:${formatInvalidList(invalid)}\n\n🔁 Please send again with allowed values.`
                : `❌ Nothing to update.`,
        };
    }
    try {
        const res = await fetch(`${TRAKR_API_URL}/api/update-fields`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, updates: valid, platform }),
        });
        const data = await res.json();
        if (!res.ok || !data.success) {
            return { ok: false, errorText: `❌ ${data.error || data.details || `HTTP ${res.status}`}` };
        }
        const applied = (data.applied || []).map(a => `${a.field} → ${a.value}`);
        const rejected = (data.rejected || []);
        const label = prettyTarget(platform, username);
        let txt = `✅ *${label}* updated (${applied.length} field${applied.length === 1 ? '' : 's'}):\n` +
            applied.map(a => `   • ${a}`).join('\n');
        if (rejected.length) {
            txt += `\n\n⚠️ Rejected:`;
            for (const r of rejected) {
                const allowed = r.allowed ? `\n     _Allowed:_ ${formatAllowedList(r.allowed)}` : '';
                txt += `\n   • ${r.field}="${r.value}" — ${r.reason}${allowed}`;
            }
        }
        // Also surface pre-validated invalids (if any)
        if (invalid.length) {
            txt += `\n\n⚠️ Not sent (invalid values):${formatInvalidList(invalid)}`;
        }
        return { ok: true, appliedLabel: txt };
    } catch (e) {
        return { ok: false, errorText: `❌ Error: ${e.message}` };
    }
}

/**
 * Process a single scrape: scrape profile → OCR screenshots (IG only) → ask mandatory fields.
 * target = { platform, handle } OR legacy string (treated as instagram).
 */
async function processScrapeForUser(sock, jid, msg, target, pending) {
    // Normalize target
    const tgt = typeof target === 'string' ? { platform: 'instagram', handle: target } : target;
    const platform = tgt.platform || 'instagram';
    const username = tgt.handle;
    const label = prettyTarget(platform, username);

    // OCR is IG-only (analytics screenshots format is IG-specific)
    const screenshots = platform === 'instagram' ? findRecentScreenshots(jid) : [];
    const hasScreenshots = screenshots.length > 0;

    await sock.sendMessage(jid, {
        text: `${quip('scrapeStart')}\n\n👤 Target: *${label}* _(${PLATFORM_LABELS[platform]})_${hasScreenshots ? `\n📸 Screenshots found: *${screenshots.length}*` : ''}`,
    }, { quoted: msg });

    const scrapeResult = await callScraper(tgt);
    let reply = '';

    if (scrapeResult.success) {
        const d = scrapeResult.data;
        reply += `${randomFrom(QUIPS.scrapeSuccess)}\n`;
        if (platform === 'youtube') {
            reply += `   📺 *${d.channelName || username}*\n`;
            reply += `   👥 Subscribers: *${formatNumber(d.subscribers)}*\n\n`;
        } else if (platform === 'linkedin') {
            reply += `   💼 *${d.fullName || username}*\n`;
            if (d.headline) reply += `   _${d.headline}_\n`;
            reply += `\n`;
        } else {
            reply += `   👤 *${d.creatorName || username}*\n`;
            reply += `   👥 Followers: *${formatNumber(d.followers)}*\n\n`;
        }
    } else if (scrapeResult.error && scrapeResult.error.includes('Insufficient Data')) {
        reply += `⚠️ *${label}* doesn't have enough public data to calculate metrics.\n`;
        reply += `_${scrapeResult.details || scrapeResult.error}_\n`;
        reply += `❌ *Creator was NOT added to the database.*\n\n`;
        await sock.sendMessage(jid, { text: reply.trim() }, { quoted: msg });
        const next = pending.queue.shift();
        if (next) {
            await processScrapeForUser(sock, jid, msg, next, pending);
        } else {
            pendingScrapes.delete(jid);
        }
        return;
    } else {
        reply += `${randomFrom(REACTIONS.fail)} Scrape hiccup: ${scrapeResult.error}\n\n`;
    }

    // OCR if screenshots are present (Instagram only)
    if (hasScreenshots) {
        let ocrSuccess = 0;
        let extractedFields = [];

        for (let i = 0; i < screenshots.length; i++) {
            const result = await processImage(screenshots[i], username);
            if (result.success) {
                ocrSuccess++;
                if (result.result) {
                    Object.entries(result.result).forEach(([k, v]) => {
                        if (v && v !== '' && v !== '-' && v !== 'N/A' && !extractedFields.includes(k)) {
                            extractedFields.push(k);
                        }
                    });
                }
            } else {
                console.error(`  Screenshot ${i + 1} failed: ${result.error}`);
            }
        }

        if (ocrSuccess > 0) {
            reply += `📸 *OCR processed!* ${ocrSuccess}/${screenshots.length} screenshot(s)\n`;
            if (extractedFields.length > 0) {
                const labels = extractedFields.map(f => f.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                reply += `📋 *Data pulled:* ${labels.join(', ')}\n`;
            }
        }
    }

    await sock.sendMessage(jid, { text: reply.trim() });

    // ── Apply any extra_updates the user gave alongside the scrape link ──
    const extras = Array.isArray(pending.extraUpdates) ? pending.extraUpdates : [];
    const prefilled = {};

    if (scrapeResult.success && extras.length) {
        const bulk = await applyBulkUpdates(username, extras, platform);
        if (bulk.ok) {
            await sock.sendMessage(jid, { text: bulk.appliedLabel });
            for (const e of extras) {
                if (e.field === 'language' || e.field === 'niche' || e.field === 'gender') {
                    const canon = await canonicalValue(e.field, e.value);
                    if (canon) prefilled[e.field] = canon;
                }
            }
        } else {
            await sock.sendMessage(jid, { text: `⚠️ Couldn't apply inline updates.\n${bulk.errorText}` });
        }
    }

    // Check existing row for mandatory fields (IG only for now — YT/LI lookup endpoints differ)
    let needMandatory = true;
    let existingRow = null;
    if (platform === 'instagram') {
        try {
            const existingRes = await fetch(`${TRAKR_API_URL}/api/roster/${username}`);
            if (existingRes.ok) {
                existingRow = await existingRes.json();
                if (existingRow && existingRow.language && existingRow.niche && existingRow.gender) {
                    needMandatory = false;
                    console.log(`[SCRAPE] ${label} already has mandatory fields, skipping prompt`);
                    await sock.sendMessage(jid, {
                        text: `${label} already has all details in DB! Scrape data updated.`
                    });
                }
            }
        } catch (e) {
            console.error('[SCRAPE] Error checking existing creator:', e.message);
        }
    }

    if (existingRow) {
        if (!prefilled.language && existingRow.language) prefilled.language = existingRow.language;
        if (!prefilled.niche && existingRow.niche) prefilled.niche = existingRow.niche;
        if (!prefilled.gender && existingRow.gender) prefilled.gender = existingRow.gender;
    }

    if (needMandatory && prefilled.language && prefilled.niche && prefilled.gender) {
        needMandatory = false;
        await sock.sendMessage(jid, {
            text: `✅ ${label} finalized with given details (Language: ${prefilled.language}, Niche: ${prefilled.niche}, Gender: ${prefilled.gender}).`
        });
    }

    if (needMandatory) {
        pending.current = { username, platform, step: 'awaiting_mandatory', data: {}, prefilled };
        const missing = [];
        if (!prefilled.language) missing.push('Language: (e.g. Hindi)');
        if (!prefilled.niche) missing.push('Niche: (e.g. Finance)');
        if (!prefilled.gender) missing.push('Gender: (Male / Female / Other)');
        const hint = missing.length === 3
            ? `Or in one line: Hindi, Finance, Male`
            : `_Only the missing one(s) above — the rest are already set._`;
        await sock.sendMessage(jid, {
            text: `Before I finalize ${label}, I need ${missing.length === 1 ? 'this detail' : 'these details'}:\n\n${missing.join('\n')}\n\n${hint}\n\n_Type "quit" to cancel._`
        });
    } else {
        const next = pending.queue.shift();
        if (next) {
            await processScrapeForUser(sock, jid, msg, next, pending);
        } else {
            pendingScrapes.delete(jid);
        }
    }
}

console.log(`
╔══════════════════════════════════════════╗
║  🧠 FinBot — TRAKR Agentic Bot          ║
║  "I think, therefore I bot."            ║
╚══════════════════════════════════════════╝
Bot trigger:  @${BOT_NAME} (groups) / direct (DMs)
API server:   ${TRAKR_API_URL}
Routing:      LLM-powered (agent.js)
Activation:   @mention (groups) + all DMs
`);


// ─── Bulk Import Webhook Server (port 3002) ───────────────────
// Receives progress updates and final reports from Flask background threads
let _botSocket = null;

function startWebhookServer() {
    const server = http.createServer(async (req, res) => {
        if (req.method === 'POST' && req.url === '/bulk-callback') {
            let body = '';
            req.on('data', chunk => body += chunk);
            req.on('end', async () => {
                try {
                    const data = JSON.parse(body);
                    const job = bulkImportJobs.get(data.job_id);

                    if (!job || !_botSocket) {
                        res.writeHead(404);
                        res.end('Job not found');
                        return;
                    }

                    if (data.type === 'progress') {
                        await _botSocket.sendMessage(job.jid, { text: data.message });
                    } else if (data.type === 'complete') {
                        const report = data.report;
                        let reportMsg = 'Bulk Import Report\n\n';
                        reportMsg += 'Total rows: ' + (report.total_rows || 0) + '\n';
                        reportMsg += 'Imported: ' + (report.imported || 0) + '\n';
                        reportMsg += 'Skipped: ' + (report.skipped ? report.skipped.length : 0) + '\n';
                        reportMsg += 'Errors: ' + (report.errors ? report.errors.length : 0) + '\n';

                        if (report.skipped && report.skipped.length > 0) {
                            reportMsg += '\nSkipped Rows:\n';
                            for (const s of report.skipped.slice(0, 15)) {
                                reportMsg += '  Row ' + s.row + ': ' + s.name + ' - ' + s.reason + '\n';
                            }
                        }

                        if (report.errors && report.errors.length > 0) {
                            reportMsg += '\nErrors:\n';
                            for (const e of report.errors.slice(0, 10)) {
                                reportMsg += '  @' + e.username + ': ' + e.reason + '\n';
                            }
                        }

                        if (report.error) {
                            reportMsg = 'Import Failed: ' + report.error;
                        }

                        await _botSocket.sendMessage(job.jid, { text: reportMsg.trim() });
                        bulkImportJobs.delete(data.job_id);
                    }

                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ ok: true }));
                } catch (err) {
                    console.error('[WEBHOOK] Error:', err);
                    res.writeHead(500);
                    res.end('Error');
                }
            });
        } else {
            res.writeHead(404);
            res.end('Not found');
        }
    });

    server.listen(3002, '127.0.0.1', () => {
        console.log('Bulk import webhook server listening on port 3002');
    });
}

startWebhookServer();

startBot().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});
