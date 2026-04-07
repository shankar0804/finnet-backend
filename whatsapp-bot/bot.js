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
 * Only looks back 3 messages from latest to avoid picking up old screenshots.
 */
function findRecentScreenshots(jid) {
    const history = chatHistory.get(jid) || [];
    const images = [];
    // Look at last 3 messages only (not the current message)
    const start = Math.max(0, history.length - 4);
    for (let i = history.length - 2; i >= start; i--) {
        if (history[i]?.message?.imageMessage) {
            images.unshift(history[i]);
        }
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
 * Call the Trakr scraper API (Apify).
 */
async function callScraper(username) {
    try {
        const res = await fetch(`${TRAKR_API_URL}/api/scrape-instagram`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username }),
        });
        const data = await res.json();
        if (!res.ok) return { success: false, error: data.error || data.details || 'Scrape failed' };
        return { success: true, data };
    } catch (err) {
        return { success: false, error: err.message };
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
    let msg = '';

    // Insight display removed - show data directly
        if (data.data && data.data.length > 0) {
        const allRows = data.data;

        msg += `📊 *Results* (${allRows.length} found):\n\n`;
        allRows.forEach((row, i) => {
            const name = row.creator_name || row.username || 'Unknown';
            const username = row.username ? `@${row.username}` : '';
            const niche = row.niche || '-';
            const followers = formatNumber(row.followers);
            const avgViews = formatNumber(row.avg_views);

            msg += `*${i + 1}. ${name}* ${username}\n`;
            msg += `   Niche: ${niche} | Followers: ${followers} | Avg Views: ${avgViews}\n\n`;

            // Safety: if message is getting too long, stop adding rows
            if (msg.length > MAX_MESSAGE_LENGTH - 200) {
                msg += `\n... _(${allRows.length - i - 1} more — use dashboard for full list)_\n`;
                return;
            }
        });
    } else {
        msg += '📭 No results found.';
    }

    if (msg.length > MAX_MESSAGE_LENGTH) {
        msg = msg.slice(0, MAX_MESSAGE_LENGTH - 50) + '\n\n... _(use dashboard for full results)_';
    }
    return msg.trim();
}

async function queryTrakr(query) {
    try {
        const res = await fetch(`${TRAKR_API_URL}/api/custom-search`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query }),
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

        for (const msg of messages) {
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

                        if (!language || !niche || !gender) {
                            await sock.sendMessage(jid, {
                                text: `⚠️ I need all 3 fields. Please reply like this:\n\nLanguage: Hindi\nNiche: Finance\nGender: Male\n\n_Or in one line:_ Hindi, Finance, Male`
                            }, { quoted: msg });
                            continue;
                        }

                        // Update the DB with mandatory fields
                        const username = pending.current.username;
                        try {
                            for (const [field, value] of [['language', language], ['niche', niche], ['gender', gender]]) {
                                await fetch(`${TRAKR_API_URL}/api/update-field`, {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ username, field, value })
                                });
                            }
                            await sock.sendMessage(jid, {
                                text: `${randomFrom(REACTIONS.success)} *@${username}* updated!\n   Language: ${language}\n   Niche: ${niche}\n   Gender: ${gender}\n\n📸 _Want to share analytics screenshots for more data? Send them now, or type "skip" to move on._`
                            }, { quoted: msg });
                            pending.current.step = 'awaiting_screenshots';
                        } catch (e) {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} Failed to update fields: ${e.message}` }, { quoted: msg });
                            pendingScrapes.delete(jid);
                        }
                        continue;
                    }

                    if (pending.current && pending.current.step === 'awaiting_screenshots') {
                        const lower = text.toLowerCase().trim();
                        if (lower === 'skip' || lower === 'no' || lower === 'next' || lower === 'done') {
                            // Move to next in queue
                            const nextUsername = pending.queue.shift();
                            if (nextUsername) {
                                await processScrapeForUser(sock, jid, msg, nextUsername, pending);
                            } else {
                                await sock.sendMessage(jid, { text: `✅ *All done!* All creators have been processed. 🎉` }, { quoted: msg });
                                pendingScrapes.delete(jid);
                            }
                            continue;
                        }

                        // Any other text while awaiting screenshots — remind them
                        await sock.sendMessage(jid, {
                            text: `📸 I'm waiting for analytics screenshots for *@${pending.current.username}*. Send screenshot images, or type "done" to move on.`
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

                // Check for IG links
                const igFromText = extractInstagramUsername(text);
                const allIgUsernames = extractAllInstagramUsernames(text);
                const historyIgUsername = findUsernameFromHistory(jid);

                console.log(`🧠 [PRE-AGENT] Query: "${query}" | IG links: ${allIgUsernames.length} | Context: ${agentContext ? 'yes' : 'none'}`);

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
                // ACTION: UPDATE_FIELD (modify DB column)
                // ═══════════════════════════════════════════
                if (intent.action === 'update_field') {
                    // Extract username: try agent > IG link in text > IG link from quoted context > history
                    let username = intent.instagram_username || igFromText || historyIgUsername;

                    // Also try to extract from quoted message context if available
                    if (!username && agentContext) {
                        username = extractInstagramUsername(agentContext);
                    }

                    const field = intent.update_field_name;
                    const value = intent.update_field_value;

                    // If user sent a link + "update this person details" but no specific field/value,
                    // SCRAPE the profile first (to get name/followers/etc), then ask for mandatory fields
                    if (username && (!field || value === undefined || value === null)) {
                        const pending = { queue: [], current: null };
                        pendingScrapes.set(jid, pending);
                        
                        // Scrape the profile first to get basic data
                        await processScrapeForUser(sock, jid, msg, username, pending);
                        
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    if (!username || !field || value === undefined || value === null) {
                        await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} I need the creator's Instagram link or username, the field, and the new value to update correctly.` }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    await sock.sendMessage(jid, { react: { text: '✍️', key: msg.key } });

                    try {
                        const res = await fetch(`${TRAKR_API_URL}/api/update-field`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ username, field, value })
                        });
                        const data = await res.json();
                        
                        if (res.ok && data.success) {
                            await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.success)} ${data.message}` }, { quoted: msg });
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
                // ACTION: SCRAPE (fetch from Instagram)
                // ═══════════════════════════════════════════
                if (intent.action === 'scrape') {
                    // Collect all IG usernames: from text > agent extraction > history
                    let usernames = allIgUsernames.length > 0
                        ? allIgUsernames
                        : (intent.instagram_username ? [intent.instagram_username] : []);

                    if (usernames.length === 0 && igFromText) usernames = [igFromText];
                    // Don't fall back to history — user must provide links explicitly for scraping

                    if (usernames.length === 0) {
                        await sock.sendMessage(jid, { text: quip('noLink') }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    if (usernames.length > 1) {
                        await sock.sendMessage(jid, {
                            text: `📋 *Found ${usernames.length} Instagram profiles!* I'll process them one by one.\n\n${usernames.map((u, i) => `${i + 1}. @${u}`).join('\n')}`
                        }, { quoted: msg });
                    }

                    // Set up the queue: first goes to processing, rest wait
                    const pending = {
                        queue: usernames.slice(1),
                        current: null,
                    };
                    pendingScrapes.set(jid, pending);

                    await processScrapeForUser(sock, jid, msg, usernames[0], pending);

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

                    try {
                        const importRes = await fetch(`${TRAKR_API_URL}/api/bulk-import`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                sheet_url: sheetUrl,
                                callback_url: 'http://127.0.0.1:3002/bulk-callback',
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

                // ─── SAFETY NET: Override search to scrape if IG links + action words present ───
                if (intent.action === 'search' && allIgUsernames.length > 0) {
                    const scrapeKeywords = /\b(add|put|store|save|scrape|fetch|database|db)\b/i;
                    if (scrapeKeywords.test(text)) {
                        console.log('[OVERRIDE] LLM said search but text has IG links + action words -> scrape');
                        intent.action = 'scrape';
                        intent.instagram_username = allIgUsernames[0];
                        let usernames = allIgUsernames;
                        if (usernames.length > 1) {
                            await sock.sendMessage(jid, {
                                text: `Found ${usernames.length} Instagram profiles! Processing one by one.\n\n${usernames.map((u, i) => `${i + 1}. @${u}`).join('\n')}`
                            }, { quoted: msg });
                        }
                        const pending = { queue: usernames.slice(1), current: null };
                        pendingScrapes.set(jid, pending);
                        await processScrapeForUser(sock, jid, msg, usernames[0], pending);
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }
                }

                // ═══════════════════════════════════════════
                // ACTION: SEARCH (default)
                // ═══════════════════════════════════════════
                const searchQuery = intent.query || query;
                const result = await queryTrakr(searchQuery);
                await sock.sendMessage(jid, { text: formatReply(result) }, { quoted: msg });
                console.log(`✅ Search: "${searchQuery}"`);

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
    });
}

/**
 * Process a single scrape: scrape profile → OCR screenshots → ask mandatory fields.
 */
async function processScrapeForUser(sock, jid, msg, username, pending) {
    const screenshots = findRecentScreenshots(jid);
    const hasScreenshots = screenshots.length > 0;

    await sock.sendMessage(jid, {
        text: `${quip('scrapeStart')}\n\n👤 Target: *@${username}*${hasScreenshots ? `\n📸 Screenshots found: *${screenshots.length}*` : ''}`,
    }, { quoted: msg });

    const scrapeResult = await callScraper(username);
    let reply = '';

    if (scrapeResult.success) {
        const d = scrapeResult.data;
        reply += `${randomFrom(QUIPS.scrapeSuccess)}\n`;
        reply += `   👤 *${d.creatorName || username}*\n`;
        reply += `   👥 Followers: *${formatNumber(d.followers)}*\n\n`;
    } else {
        reply += `${randomFrom(REACTIONS.fail)} Scrape hiccup: ${scrapeResult.error}\n\n`;
    }

    // OCR if screenshots are present (only from last 3 messages)
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

    // Now ask for mandatory fields
    pending.current = { username, step: 'awaiting_mandatory', data: {} };
    await sock.sendMessage(jid, {
        text: `📝 *Before I finalize @${username}, I need 3 details:*\n\nPlease reply with:\nLanguage: (e.g. Hindi)\nNiche: (e.g. Finance)\nGender: (e.g. Male)\n\n_Or in one line:_ Hindi, Finance, Male`
    });
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
