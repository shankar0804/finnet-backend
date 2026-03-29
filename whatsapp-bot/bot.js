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
const QRCode = require('qrcode');
const FormData = require('form-data');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { BOT_NAME, TRAKR_API_URL, MAX_ROWS_IN_REPLY, MAX_MESSAGE_LENGTH, AUTH_DIR } = require('./config');

const logger = pino({ level: 'warn' });

// ─── Supabase Session Persistence ───
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
let supabase = null;

if (SUPABASE_URL && SUPABASE_KEY) {
    try {
        const { createClient } = require('@supabase/supabase-js');
        supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
        console.log('📦 Supabase session persistence enabled');
    } catch (e) {
        console.log('⚠️ Supabase client not available, using local auth only');
    }
}

async function loadSessionFromSupabase() {
    if (!supabase) return false;
    try {
        const { data, error } = await supabase.from('whatsapp_auth').select('*');
        if (error || !data || data.length === 0) return false;
        if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });
        for (const row of data) {
            fs.writeFileSync(path.join(AUTH_DIR, row.file_name), row.file_data);
        }
        console.log(`📦 Loaded ${data.length} session files from Supabase`);
        return true;
    } catch (e) {
        console.log('⚠️ Failed to load session from Supabase:', e.message);
        return false;
    }
}

async function saveSessionToSupabase() {
    if (!supabase) return;
    try {
        if (!fs.existsSync(AUTH_DIR)) return;
        const files = fs.readdirSync(AUTH_DIR);
        for (const fileName of files) {
            const filePath = path.join(AUTH_DIR, fileName);
            const fileData = fs.readFileSync(filePath, 'utf8');
            await supabase.from('whatsapp_auth').upsert(
                { file_name: fileName, file_data: fileData },
                { onConflict: 'file_name' }
            );
        }
        console.log(`📦 Saved ${files.length} session files to Supabase`);
    } catch (e) {
        console.log('⚠️ Failed to save session to Supabase:', e.message);
    }
}

// ─── Bot Status & QR for Web Dashboard ───
let botStatus = { state: 'disconnected', qr: null, qrBase64: null, phone: null };

function startStatusServer() {
    const server = http.createServer((req, res) => {
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Access-Control-Allow-Origin', '*');
        if (req.url === '/status') {
            res.end(JSON.stringify(botStatus));
        } else if (req.url === '/qr') {
            res.end(JSON.stringify({ qr: botStatus.qrBase64, state: botStatus.state }));
        } else {
            res.statusCode = 404;
            res.end('{}');
        }
    });
    server.listen(3001, '127.0.0.1', () => console.log('📡 Bot status API running on 127.0.0.1:3001'));
}
// Delay status server so Render binds Gunicorn's port first
setTimeout(startStatusServer, 8000);

// ─── Debounced Supabase save (prevent save spam) ───
let saveTimer = null;
function debouncedSaveSession() {
    if (saveTimer) clearTimeout(saveTimer);
    saveTimer = setTimeout(() => saveSessionToSupabase(), 5000);
}

// ─── Reconnect protection ───
let isConnecting = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_DELAY = 30000;

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

// Store recent messages per group (last 50)
const groupHistory = new Map();
const MAX_HISTORY = 50;

function addToHistory(groupJid, msg) {
    if (!groupHistory.has(groupJid)) groupHistory.set(groupJid, []);
    const history = groupHistory.get(groupJid);
    history.push(msg);
    if (history.length > MAX_HISTORY) history.shift();
}

/**
 * Extract Instagram username from text (URL or plain username).
 */
function extractInstagramUsername(text) {
    if (!text) return null;
    // Match instagram.com/username (handles /reel/, /p/, etc. paths too)
    const urlMatch = text.match(/instagram\.com\/(?:reel\/|p\/)?([A-Za-z0-9_.]+)/i);
    if (urlMatch) return urlMatch[1].split('?')[0].split('/')[0];
    return null;
}

/**
 * Scan recent group messages for Instagram links and images.
 */
function findContextFromHistory(groupJid) {
    const history = groupHistory.get(groupJid) || [];
    const images = [];
    let instagramUsername = null;

    // Scan recent messages (newest first)
    for (let i = history.length - 1; i >= 0; i--) {
        const msg = history[i];
        const text = msg.message?.conversation
            || msg.message?.extendedTextMessage?.text
            || msg.message?.imageMessage?.caption
            || msg.message?.videoMessage?.caption
            || '';

        // Look for Instagram link
        if (!instagramUsername && text) {
            const username = extractInstagramUsername(text);
            if (username) instagramUsername = username;
        }

        // Look for images
        if (msg.message?.imageMessage) {
            images.unshift(msg);
        }

        // Don't look back more than 25 messages
        if ((history.length - 1 - i) > 25) break;
        // Cap at 10 images
        if (images.length >= 10) break;
    }

    return { images, instagramUsername };
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
        const buffer = await downloadMediaMessage(msg, 'buffer', {});
        if (!buffer || buffer.length === 0) {
            return { success: false, error: 'Failed to download image' };
        }

        const form = new FormData();
        form.append('image', buffer, {
            filename: 'screenshot.jpg',
            contentType: msg.message.imageMessage.mimetype || 'image/jpeg',
        });
        form.append('target_username', targetUsername);

        return new Promise((resolve) => {
            form.submit(`${TRAKR_API_URL}/api/upload`, (err, res) => {
                if (err) {
                    resolve({ success: false, error: err.message });
                    return;
                }
                let body = '';
                res.on('data', (chunk) => body += chunk);
                res.on('end', () => {
                    try {
                        const data = JSON.parse(body);
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve({ success: true, result: data.result });
                        } else {
                            resolve({ success: false, error: data.error || 'OCR failed' });
                        }
                    } catch (e) {
                        resolve({ success: false, error: 'Invalid server response' });
                    }
                });
            });
        });
    } catch (err) {
        return { success: false, error: err.message };
    }
}

/**
 * Check if a query is an "update" command.
 * Strong keywords always trigger update mode.
 * Weak keywords only trigger update if an Instagram link is present.
 */
function isUpdateCommand(query, fullText) {
    const lower = query.toLowerCase();
    const hasLink = extractInstagramUsername(fullText || query) !== null;
    
    // These ALWAYS mean "scrape/update" regardless of link
    const strongKeywords = ['update', 'scrape', 'add', 'save', 'ocr', 'scan',
        'screenshot', 'ss', 'above', 'below', 'these'];
    const hasStrong = strongKeywords.some(kw => lower.includes(kw));
    if (hasStrong) return true;
    
    // If there's an Instagram link, these also trigger update
    if (hasLink) {
        const weakKeywords = ['fetch', 'get', 'pull', 'process', 'extract', 'read',
            'details', 'profile', 'data', 'link', 'info', 'check', 'db'];
        return weakKeywords.some(kw => lower.includes(kw)) || true; // link alone = update
    }
    
    return false;
}

/**
 * Check if a query is an "export" command.
 */
function isExportCommand(query) {
    const keywords = ['export', 'excel', 'sheet', 'spreadsheet', 'google sheet', 'csv'];
    const lower = query.toLowerCase();
    return keywords.some(kw => lower.includes(kw));
}

/**
 * Fetch all roster data and export to Google Sheet.
 */
async function exportToSheet(searchQuery) {
    try {
        let rows;
        let title;

        if (searchQuery) {
            // Export specific search results
            const searchResult = await queryTrakr(searchQuery);
            if (!searchResult || searchResult.type === 'error' || !searchResult.data?.length) {
                return { success: false, error: 'No data found for that query.' };
            }
            rows = searchResult.data;
            title = `TRAKR Export: ${searchQuery}`;
        } else {
            // Export all roster data
            const res = await fetch(`${TRAKR_API_URL}/api/roster`);
            if (!res.ok) return { success: false, error: 'Failed to fetch roster data' };
            const data = await res.json();
            rows = data;
            title = `TRAKR Full Roster Export`;
        }

        if (!rows || rows.length === 0) {
            return { success: false, error: 'No data to export.' };
        }

        // Call export endpoint
        const exportRes = await fetch(`${TRAKR_API_URL}/api/export-to-sheet`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ data: rows, title }),
        });

        const exportData = await exportRes.json();
        if (!exportRes.ok) {
            return { success: false, error: exportData.error || 'Export failed' };
        }

        return { success: true, url: exportData.sheet_url, count: rows.length };
    } catch (err) {
        return { success: false, error: err.message };
    }
}

// ─── AI Search Formatting ───

function formatReply(data) {
    if (!data || data.type === 'error') {
        return `❌ *Error:* ${data?.message || 'Something went wrong.'}`;
    }
    let msg = '';
    if (data.insight) {
        const clean = data.insight.replace(/<strong>/gi, '*').replace(/<\/strong>/gi, '*').replace(/<[^>]+>/g, '');
        msg += `💡 ${clean}\n\n`;
    }
    if (data.data && data.data.length > 0) {
        const rows = data.data.slice(0, MAX_ROWS_IN_REPLY);
        const cols = Object.keys(rows[0]);
        const priority = ['username', 'creator_name', 'followers', 'avg_views', 'engagement_rate', 'niche', 'location', 'platform'];
        const display = priority.filter(c => cols.includes(c));
        const final = display.length > 0 ? display : cols.slice(0, 4);

        msg += `📊 *Results* (${data.data.length} found${data.data.length > MAX_ROWS_IN_REPLY ? `, top ${MAX_ROWS_IN_REPLY}` : ''}):\n\n`;
        rows.forEach((row, i) => {
            msg += `*${i + 1}. ${row.creator_name || row.username || 'Unknown'}*\n`;
            final.forEach(col => {
                if (col === 'creator_name') return;
                let val = row[col];
                if (val === null || val === undefined || val === '') val = '-';
                if ((col === 'followers' || col === 'avg_views') && !isNaN(parseInt(val))) {
                    const n = parseInt(val);
                    val = n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}K` : `${n}`;
                }
                msg += `   ${col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}: ${val}\n`;
            });
            msg += '\n';
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
    // Prevent concurrent reconnects
    if (isConnecting) {
        console.log('⏳ Already connecting, skipping...');
        return;
    }
    isConnecting = true;

    // Try loading session from Supabase first (for cloud deploys)
    await loadSessionFromSupabase();

    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
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

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            console.log('\n📱 Scan this QR code with WhatsApp:\n');
            qrcode.generate(qr, { small: true });
            console.log('\nWaiting for scan...\n');
            try {
                const qrBase64 = await QRCode.toDataURL(qr, { width: 300, margin: 2 });
                botStatus = { state: 'qr', qr: qr, qrBase64, phone: null };
            } catch (e) {
                botStatus = { state: 'qr', qr: qr, qrBase64: null, phone: null };
            }
        }
        if (connection === 'close') {
            isConnecting = false;
            const code = lastDisconnect?.error?.output?.statusCode;
            const retry = code !== DisconnectReason.loggedOut;
            console.log(`❌ Closed (${code}). ${retry ? 'Reconnecting...' : 'Logged out.'}`);
            botStatus = { state: retry ? 'reconnecting' : 'logged_out', qr: null, qrBase64: null, phone: null };
            if (retry) {
                // Exponential backoff: 3s, 6s, 12s... max 30s
                reconnectAttempts++;
                const delay = Math.min(3000 * Math.pow(2, reconnectAttempts - 1), MAX_RECONNECT_DELAY);
                console.log(`⏳ Waiting ${delay / 1000}s before reconnect (attempt ${reconnectAttempts})...`);
                setTimeout(() => startBot(), delay);
            }
        }
        if (connection === 'open') {
            isConnecting = false;
            reconnectAttempts = 0; // Reset on successful connection
            console.log('✅ WhatsApp connected! Bot is live.');
            console.log(`🤖 Listening for @${BOT_NAME} in groups...\n`);
            const phone = sock.user?.id?.split(':')[0] || sock.user?.id?.split('@')[0] || 'unknown';
            botStatus = { state: 'connected', qr: null, qrBase64: null, phone };
            debouncedSaveSession();
        }
    });

    sock.ev.on('creds.update', async () => {
        await saveCreds();
        debouncedSaveSession();
    });

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        for (const msg of messages) {
            try {
                const jid = msg.key.remoteJid;
                if (!jid || !jid.endsWith('@g.us')) continue;

                // Store ALL messages in history
                addToHistory(jid, msg);

                if (msg.key.fromMe) continue;

                // DEBUG: Log raw message keys to understand structure
                const msgTypes = Object.keys(msg.message || {});
                console.log(`🔍 [DEBUG] Raw msg types: ${JSON.stringify(msgTypes)}`);

                // Extract text from ALL possible WhatsApp message types
                const text = msg.message?.conversation
                    || msg.message?.extendedTextMessage?.text
                    || msg.message?.imageMessage?.caption
                    || msg.message?.videoMessage?.caption
                    || '';

                console.log(`🔍 [DEBUG] Extracted text: "${text.substring(0, 200)}"`);

                if (!text) {
                    console.log(`🔍 [DEBUG] No text found, skipping`);
                    continue;
                }

                // Check for bot mention — handle both "@finbot" and "finbot" as separate word
                // In WhatsApp groups, user may type "@hi finbot" where @ is on a different word
                const mentionRe = new RegExp(`\\b${BOT_NAME}\\b`, 'i');
                const mentionMatch = mentionRe.test(text);
                console.log(`🔍 [DEBUG] Mention regex /\\b${BOT_NAME}\\b/ match: ${mentionMatch}`);

                if (!mentionMatch) {
                    console.log(`🔍 [DEBUG] No mention found, skipping`);
                    continue;
                }

                // Strip bot name and @mentions from query text
                const query = text
                    .replace(/@\S+/g, '')           // remove @mentions
                    .replace(new RegExp(`\\b${BOT_NAME}\\b`, 'gi'), '')  // remove bare "finbot"
                    .trim();
                console.log(`🔍 [DEBUG] Query after stripping @mentions: "${query.substring(0, 200)}"`);

                const igFromQuery = extractInstagramUsername(query);
                const igFromText = extractInstagramUsername(text);
                console.log(`🔍 [DEBUG] IG from query: ${igFromQuery}, IG from text: ${igFromText}`);
                console.log(`🔍 [DEBUG] isUpdateCommand: ${isUpdateCommand(query, text)}, isExportCommand: ${isExportCommand(query)}`);

                // Greetings & help → show intro (don't send to AI search)
                const greetings = ['hi', 'hello', 'hey', 'sup', 'yo', 'help', 'menu', 'what can you do', 'hii', 'hiii', 'helo', 'hlw'];
                const isGreeting = !query || greetings.includes(query.toLowerCase());

                if (isGreeting) {
                    await sock.sendMessage(jid, {
                        text: `🤖 *Yo! I'm FinBot — your influencer intel assistant.*\n\nHere's what I can do:\n\n` +
                            `🔍 *Ask me anything:*\n` +
                            `• _@${BOT_NAME} show all creators_\n` +
                            `• _@${BOT_NAME} who has the most followers?_\n` +
                            `• _@${BOT_NAME} top 5 by engagement rate_\n\n` +
                            `📸 *Feed me data:*\n` +
                            `1. Drop an Instagram link\n` +
                            `2. (Optional) Send analytics screenshots\n` +
                            `3. _@${BOT_NAME} update db_\n\n` +
                            `📊 *Export to Google Sheet:*\n` +
                            `• _@${BOT_NAME} export to sheet_\n` +
                            `• _@${BOT_NAME} export top beauty creators_\n\n` +
                            `_I'm fast, I'm smart, and I don't take lunch breaks._ 😎`,
                    }, { quoted: msg });
                    continue;
                }

                const mode = isExportCommand(query) ? 'EXPORT' : isUpdateCommand(query, text) ? 'UPDATE' : 'SEARCH';
                console.log(`📩 [${mode}] "${query}"`);

                await sock.sendMessage(jid, { react: { text: randomFrom(REACTIONS.thinking), key: msg.key } });

                // ═══════════════════════════════════════════
                // MODE: EXPORT TO GOOGLE SHEET
                // ═══════════════════════════════════════════
                if (isExportCommand(query)) {
                    // Check if there's a specific search query after the export keyword
                    const exportKeywords = ['export', 'excel', 'sheet', 'spreadsheet', 'google sheet', 'csv'];
                    let searchQuery = query;
                    exportKeywords.forEach(kw => {
                        searchQuery = searchQuery.replace(new RegExp(kw, 'gi'), '').trim();
                    });
                    // Remove filler words — be aggressive so natural phrasing works
                    searchQuery = searchQuery.replace(/\b(to|the|a|an|in|on|of|all|data|it|put|make|create|share|send|fetch|get|grab|pull|give|me|my|our|and|or|from|into|for|this|that|please|pls|db|database|roster|influencer|influencers|creator|creators|everyone|everything|list|full|whole|entire|complete|every)\b/gi, '').trim();

                    await sock.sendMessage(jid, {
                        text: quip('exportStart'),
                    }, { quoted: msg });

                    const result = await exportToSheet(searchQuery || null);

                    if (result.success) {
                        await sock.sendMessage(jid, {
                            text: `${randomFrom(REACTIONS.success)} *Sheet is ready!*\n\n` +
                                `📋 *${result.count}* records exported\n` +
                                `🔗 ${result.url}\n\n` +
                                `_Anyone with the link can view & edit. You're welcome._ 😎`,
                        }, { quoted: msg });
                    } else {
                        await sock.sendMessage(jid, {
                            text: `${randomFrom(REACTIONS.fail)} Export didn't make it: ${result.error}`,
                        }, { quoted: msg });
                    }

                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // MODE: UPDATE DB (scrape + optional OCR)
                // ═══════════════════════════════════════════
                if (isUpdateCommand(query, text)) {
                    const context = findContextFromHistory(jid);

                    // Also check if the command text itself has a link
                    // INLINE link takes PRIORITY over history link
                    const usernameFromText = extractInstagramUsername(text);
                    if (usernameFromText) {
                        context.instagramUsername = usernameFromText;
                    }

                    // RULE: Link is ALWAYS required
                    if (!context.instagramUsername) {
                        await sock.sendMessage(jid, {
                            text: quip('noLink'),
                        }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    const username = context.instagramUsername;
                    const hasScreenshots = context.images.length > 0;

                    // Step 1: Always scrape
                    const startMsg = quip('scrapeStart');
                    await sock.sendMessage(jid, {
                        text: `${startMsg}\n\n👤 Target: *@${username}*${hasScreenshots ? `\n📸 Screenshots found: *${context.images.length}*` : ''}`,
                    }, { quoted: msg });

                    const scrapeResult = await callScraper(username);

                    let reply = '';

                    if (scrapeResult.success) {
                        const d = scrapeResult.data;
                        reply += `${randomFrom(QUIPS.scrapeSuccess)}\n`;
                        reply += `   👤 *${d.creatorName || username}*\n`;
                        reply += `   👥 Followers: *${d.followers ? (d.followers >= 1e6 ? `${(d.followers / 1e6).toFixed(1)}M` : d.followers >= 1e3 ? `${(d.followers / 1e3).toFixed(1)}K` : d.followers) : '-'}*\n\n`;
                    } else {
                        reply += `${randomFrom(REACTIONS.fail)} Scrape hiccup: ${scrapeResult.error}\n\n`;
                    }

                    // Step 2: OCR if screenshots present
                    if (hasScreenshots) {
                        let ocrSuccess = 0;
                        let ocrFail = 0;
                        let extractedFields = [];

                        for (let i = 0; i < context.images.length; i++) {
                            const result = await processImage(context.images[i], username);
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
                                ocrFail++;
                                console.error(`  Screenshot ${i + 1} failed: ${result.error}`);
                            }
                        }

                        if (ocrSuccess > 0) {
                            reply += `📸 *OCR nailed it!* ${ocrSuccess}/${context.images.length} screenshot(s) processed\n`;
                            if (extractedFields.length > 0) {
                                const labels = extractedFields.map(f => f.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                                reply += `📋 *Data pulled:* ${labels.join(', ')}\n`;
                            }
                            reply += `\n_Database updated. You can check the dashboard anytime._ 💅`;
                        } else {
                            reply += `${randomFrom(REACTIONS.fail)} OCR couldn't read any of the ${context.images.length} screenshot(s). Make sure they're clear!`;
                        }
                    } else {
                        reply += quip('noScreenshots');
                    }

                    await sock.sendMessage(jid, { text: reply.trim() }, { quoted: msg });
                    console.log(`✅ Updated @${username}: scrape=${scrapeResult.success}, OCR=${hasScreenshots ? context.images.length : 'none'}`);

                    // ═══════════════════════════════════════════
                    // MODE: SEARCH DATABASE
                    // ═══════════════════════════════════════════
                } else {
                    const result = await queryTrakr(query);
                    await sock.sendMessage(jid, { text: formatReply(result) }, { quoted: msg });
                    console.log(`✅ Search: "${query}"`);
                }

                await sock.sendMessage(jid, { react: { text: '', key: msg.key } });

            } catch (err) {
                console.error('Bot error:', err);
            }
        }
    });
}

console.log(`
╔══════════════════════════════════════════╗
║  🤖 FinBot — TRAKR Intelligence Agent  ║
║  "Fast, smart, no lunch breaks."        ║
╚══════════════════════════════════════════╝
Bot trigger: @${BOT_NAME}
API server:  ${TRAKR_API_URL}
Modes:       Search | Scrape | Scrape + OCR | Export
`);

startBot().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});
