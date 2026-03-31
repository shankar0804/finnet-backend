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
const { BOT_NAME, TRAKR_API_URL, MAX_ROWS_IN_REPLY, MAX_MESSAGE_LENGTH, AUTH_DIR } = require('./config');
const { classifyIntent } = require('./agent');

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

statusServer.listen(STATUS_PORT, '127.0.0.1', () => {
    console.log(`📡 Bot status server running on http://127.0.0.1:${STATUS_PORT}`);
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
 * Fetch all roster data and export to Google Sheet.
 */
async function exportToSheet(searchQuery) {
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
            const res = await fetch(`${TRAKR_API_URL}/api/roster`);
            if (!res.ok) return { success: false, error: 'Failed to fetch roster data' };
            const data = await res.json();
            rows = data;
            title = `TRAKR Full Roster Export`;
        }

        if (!rows || rows.length === 0) {
            return { success: false, error: 'No data to export.' };
        }

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
            body: JSON.stringify({ query, skip_insight: true }),
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
                botState = { state: 'reconnecting', qr: null, qrBase64: null, phone: null };
                startBot();
            } else {
                botState = { state: 'logged_out', qr: null, qrBase64: null, phone: null };
                // If logged out (401) due to corrupted session on stop, delete auth and restart fresh
                const fs = require('fs');
                if (fs.existsSync(AUTH_DIR)) {
                    fs.rmSync(AUTH_DIR, { recursive: true, force: true });
                }
                console.log('🔄 Session cleared. Restarting bot for new QR scan...');
                startBot();
            }
        }
        if (connection === 'open') {
            const phoneNumber = sock.user?.id?.split(':')[0] || sock.user?.id?.split('@')[0] || null;
            botState = { state: 'connected', qr: null, qrBase64: null, phone: phoneNumber };
            console.log('✅ WhatsApp connected! Bot is live.');
            console.log(`🤖 Listening for @${BOT_NAME} in groups...\n`);
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        for (const msg of messages) {
            try {
                const jid = msg.key.remoteJid;
                if (!jid || !jid.endsWith('@g.us')) continue;

                // Store ALL messages in history
                addToHistory(jid, msg);

                if (msg.key.fromMe) continue;

                // Extract text from ALL possible WhatsApp message types
                const text = msg.message?.conversation
                    || msg.message?.extendedTextMessage?.text
                    || msg.message?.imageMessage?.caption
                    || msg.message?.videoMessage?.caption
                    || '';

                if (!text) continue;

                // ─── ACTIVATION CHECK ───
                // Method 1: Direct mention (@finbotlocal / finbotlocal)
                const mentionRe = new RegExp(`\\b${BOT_NAME}\\b`, 'i');
                const mentionMatch = mentionRe.test(text);

                // Method 2: Reply detection (to the bot, or to another tagged message)
                const contextInfo = msg.message?.extendedTextMessage?.contextInfo;
                const quotedParticipant = contextInfo?.participant || '';
                const botJid = sock.user?.id || '';
                const botNumber = botJid.split(':')[0] || botJid.split('@')[0];
                const botLid = sock.user?.lid || '';
                const baseLid = botLid ? (botLid.split(':')[0] || botLid.split('@')[0]) : '';
                
                // Extract quoted text
                let quotedText = '';
                if (contextInfo?.quotedMessage) {
                    quotedText = contextInfo.quotedMessage?.conversation
                        || contextInfo.quotedMessage?.extendedTextMessage?.text
                        || contextInfo.quotedMessage?.imageMessage?.caption
                        || '';
                }

                // WhatsApp Multi-Device masks linked devices with @lid. We check phone number, lid, OR our known bot emojis.
                const botEmojis = ['💡', '📊', '❌', '🤖', '📋', '📭', '👤', '✅'];
                const isBotText = botEmojis.some(emoji => quotedText.trim().startsWith(emoji));

                const isReplyToBot = !!(contextInfo?.quotedMessage && (
                    quotedParticipant.includes(botNumber) || 
                    (baseLid && quotedParticipant.includes(baseLid)) ||
                    (quotedParticipant.includes('@lid') && isBotText)
                ));

                // If they replied to their own previous question containing the tag
                const isReplyToTag = !!(quotedText && mentionRe.test(quotedText));

                const isActivated = mentionMatch || isReplyToBot || isReplyToTag;

                console.log(`\n--- DEBUG MESSAGE EVENT ---`);
                console.log(`Text: "${text.substring(0,50)}"`);
                console.log(`Quoted Participant: ${quotedParticipant}`);
                console.log(`Bot Number: ${botNumber}`);
                console.log(`Included? ${quotedParticipant.includes(botNumber)}`);
                console.log(`isReplyToBot: ${isReplyToBot} | isReplyToTag: ${isReplyToTag}`);
                console.log(`---------------------------\n`);

                if (!isActivated) continue;

                console.log(`📩 [ACTIVATED] mention=${mentionMatch} replyToBot=${isReplyToBot} replyToTag=${isReplyToTag}`);

                // ─── BUILD QUERY & CONTEXT ───
                // Strip bot name from query
                let query = text
                    .replace(/@\S+/g, '')
                    .replace(new RegExp(`\\b${BOT_NAME}\\b`, 'gi'), '')
                    .trim();

                // Build conversation context from quoted message
                let agentContext = null;
                if (quotedText) {
                    const ctxPrefix = isReplyToBot ? 'Previous bot reply' : 'Previous user question';
                    agentContext = `${ctxPrefix}: "${quotedText.substring(0, 500)}"`;
                }

                // Also check for IG links in the full text and chat history
                const igFromText = extractInstagramUsername(text);
                const historyContext = findContextFromHistory(jid);

                console.log(`🧠 [PRE-AGENT] Query: "${query}" | Context: ${agentContext ? 'yes' : 'none'} | IG: ${igFromText || historyContext.instagramUsername || 'none'}`);

                // ─── AGENT: CLASSIFY INTENT ───
                await sock.sendMessage(jid, { react: { text: randomFrom(REACTIONS.thinking), key: msg.key } });
                const intent = await classifyIntent(query, agentContext);

                console.log(`🧠 [AGENT] Action: ${intent.action}`);

                // ═══════════════════════════════════════════
                // ACTION: GREETING
                // ═══════════════════════════════════════════
                if (intent.action === 'greeting') {
                    const greetingText = intent.greeting_response || 
                        `🤖 *Yo! I'm FinBot — your influencer intel assistant.*\n\n` +
                        `🔍 *Ask me anything:*\n` +
                        `• _@${BOT_NAME} show all creators_\n` +
                        `• _@${BOT_NAME} who has the most followers?_\n\n` +
                        `📸 *Feed me data:*\n` +
                        `• Drop an IG link + _@${BOT_NAME} update_\n\n` +
                        `📊 *Export:*\n` +
                        `• _@${BOT_NAME} export to sheet_\n\n` +
                        `_Just reply to any of my messages to follow up!_ 💬`;
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

                        // If search_and_export, also show the search results
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
                    const username = intent.instagram_username || historyContext.instagramUsername || igFromText;
                    const field = intent.update_field_name;
                    const value = intent.update_field_value;

                    if (!username || !field || value === undefined || value === null) {
                        await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.fail)} I need the creator's username, the field, and the new value to update correctly.` }, { quoted: msg });
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
                         await sock.sendMessage(jid, { text: `${randomFrom(REACTIONS.error)} Error saving to database: ${e.message}` }, { quoted: msg });
                    }
                    
                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
                }

                // ═══════════════════════════════════════════
                // ACTION: SCRAPE (fetch from Instagram)
                // ═══════════════════════════════════════════
                if (intent.action === 'scrape') {
                    // Get username from: agent extraction > text > chat history
                    const username = intent.instagram_username
                        || igFromText
                        || historyContext.instagramUsername;

                    if (!username) {
                        await sock.sendMessage(jid, { text: quip('noLink') }, { quoted: msg });
                        await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                        continue;
                    }

                    const hasScreenshots = historyContext.images.length > 0;

                    await sock.sendMessage(jid, {
                        text: `${quip('scrapeStart')}\n\n👤 Target: *@${username}*${hasScreenshots ? `\n📸 Screenshots found: *${historyContext.images.length}*` : ''}`,
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

                    // OCR if screenshots present
                    if (hasScreenshots) {
                        let ocrSuccess = 0;
                        let extractedFields = [];

                        for (let i = 0; i < historyContext.images.length; i++) {
                            const result = await processImage(historyContext.images[i], username);
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
                            reply += `📸 *OCR nailed it!* ${ocrSuccess}/${historyContext.images.length} screenshot(s) processed\n`;
                            if (extractedFields.length > 0) {
                                const labels = extractedFields.map(f => f.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));
                                reply += `📋 *Data pulled:* ${labels.join(', ')}\n`;
                            }
                            reply += `\n_Database updated. Check the dashboard anytime._ 💅`;
                        } else {
                            reply += `${randomFrom(REACTIONS.fail)} OCR couldn't read the screenshot(s). Make sure they're clear!`;
                        }
                    } else {
                        reply += quip('noScreenshots');
                    }

                    await sock.sendMessage(jid, { text: reply.trim() }, { quoted: msg });
                    console.log(`✅ Updated @${username}: scrape=${scrapeResult.success}, OCR=${hasScreenshots ? historyContext.images.length : 'none'}`);
                    await sock.sendMessage(jid, { react: { text: '', key: msg.key } });
                    continue;
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

console.log(`
╔══════════════════════════════════════════╗
║  🧠 FinBot — TRAKR Agentic Bot          ║
║  "I think, therefore I bot."            ║
╚══════════════════════════════════════════╝
Bot trigger:  @${BOT_NAME}
API server:   ${TRAKR_API_URL}
Routing:      LLM-powered (agent.js)
Activation:   @mention OR reply-to-bot
`);

startBot().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});
