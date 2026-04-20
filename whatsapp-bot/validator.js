/**
 * TRAKR Bot — Validation & Whitelist utilities
 *
 * - Caches allowed niche + language values from the Trakr API (30s TTL)
 * - Caches the WhatsApp whitelist + enforcement toggle (30s TTL)
 * - Provides sender-phone extraction + whitelist check
 * - Provides an inline multi-field parser ("managed by X, email Y, phone Z")
 */

const { TRAKR_API_URL } = require('./config');

const CACHE_TTL_MS = 30 * 1000;
const HARD_GENDERS = ['Male', 'Female', 'Other'];

// ─── Allowed niche + language cache ───────────────────────────
let _allowedCache = { ts: 0, niche: [], language: [] };

async function refreshAllowed(force = false) {
    const now = Date.now();
    if (!force && now - _allowedCache.ts < CACHE_TTL_MS && _allowedCache.niche.length) return _allowedCache;
    try {
        const res = await fetch(`${TRAKR_API_URL}/api/allowed-values`);
        if (!res.ok) return _allowedCache;
        const data = await res.json();
        const niche = (data.niche || []).map(r => r.value).filter(Boolean);
        const language = (data.language || []).map(r => r.value).filter(Boolean);
        _allowedCache = { ts: now, niche, language };
    } catch (e) {
        console.warn('[VALIDATOR] refreshAllowed failed:', e.message);
    }
    return _allowedCache;
}

async function getAllowed(category) {
    await refreshAllowed();
    if (category === 'gender') return HARD_GENDERS.slice();
    return (_allowedCache[category] || []).slice();
}

/**
 * Returns canonically-cased value if `value` is allowed for `category`, else null.
 * For niche, a comma-separated list is validated and canonicalized part-by-part.
 */
async function canonicalValue(category, value) {
    if (value === null || value === undefined) return null;
    const v = String(value).trim();
    if (!v) return null;
    if (category === 'gender') {
        const hit = HARD_GENDERS.find(g => g.toLowerCase() === v.toLowerCase());
        return hit || null;
    }
    await refreshAllowed();
    const list = _allowedCache[category] || [];
    const lookup = new Map(list.map(x => [x.toLowerCase(), x]));
    if (category === 'niche') {
        const parts = v.split(',').map(p => p.trim()).filter(Boolean);
        if (!parts.length) return null;
        const canon = [];
        for (const p of parts) {
            const c = lookup.get(p.toLowerCase());
            if (!c) return null;
            canon.push(c);
        }
        return canon.join(', ');
    }
    return lookup.get(v.toLowerCase()) || null;
}

function formatAllowedList(values, limit = 25) {
    if (!values || !values.length) return '(no values configured)';
    const shown = values.slice(0, limit);
    const more = values.length - shown.length;
    return shown.join(', ') + (more > 0 ? `, … +${more} more` : '');
}

// ─── Whitelist cache ───────────────────────────────────────────
let _whitelistCache = { ts: 0, enabled: false, numbers: new Set() };

async function refreshWhitelist(force = false) {
    const now = Date.now();
    if (!force && now - _whitelistCache.ts < CACHE_TTL_MS) return _whitelistCache;
    try {
        const [settingsRes, listRes] = await Promise.all([
            fetch(`${TRAKR_API_URL}/api/whatsapp/settings`),
            fetch(`${TRAKR_API_URL}/api/whatsapp/whitelist`),
        ]);
        const settings = settingsRes.ok ? await settingsRes.json() : {};
        const list = listRes.ok ? await listRes.json() : [];
        const enabled = String(settings.whitelist_enabled || 'false').toLowerCase() === 'true';
        const numbers = new Set(
            (list || [])
                .filter(e => e.enabled !== false)
                .map(e => String(e.phone_number || '').replace(/\D/g, ''))
                .filter(Boolean)
        );
        _whitelistCache = { ts: now, enabled, numbers };
    } catch (e) {
        console.warn('[VALIDATOR] refreshWhitelist failed:', e.message);
    }
    return _whitelistCache;
}

/**
 * Extract the sender's phone number from a Baileys message.
 * In DMs, msg.key.remoteJid is the sender's JID.
 * In groups, msg.key.participant holds the actual sender.
 */
function getSenderPhone(msg) {
    const sender = msg?.key?.participant || msg?.key?.remoteJid || '';
    if (!sender) return '';
    const raw = sender.split('@')[0].split(':')[0];
    return raw.replace(/\D/g, '');
}

/**
 * Returns {allowed, enabled, phone}. If enforcement is OFF → always allowed.
 */
async function isSenderAllowed(msg) {
    const wl = await refreshWhitelist();
    const phone = getSenderPhone(msg);
    if (!wl.enabled) return { allowed: true, enabled: false, phone };
    if (!phone) return { allowed: false, enabled: true, phone: '' };
    return { allowed: wl.numbers.has(phone), enabled: true, phone };
}

// ─── Inline multi-field parser ─────────────────────────────────
// Catches common patterns used by the team in WhatsApp, e.g.:
//   "creator is managed by Finnet Media"
//   "email is sharan@gmail.co.in"
//   "number is 9876541230"
//   "niche Finance"
//   "language Hindi"
//   "gender Male"
//   "location Mumbai"
// Returns an array of { field, value } in the order they appear.
function parseInlineUpdates(text) {
    if (!text || typeof text !== 'string') return [];
    // Strip URLs so regexes don't eat them
    const stripped = text.replace(/https?:\/\/\S+/g, ' ');

    const patterns = [
        // managed_by — "managed by X", stops at newline or comma + typical keyword
        { field: 'managed_by', re: /managed\s*by\s*(?:is|:|-)?\s*([^\n,]+?)(?=\s*(?:$|\n|,|\.|;|email|mail|number|phone|contact|niche|language|gender|location))/i },
        // mail_id — explicit email keywords or a raw email address
        { field: 'mail_id',   re: /(?:e[-\s]?mail|email|mail(?:\s*id)?)\s*(?:is|:|-)?\s*([\w.+-]+@[\w-]+\.[\w.-]+)/i },
        { field: 'mail_id',   re: /\b([\w.+-]+@[\w-]+\.[\w.-]+)\b/i },
        // contact_numbers — "number is 98765…" or 10+ digit sequence
        { field: 'contact_numbers', re: /(?:phone|number|contact|mobile|whatsapp)\s*(?:number|no\.?)?\s*(?:is|:|-)?\s*((?:\+?\d[\d\s-]{8,14}\d))/i },
        // niche / language / gender / location — "niche is X" or "niche X"
        { field: 'niche',     re: /\bniche\s*(?:is|:|-)?\s*([^\n,]+?)(?=\s*(?:$|\n|,|\.|;|email|mail|number|phone|contact|managed|language|gender|location))/i },
        { field: 'language',  re: /\blanguage\s*(?:is|:|-)?\s*([^\n,]+?)(?=\s*(?:$|\n|,|\.|;|email|mail|number|phone|contact|managed|niche|gender|location))/i },
        { field: 'gender',    re: /\bgender\s*(?:is|:|-)?\s*(male|female|other)\b/i },
        { field: 'location',  re: /\blocation\s*(?:is|:|-)?\s*([^\n,]+?)(?=\s*(?:$|\n|,|\.|;|email|mail|number|phone|contact|managed|niche|language|gender))/i },
    ];

    const out = [];
    const seen = new Set();
    for (const { field, re } of patterns) {
        const m = stripped.match(re);
        if (!m) continue;
        const value = (m[1] || '').trim().replace(/[\s.,;]+$/, '');
        if (!value) continue;
        if (seen.has(field)) continue; // first match wins
        seen.add(field);
        out.push({ field, value });
    }
    return out;
}

// ─── Multi-platform creator target extractor ───────────────────
// Returns array of { platform: 'instagram'|'youtube'|'linkedin', handle, url }
// in the order they appear. Duplicates are removed.
function extractCreatorTargets(text) {
    if (!text || typeof text !== 'string') return [];
    const out = [];
    const seen = new Set();
    const push = (platform, handle, url) => {
        if (!handle) return;
        const key = `${platform}:${handle.toLowerCase()}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push({ platform, handle, url: url || '' });
    };

    // Instagram: instagram.com/<user>, instagram.com/reel/..., instagram.com/p/...
    const igRe = /https?:\/\/(?:www\.)?instagram\.com\/(?:reel\/|p\/|tv\/)?([A-Za-z0-9_.]+)/gi;
    for (const m of text.matchAll(igRe)) {
        const h = (m[1] || '').split('?')[0].split('/')[0];
        if (h && !['reel', 'p', 'tv', 'explore'].includes(h.toLowerCase())) {
            push('instagram', h, m[0]);
        }
    }

    // YouTube: youtube.com/@handle, youtube.com/channel/UCxxxx, youtu.be/VIDEO (ignore)
    const ytHandleRe = /https?:\/\/(?:www\.)?youtube\.com\/@([A-Za-z0-9_.-]+)/gi;
    for (const m of text.matchAll(ytHandleRe)) {
        push('youtube', m[1], m[0]);
    }
    const ytChannelRe = /https?:\/\/(?:www\.)?youtube\.com\/channel\/(UC[A-Za-z0-9_-]{22})/gi;
    for (const m of text.matchAll(ytChannelRe)) {
        push('youtube', m[1], m[0]);
    }

    // LinkedIn: linkedin.com/in/<slug>
    const liRe = /https?:\/\/(?:www\.)?linkedin\.com\/in\/([A-Za-z0-9_%-]+)/gi;
    for (const m of text.matchAll(liRe)) {
        const h = decodeURIComponent(m[1]);
        push('linkedin', h, m[0]);
    }

    return out;
}

module.exports = {
    // allowed values
    refreshAllowed,
    getAllowed,
    canonicalValue,
    formatAllowedList,
    HARD_GENDERS,
    // whitelist
    refreshWhitelist,
    isSenderAllowed,
    getSenderPhone,
    // parser
    parseInlineUpdates,
    // platform
    extractCreatorTargets,
};
