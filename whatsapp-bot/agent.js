/**
 * TRAKR Agent — LLM-powered intent classifier
 * Replaces all rule-based keyword matching with a single AI call.
 */

const { NVIDIA_API_URL, NVIDIA_KEY } = require('./config');

const SYSTEM_PROMPT = `You are FinBot, an AI assistant for an influencer talent agency database.

Given a user message (+ optional context), classify the INTENT and extract parameters. Return ONLY valid JSON.

## Platforms supported:
- **instagram** — instagram.com/<handle> or instagram.com/reel/... or instagram.com/p/...
- **youtube**  — youtube.com/@<handle> or youtube.com/channel/UCxxxx
- **linkedin** — linkedin.com/in/<profile-slug>

Always set "platform" to one of: "instagram", "youtube", "linkedin", or null (for search/bulk_import/export/greeting when platform is irrelevant).

## Actions:
1. **greeting** — hi, help, what can you do. ALWAYS set greeting_response to null.
2. **search** — Query/view data (DEFAULT for any data question)
3. **scrape** — ADD a new creator profile. Needs a creator LINK (IG/YT/LI) + action word (add, scrape, save, put in db, store, database, fetch). If the user also provides field updates in the same message (managed by, email, niche, language, gender, location, phone), put them in \`extra_updates\`.
4. **update_field** — UPDATE ONE field for an existing creator. REQUIRES a creator link.
5. **update_multi** — UPDATE 2+ fields in one message for an existing creator. REQUIRES a creator link.
6. **export** — Export data to Google Sheet
7. **search_and_export** — Search + export in one go
8. **bulk_import** — Import from a Google Sheets link (docs.google.com/spreadsheets). If the message also has field updates (managed by, email, niche, …), put them in \`extra_updates\` — they will be applied as defaults to every imported row.

## Updatable Fields (use exact column names — same names across all platforms):
| Column          | Description               | Example values |
|-----------------|---------------------------|----------------|
| managed_by      | Manager name              | "Rahul", "Finnet Media" |
| niche           | Content category (CSV)    | "Finance", "Beauty, Lifestyle" |
| language        | Content language          | "Hindi", "English", "Tamil" |
| gender          | Creator gender            | "Male", "Female", "Other" |
| location        | City/region               | "Mumbai", "Delhi" |
| mail_id         | Email                     | "name@gmail.com" |
| contact_numbers | Phone                     | "9876543210" |

## Platform extraction rules:
- For **scrape / update_field / update_multi**, set \`platform\` based on the link:
  * instagram.com/...       → platform:"instagram", instagram_username = handle
  * youtube.com/@handle or channel/UC… → platform:"youtube",   instagram_username = handle or channel id
  * linkedin.com/in/slug    → platform:"linkedin",  instagram_username = slug
  * NOTE: the field name stays "instagram_username" for backward-compat, but it carries the YT/LI handle when platform is set to those.

## Rules:
- Bare creator link with NO action word → **search** (not scrape)
- Link + ANY action word → **scrape** / **update_field** / **update_multi**
- Link + ONE field update → **update_field**
- Link + 2+ field updates → **update_multi**
- Link + "add"/"scrape"/"save"/"put in db"/"store" → **scrape** (add \`extra_updates\` if extra fields given)
- update_field / update_multi without a creator link → default to **search** instead
- "niche" updates: use ONLY the new niche (API auto-appends)
- When context is given, MERGE it into a standalone query (search engine has NO memory)

## Output Format (JSON only):
{"action":"...","query":"...or null","platform":"instagram|youtube|linkedin|null","instagram_username":"...or null","update_field_name":"...or null","update_field_value":"...or null","updates":null_or_array,"extra_updates":null_or_array,"greeting_response":null,"sheet_url":"...or null"}

Where \`updates\` and \`extra_updates\` are arrays of {"field":"...","value":"..."}.

## Examples:
"hi" → {"action":"greeting","query":null,"platform":null,"instagram_username":null,"greeting_response":null}
"show beauty creators in Mumbai" → {"action":"search","query":"beauty creators in Mumbai","platform":null,"instagram_username":null,"greeting_response":null}
"add https://instagram.com/creator1" → {"action":"scrape","query":null,"platform":"instagram","instagram_username":"creator1","extra_updates":null,"greeting_response":null}
"add https://youtube.com/@financewithsharan" → {"action":"scrape","query":null,"platform":"youtube","instagram_username":"financewithsharan","extra_updates":null,"greeting_response":null}
"add https://www.linkedin.com/in/sharan-hegde" → {"action":"scrape","query":null,"platform":"linkedin","instagram_username":"sharan-hegde","extra_updates":null,"greeting_response":null}
"https://youtube.com/@mkbhd managed by AgentC" → {"action":"update_field","platform":"youtube","instagram_username":"mkbhd","update_field_name":"managed_by","update_field_value":"AgentC","greeting_response":null}
"https://linkedin.com/in/alice-b managed by Finnet, email a@b.co, number 9876543210" → {"action":"update_multi","platform":"linkedin","instagram_username":"alice-b","updates":[{"field":"managed_by","value":"Finnet"},{"field":"mail_id","value":"a@b.co"},{"field":"contact_numbers","value":"9876543210"}],"greeting_response":null}
"https://instagram.com/creator1 managed by Finnet Media add to db" → {"action":"scrape","platform":"instagram","instagram_username":"creator1","extra_updates":[{"field":"managed_by","value":"Finnet Media"}],"greeting_response":null}
"https://instagram.com/creator1 niche is Finance" → {"action":"update_field","platform":"instagram","instagram_username":"creator1","update_field_name":"niche","update_field_value":"Finance","greeting_response":null}
"https://docs.google.com/spreadsheets/d/ABC123 record in db managed by Finnet" → {"action":"bulk_import","sheet_url":"https://docs.google.com/spreadsheets/d/ABC123","platform":null,"extra_updates":[{"field":"managed_by","value":"Finnet"}],"greeting_response":null}
"export to sheet" → {"action":"export","query":null,"platform":null,"instagram_username":null,"greeting_response":null}
Context:"beauty creators". User:"sort by followers" → {"action":"search","query":"beauty creators sorted by followers","platform":null,"instagram_username":null,"greeting_response":null}
`;

/**
 * Classify user intent using NVIDIA LLM.
 * @param {string} userMessage - The user's message text
 * @param {string|null} context - Optional conversation context (quoted bot reply, etc.)
 * @returns {Object} - { action, query, instagram_username, greeting_response }
 */
// Hard timeout so a stuck LLM call can't freeze the user's message queue.
const INTENT_TIMEOUT_MS = parseInt(process.env.INTENT_TIMEOUT_MS || '10000', 10);

async function classifyIntent(userMessage, context = null) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), INTENT_TIMEOUT_MS);
    try {
        let userPrompt = userMessage;
        if (context) {
            userPrompt = `Context: ${context}\nUser: ${userMessage}`;
        }

        const t0 = Date.now();
        const response = await fetch(`${NVIDIA_API_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${NVIDIA_KEY}`,
            },
            body: JSON.stringify({
                model: 'meta/llama-3.1-8b-instruct',
                messages: [
                    { role: 'system', content: SYSTEM_PROMPT },
                    { role: 'user', content: userPrompt },
                ],
                temperature: 0.0,
                max_tokens: 256,
            }),
            signal: controller.signal,
        });

        if (!response.ok) {
            console.error(`❌ LLM API error: ${response.status}`);
            return fallbackClassify(userMessage);
        }

        const data = await response.json();
        console.log(`[TIMING] Agent intent classification: ${Date.now() - t0}ms`);
        let raw = data.choices?.[0]?.message?.content?.trim() || '';

        // Clean markdown wrapping if present
        raw = raw.replace(/```json\s*/gi, '').replace(/```/g, '').trim();

        const result = JSON.parse(raw);
        const upd = Array.isArray(result.updates) ? result.updates.length : 0;
        const extra = Array.isArray(result.extra_updates) ? result.extra_updates.length : 0;
        console.log(`🧠 [AGENT] Intent: ${result.action} | platform:${result.platform || '-'} | Query: ${result.query} | handle: ${result.instagram_username} | updates:${upd} extra:${extra}`);
        return result;

    } catch (err) {
        if (err.name === 'AbortError') {
            console.warn(`⏱️ Agent classify timed out after ${INTENT_TIMEOUT_MS}ms — using rule-based fallback`);
        } else {
            console.error('⚠️ Agent classify error:', err.message);
        }
        return fallbackClassify(userMessage);
    } finally {
        clearTimeout(timer);
    }
}

/**
 * Fallback classifier when LLM is unavailable.
 * Simple keyword matching as safety net.
 */
function fallbackClassify(text) {
    const lower = text.toLowerCase();
    console.log('⚠️ [AGENT] Using fallback classifier');

    // Detect creator links across all platforms
    let platform = null;
    let handle = null;

    const ig = text.match(/instagram\.com\/(?:reel\/|p\/|tv\/)?([A-Za-z0-9_.]+)/i);
    if (ig) { platform = 'instagram'; handle = ig[1].split('?')[0].split('/')[0]; }

    if (!handle) {
        const yt = text.match(/youtube\.com\/@([A-Za-z0-9_.-]+)/i)
                || text.match(/youtube\.com\/channel\/(UC[A-Za-z0-9_-]{22})/i);
        if (yt) { platform = 'youtube'; handle = yt[1]; }
    }

    if (!handle) {
        const li = text.match(/linkedin\.com\/in\/([A-Za-z0-9_%-]+)/i);
        if (li) { platform = 'linkedin'; handle = decodeURIComponent(li[1]); }
    }

    const greetings = ['hi', 'hello', 'hey', 'sup', 'yo', 'help', 'menu', 'hii', 'hiii', 'thanks', 'thank', 'what can you do', 'what do you do', 'features'];
    if (!text.trim() || greetings.some(g => lower === g || lower === `${g}!`)) {
        return { action: 'greeting', query: null, platform: null, instagram_username: null, greeting_response: "Hey! I'm FinBot 🤖 Ask me anything about your roster!" };
    }

    const exportWords = ['export', 'sheet', 'spreadsheet', 'excel', 'csv'];
    if (exportWords.some(w => lower.includes(w)) && !/docs\.google\.com\/spreadsheets/i.test(text)) {
        return { action: 'export', query: null, platform: null, instagram_username: null, greeting_response: null };
    }

    const scrapeWords = ['scrape', 'add', 'save', 'ocr', 'scan', 'screenshot', 'put in db', 'put in the db', 'store', 'database', 'fetch'];
    if (handle && scrapeWords.some(w => lower.includes(w))) {
        return { action: 'scrape', query: null, platform, instagram_username: handle, greeting_response: null };
    }

    if (handle) {
        return { action: 'search', query: `show details for ${handle}`, platform: null, instagram_username: null, greeting_response: null };
    }

    const sheetMatch = text.match(/https?:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9_-]+/i);
    if (sheetMatch) {
        return { action: 'bulk_import', query: null, platform: null, instagram_username: null, sheet_url: sheetMatch[0], greeting_response: null };
    }

    return { action: 'search', query: text, platform: null, instagram_username: null, greeting_response: null };
}

module.exports = { classifyIntent };
