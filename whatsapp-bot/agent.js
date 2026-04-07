/**
 * TRAKR Agent — LLM-powered intent classifier
 * Replaces all rule-based keyword matching with a single AI call.
 */

const { NVIDIA_API_URL, NVIDIA_KEY } = require('./config');

const SYSTEM_PROMPT = `You are FinBot, an AI assistant for an influencer talent agency database.

Given a user message (+ optional context), classify the INTENT and extract parameters. Return ONLY valid JSON.

## Actions:
1. **greeting** — hi, help, what can you do. ALWAYS set greeting_response to null.
2. **search** — Query/view data (DEFAULT for any data question)
3. **scrape** — ADD a new Instagram profile. Needs IG link/username + action word (add, scrape, save, put in db, store, database, fetch)
4. **update_field** — UPDATE a specific field for an existing creator. REQUIRES IG link.
5. **export** — Export data to Google Sheet
6. **search_and_export** — Search + export in one go
7. **bulk_import** — Import from a Google Sheets link (docs.google.com/spreadsheets)

## Updatable Fields (use exact column names):
| Column | Description | Example values |
|--------|-------------|----------------|
| managed_by | Manager name | "Rahul", "AgentC" |
| niche | Content category (comma-sep for multiple) | "Finance", "Beauty, Lifestyle" |
| language | Content language | "Hindi", "English", "Tamil" |
| gender | Creator gender | "Male", "Female" |
| location | City/region | "Mumbai", "Delhi", "Bangalore" |
| mail_id | Email address | "name@gmail.com" |
| contact_numbers | Phone number | "9876543210" |

## Rules:
- Bare IG link with NO action word → **search** (not scrape)
- IG link + ANY action word (add, update, save, put in db, store) → **scrape** or **update_field**
- IG link + "update"/"managed by"/"set"/"change" + field info → **update_field**
- IG link + "add"/"scrape"/"save"/"put in db"/"store" → **scrape**
- Multiple IG links + action word → **scrape**, extract FIRST username
- update_field without IG link → default to **search** instead
- "niche" updates: set update_field_value to ONLY the new niche (API auto-appends)
- When context is given, MERGE it into a standalone query (search engine has NO memory)

## Output Format (JSON only):
{"action":"...","query":"...or null","instagram_username":"...or null","update_field_name":"...or null","update_field_value":"...or null","greeting_response":null,"sheet_url":"...or null"}

## Examples:
"hi" → {"action":"greeting","query":null,"instagram_username":null,"greeting_response":null}
"show beauty creators in Mumbai" → {"action":"search","query":"beauty creators in Mumbai","instagram_username":null,"greeting_response":null}
"add https://instagram.com/creator1" → {"action":"scrape","query":null,"instagram_username":"creator1","greeting_response":null}
"https://instagram.com/creator1 managed by Rahul" → {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":"managed_by","update_field_value":"Rahul","greeting_response":null}
"https://instagram.com/creator1 niche is Finance" → {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":"niche","update_field_value":"Finance","greeting_response":null}
"https://instagram.com/creator1 email test@gmail.com" → {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":"mail_id","update_field_value":"test@gmail.com","greeting_response":null}
"https://instagram.com/creator1 location Mumbai" → {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":"location","update_field_value":"Mumbai","greeting_response":null}
"https://instagram.com/creator1 update details" → {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":null,"update_field_value":null,"greeting_response":null}
"export to sheet" → {"action":"export","query":null,"instagram_username":null,"greeting_response":null}
Context:"beauty creators". User:"sort by followers" → {"action":"search","query":"beauty creators sorted by followers","instagram_username":null,"greeting_response":null}
`;

/**
 * Classify user intent using NVIDIA LLM.
 * @param {string} userMessage - The user's message text
 * @param {string|null} context - Optional conversation context (quoted bot reply, etc.)
 * @returns {Object} - { action, query, instagram_username, greeting_response }
 */
async function classifyIntent(userMessage, context = null) {
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
        console.log(`🧠 [AGENT] Intent: ${result.action} | Query: ${result.query} | IG: ${result.instagram_username}`);
        return result;

    } catch (err) {
        console.error('⚠️ Agent classify error:', err.message);
        return fallbackClassify(userMessage);
    }
}

/**
 * Fallback classifier when LLM is unavailable.
 * Simple keyword matching as safety net.
 */
function fallbackClassify(text) {
    const lower = text.toLowerCase();
    console.log('⚠️ [AGENT] Using fallback classifier');

    // Check for Instagram link
    const igMatch = text.match(/instagram\.com\/(?:reel\/|p\/)?([A-Za-z0-9_.]+)/i);
    const igUsername = igMatch ? igMatch[1].split('?')[0].split('/')[0] : null;

    // Greeting
    const greetings = ['hi', 'hello', 'hey', 'sup', 'yo', 'help', 'menu', 'hii', 'hiii', 'thanks', 'thank', 'what can you do', 'what do you do', 'features'];
    if (!text.trim() || greetings.some(g => lower === g || lower === `${g}!`)) {
        return { action: 'greeting', query: null, instagram_username: null, greeting_response: "Hey! I'm FinBot 🤖 Ask me anything about your roster!" };
    }

    // Export
    const exportWords = ['export', 'sheet', 'spreadsheet', 'excel', 'csv'];
    if (exportWords.some(w => lower.includes(w))) {
        return { action: 'export', query: null, instagram_username: null, greeting_response: null };
    }

    // scrape (only if IG link present + action words)
    const scrapeWords = ['scrape', 'add', 'save', 'ocr', 'scan', 'screenshot', 'put in db', 'put in the db', 'store', 'database', 'fetch'];
    if (igUsername && scrapeWords.some(w => lower.includes(w))) {
        return { action: 'scrape', query: null, instagram_username: igUsername, greeting_response: null };
    }

    // If IG link present but no scrape words, DON'T auto-scrape — treat as search
    if (igUsername) {
        return { action: 'search', query: `show details for ${igUsername}`, instagram_username: null, greeting_response: null };
    }

    // Google Sheet link → bulk_import
    const sheetMatch = text.match(/https?:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9_-]+/i);
    if (sheetMatch) {
        return { action: 'bulk_import', query: null, instagram_username: null, sheet_url: sheetMatch[0], greeting_response: null };
    }

    // Default: search
    return { action: 'search', query: text, instagram_username: null, greeting_response: null };
}

module.exports = { classifyIntent };
