/**
 * TRAKR Agent — LLM-powered intent classifier
 * Replaces all rule-based keyword matching with a single AI call.
 */

const { NVIDIA_API_URL, NVIDIA_KEY } = require('./config');

const SYSTEM_PROMPT = `You are FinBot, an AI assistant for an influencer talent agency. You help manage an influencer database.

Given a user message (and optionally conversation context), classify the user's INTENT and extract relevant parameters.

## Available Actions:

1. **greeting** — User is saying hi, asking for help, asking what you can do, or making small talk. ALWAYS set greeting_response to null.
2. **search** — User wants to QUERY/VIEW data from the influencer database (this is the DEFAULT for any question about the data)
3. **scrape** — User wants to ADD/SCRAPE a new Instagram profile into the database. REQUIRES an Instagram username or URL to be present.
4. **update_field** — User wants to UPDATE/MODIFY a specific data field for an EXISTING creator (e.g. changing their niche, email, location).
5. **export** — User wants to EXPORT data to a Google Sheet
6. **search_and_export** — User wants to find specific data AND export it to a sheet in one go
7. **bulk_import** — User sends a Google Sheets link (docs.google.com/spreadsheets) and wants to IMPORT/UPDATE the database from it

## Rules:
- If the user asks about data, influencers, creators, stats → **search** 
- Only use **scrape** if the user explicitly wants to ADD/STORE/SCRAPE a new Instagram profile into the database. REQUIRES an Instagram username or URL. Keywords: "add", "scrape", "save", "fetch", "put in db", "put in the db", "store", "database", "save in db", "add to db".
- If the user sends MULTIPLE Instagram links with an action word (add, put in db, scrape, save), it is still **scrape**. Extract the FIRST username.
- If someone sends a bare Instagram link with NO action word at all (just the URL, nothing else), classify as **search** with a query about that user. But if ANY action word is present alongside the link(s), classify accordingly.
- If someone sends a link with "update" → classify as **update_field** and extract the Instagram username from the link.
- **update_field REQUIRES an Instagram link. No link = no update. Default to search instead.**
- If someone sends a link + "update this person details" or "update details" with NO specific field, set action=**update_field**, extract the username, leave update_field_name and update_field_value as null.
- For update_field, map the user's intent to the correct db column name from the schema below.
- "fetch all influencers" = search. "fetch @xyz profile" = scrape.
- If unsure, default to **search**
- For **scrape**: extract the Instagram username from any URL like instagram.com/username or @username
- For **export**: if user wants to export specific results, set query. If everything, set query to null.
- For **search_and_export**: user wants both in one message (e.g. "get top 5 and put in sheet")
- For **bulk_import**: user sends a docs.google.com/spreadsheets link and wants to import/update/add creators from it. Keywords: "import", "update db", "add from sheet", "bulk". Set 'sheet_url' to the full Google Sheets URL.

## Updatable Fields Schema (use exact column names for update_field_name):
managed_by     — Who manages this creator (text, e.g. "AgentC", "Rahul")
niche          — Content category. Supports MULTIPLE niches comma-separated (e.g. "Finance", "Finance, Infotainment"). To ADD a niche, set update_field_value to ONLY the new niche (e.g. "Infotainment") — the API will auto-append it.
language       — Content language (text, e.g. "Hindi", "English")
gender         — Creator gender (text, e.g. "Male", "Female")
location       — City/region (text, e.g. "Mumbai", "Delhi")
mail_id        — Email address (text)
contact_numbers — Phone number (text)

🔥 CRITICAL FOLLOW-UP RULE 🔥
If you are provided with 'Context' from a previous bot message, you MUST merge the context and the user's new question into a completely **standalone** 'query'. Your search engine has NO memory. 
- Example: If Context was about "beauty creators" and User says "only show the ones in NY", your query MUST be "beauty creators in NY".
- Never output vague follow-ups like "sort by followers" without the original subject attached.
- If the User asks to "export" or "put this in a sheet" while replying to Context, the action MUST be **export**. You must still set the 'query' to the subject of the Context! (e.g. Context: "Results for Sharan Hegde", User: "put in a sheet" -> action="export", query="Sharan Hegde").

## Output Format (JSON only, no markdown, no explanation):
{
  "action": "greeting|search|scrape|update_field|export|search_and_export|bulk_import",
  "query": "the search query if applicable, null otherwise",
  "instagram_username": "extracted username if present, null otherwise",
  "update_field_name": "the field to update (e.g., niche) if applicable, null otherwise",
  "update_field_value": "the new value of the field if applicable, null otherwise",
  "greeting_response": "a short friendly response if greeting, null otherwise",
  "sheet_url": "the full Google Sheets URL if bulk_import, null otherwise"
}

## Examples (one per action):
User: "hi" -> {"action":"greeting","query":null,"instagram_username":null,"greeting_response":null}
User: "what can you do" -> {"action":"greeting","query":null,"instagram_username":null,"greeting_response":null}
User: "help" -> {"action":"greeting","query":null,"instagram_username":null,"greeting_response":null 🤖"}
User: "show top 5 in mumbai" -> {"action":"search","query":"top 5 creators in mumbai","instagram_username":null,"greeting_response":null}
User: "add https://instagram.com/viratKohli" -> {"action":"scrape","query":null,"instagram_username":"viratKohli","update_field_name":null,"update_field_value":null,"greeting_response":null}
User: "put in the db https://instagram.com/creator1 https://instagram.com/creator2" -> {"action":"scrape","query":null,"instagram_username":"creator1","greeting_response":null}
User: "https://instagram.com/riaspeaks managed by Rahul" -> {"action":"update_field","query":null,"instagram_username":"riaspeaks","update_field_name":"managed_by","update_field_value":"Rahul","greeting_response":null}
User: "https://instagram.com/riaspeaks update details" -> {"action":"update_field","query":null,"instagram_username":"riaspeaks","update_field_name":null,"update_field_value":null,"greeting_response":null}
User: "https://instagram.com/creator1 also add infotainment niche" -> {"action":"update_field","query":null,"instagram_username":"creator1","update_field_name":"niche","update_field_value":"Infotainment","greeting_response":null}
User: "update managed_by to Rahul" (NO link) -> {"action":"search","query":"creators managed by Rahul","instagram_username":null,"greeting_response":null}
User: "export to sheet" -> {"action":"export","query":null,"instagram_username":null,"greeting_response":null}
User: "get top 5 beauty and export" -> {"action":"search_and_export","query":"top 5 beauty creators","instagram_username":null,"greeting_response":null}
Context: "beauty creators in Mumbai". User: "sort by followers" -> {"action":"search","query":"beauty creators in Mumbai sorted by followers","instagram_username":null,"greeting_response":null}
User: "https://docs.google.com/spreadsheets/d/abc123/edit update db from this sheet" -> {"action":"bulk_import","query":null,"instagram_username":null,"sheet_url":"https://docs.google.com/spreadsheets/d/abc123/edit","greeting_response":null}
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
