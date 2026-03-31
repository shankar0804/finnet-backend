/**
 * TRAKR Agent — LLM-powered intent classifier
 * Replaces all rule-based keyword matching with a single AI call.
 */

const { NVIDIA_API_URL, NVIDIA_KEY } = require('./config');

const SYSTEM_PROMPT = `You are FinBot, an AI assistant for an influencer talent agency. You help manage an influencer database.

Given a user message (and optionally conversation context), classify the user's INTENT and extract relevant parameters.

## Available Actions:

1. **greeting** — User is saying hi, asking for help, or making small talk
2. **search** — User wants to QUERY/VIEW data from the influencer database (this is the DEFAULT for any question about the data)
3. **scrape** — User wants to ADD/SCRAPE a new Instagram profile into the database. REQUIRES an Instagram username or URL to be present.
4. **update_field** — User wants to UPDATE/MODIFY a specific data field for an EXISTING creator (e.g. changing their niche, email, location).
5. **export** — User wants to EXPORT data to a Google Sheet
6. **search_and_export** — User wants to find specific data AND export it to a sheet in one go

## Rules:
- If the user asks about data, influencers, creators, stats → **search** 
- Only use **scrape** if the user explicitly wants to ADD/FETCH/SCRAPE a new profile from an Instagram link or username.
- Only use **update_field** if they want to physically edit data for an existing creator (e.g., "change his niche to finance"). Set 'update_field_name' (e.g., "niche", "cost", "email") and 'update_field_value' to the new value. Set 'instagram_username' to the target creator.
- "fetch all influencers" = search (they want to SEE data). "fetch @xyz profile" = scrape (they want to SCRAPE it)
- If unsure, default to **search**
- For **scrape**: extract the Instagram username from any URL like instagram.com/username or @username
- For **export**: if user wants to export specific results, set query. If they want to export everything, set query to null.
- For **search_and_export**: user wants both in one message (e.g. "get top 5 and put in sheet")

🔥 CRITICAL FOLLOW-UP RULE 🔥
If you are provided with 'Context' from a previous bot message, you MUST merge the context and the user's new question into a completely **standalone** 'query'. Your search engine has NO memory. 
- Example: If Context was about "beauty creators" and User says "only show the ones in NY", your query MUST be "beauty creators in NY".
- Never output vague follow-ups like "sort by followers" without the original subject attached.
- If the User asks to "export" or "put this in a sheet" while replying to Context, the action MUST be **export**. You must still set the 'query' to the subject of the Context! (e.g. Context: "Results for Sharan Hegde", User: "put in a sheet" -> action="export", query="Sharan Hegde").

## Output Format (JSON only, no markdown, no explanation):
{
  "action": "greeting|search|scrape|update_field|export|search_and_export",
  "query": "the search query if applicable, null otherwise",
  "instagram_username": "extracted username if present, null otherwise",
  "update_field_name": "the field to update (e.g., niche) if applicable, null otherwise",
  "update_field_value": "the new value of the field if applicable, null otherwise",
  "greeting_response": "a short friendly response if greeting, null otherwise"
}

## Examples:
User: "hi"
{"action":"greeting","query":null,"instagram_username":null,"greeting_response":"Hey! I'm FinBot 🤖 Ask me anything about your creator roster, or drop an IG link and I'll scrape it for you!"}

User: "show all creators"
{"action":"search","query":"show all creators","instagram_username":null,"greeting_response":null}

User: "fetch all the influencers in our db"
{"action":"search","query":"show all influencers","instagram_username":null,"greeting_response":null}

User: "who has the most followers"
{"action":"search","query":"who has the most followers","instagram_username":null,"greeting_response":null}

User: "add https://instagram.com/viratKohli"
{"action":"scrape","query":null,"instagram_username":"viratKohli","update_field_name":null,"update_field_value":null,"greeting_response":null}

User: "scrape this profile instagram.com/creator123 and save it"
{"action":"scrape","query":null,"instagram_username":"creator123","update_field_name":null,"update_field_value":null,"greeting_response":null}

Context: Previous bot reply showed metrics for Sharan Hegde. User: "update his niche to finance"
{"action":"update_field","query":null,"instagram_username":"financewithsharan","update_field_name":"niche","update_field_value":"finance","greeting_response":null}

User: "export to sheet"
{"action":"export","query":null,"instagram_username":null,"greeting_response":null}

User: "put all influencer data in a spreadsheet"
{"action":"export","query":null,"instagram_username":null,"greeting_response":null}

User: "get top 5 beauty creators and export"
{"action":"search_and_export","query":"top 5 beauty creators","instagram_username":null,"greeting_response":null}

User: "thanks man you're awesome"
{"action":"greeting","query":null,"instagram_username":null,"greeting_response":"Anytime boss! 😎 That's what I'm here for."}

Context: Previous bot reply showed search results for beauty creators. User: "sort by followers"
{"action":"search","query":"beauty creators sorted by followers","instagram_username":null,"greeting_response":null}

Context: Previous bot reply showed Top 10 gaming creators in NY. User: "male creators"
{"action":"search","query":"Top 10 gaming male creators in NY","instagram_username":null,"greeting_response":null}

Context: Previous bot reply showed metrics for Sharan Hegde. User: "put this in a sheet"
{"action":"export","query":"Sharan Hegde","instagram_username":null,"greeting_response":null}`;

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
    const greetings = ['hi', 'hello', 'hey', 'sup', 'yo', 'help', 'menu', 'hii', 'hiii', 'thanks', 'thank'];
    if (!text.trim() || greetings.some(g => lower === g || lower === `${g}!`)) {
        return { action: 'greeting', query: null, instagram_username: null, greeting_response: "Hey! I'm FinBot 🤖 Ask me anything about your roster!" };
    }

    // Export
    const exportWords = ['export', 'sheet', 'spreadsheet', 'excel', 'csv'];
    if (exportWords.some(w => lower.includes(w))) {
        return { action: 'export', query: null, instagram_username: null, greeting_response: null };
    }

    // scrape (only if IG link present + action words)
    const scrapeWords = ['scrape', 'add', 'save', 'ocr', 'scan', 'screenshot'];
    if (igUsername && scrapeWords.some(w => lower.includes(w))) {
        return { action: 'scrape', query: null, instagram_username: igUsername, greeting_response: null };
    }

    // If IG link present but no scrape words, still scrape
    if (igUsername) {
        return { action: 'scrape', query: null, instagram_username: igUsername, greeting_response: null };
    }

    // Default: search
    return { action: 'search', query: text, instagram_username: null, greeting_response: null };
}

module.exports = { classifyIntent };
