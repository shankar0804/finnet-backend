const { initAuthCreds, BufferJSON, proto } = require('@whiskeysockets/baileys');

/**
 * Creates Baileys auth state linked to a Supabase table.
 * 
 * OPTIMIZED:
 * - In-memory cache to avoid repeated DB reads
 * - Batch reads (single query for multiple keys)
 * - Batch writes (single upsert for all keys at once)
 * - Batch deletes
 * 
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} prefix - Optional prefix for keys (e.g. 'wa_')
 * @returns {Promise<{state: Object, saveCreds: Function}>}
 */
module.exports = async (supabase, prefix = 'wa_') => {

    // ─── In-memory cache ───
    // Keys are read far more often than written. Cache them to avoid
    // hitting Supabase on every Baileys key lookup.
    const cache = new Map();

    // ─── Single-row operations (for creds only) ───
    const readData = async (fileName) => {
        const key = `${prefix}${fileName}`;

        // Check cache first
        if (cache.has(key)) return cache.get(key);

        try {
            const { data, error } = await supabase
                .from('whatsapp_auth')
                .select('file_data')
                .eq('file_name', key)
                .single();

            if (error || !data) return null;
            const parsed = JSON.parse(data.file_data, BufferJSON.reviver);
            cache.set(key, parsed);
            return parsed;
        } catch (e) {
            return null;
        }
    };

    const writeData = async (fileName, data) => {
        const key = `${prefix}${fileName}`;
        const str = JSON.stringify(data, BufferJSON.replacer);
        cache.set(key, data);
        await supabase
            .from('whatsapp_auth')
            .upsert(
                { file_name: key, file_data: str, updated_at: new Date().toISOString() },
                { onConflict: 'file_name' }
            );
    };

    // ─── Batch operations (for keys.get / keys.set) ───

    /**
     * Read multiple keys in a SINGLE Supabase query.
     * Before: N individual SELECT queries
     * After:  1 SELECT ... WHERE file_name IN (...)
     */
    const readBatch = async (fileNames) => {
        if (fileNames.length === 0) return {};

        const keys = fileNames.map(f => `${prefix}${f}`);
        const result = {};

        // Check cache first, collect misses
        const misses = [];
        for (let i = 0; i < fileNames.length; i++) {
            if (cache.has(keys[i])) {
                result[fileNames[i]] = cache.get(keys[i]);
            } else {
                misses.push({ fileName: fileNames[i], key: keys[i] });
            }
        }

        // Fetch all cache misses in one query
        if (misses.length > 0) {
            try {
                const missKeys = misses.map(m => m.key);
                const { data, error } = await supabase
                    .from('whatsapp_auth')
                    .select('file_name, file_data')
                    .in('file_name', missKeys);

                if (!error && data) {
                    const dataMap = new Map(data.map(row => [row.file_name, row.file_data]));
                    for (const miss of misses) {
                        const raw = dataMap.get(miss.key);
                        if (raw) {
                            const parsed = JSON.parse(raw, BufferJSON.reviver);
                            cache.set(miss.key, parsed);
                            result[miss.fileName] = parsed;
                        }
                    }
                }
            } catch (e) {
                console.error('Batch read error:', e.message);
            }
        }

        return result;
    };

    /**
     * Write multiple keys in a SINGLE Supabase upsert.
     * Before: N individual UPSERT calls
     * After:  1 UPSERT with N rows
     */
    const writeBatch = async (rows) => {
        if (rows.length === 0) return;
        const now = new Date().toISOString();
        const payload = rows.map(({ fileName, data }) => {
            const key = `${prefix}${fileName}`;
            cache.set(key, data);
            return {
                file_name: key,
                file_data: JSON.stringify(data, BufferJSON.replacer),
                updated_at: now,
            };
        });
        try {
            await supabase
                .from('whatsapp_auth')
                .upsert(payload, { onConflict: 'file_name' });
        } catch (e) {
            console.error('Batch write error:', e.message);
        }
    };

    /**
     * Delete multiple keys in a SINGLE Supabase call.
     */
    const deleteBatch = async (fileNames) => {
        if (fileNames.length === 0) return;
        const keys = fileNames.map(f => `${prefix}${f}`);
        keys.forEach(k => cache.delete(k));
        try {
            await supabase
                .from('whatsapp_auth')
                .delete()
                .in('file_name', keys);
        } catch (e) {
            console.error('Batch delete error:', e.message);
        }
    };

    // ─── Initialize creds ───
    let creds = await readData('creds.json');
    if (!creds) {
        creds = initAuthCreds();
        await writeData('creds.json', creds);
    }

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    // Batch read all requested keys in one query
                    const fileNames = ids.map(id => `${type}-${id}.json`);
                    const batchResult = await readBatch(fileNames);

                    const dict = {};
                    for (const id of ids) {
                        const file = `${type}-${id}.json`;
                        let data = batchResult[file];
                        if (data) {
                            if (type === 'app-state-sync-key') {
                                data = proto.Message.AppStateSyncKeyData.fromObject(data);
                            }
                            dict[id] = data;
                        }
                    }
                    return dict;
                },
                set: async (data) => {
                    // Collect all writes and deletes, then execute in batch
                    const toWrite = [];
                    const toDelete = [];

                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const file = `${category}-${id}.json`;
                            if (value) {
                                toWrite.push({ fileName: file, data: value });
                            } else {
                                toDelete.push(file);
                            }
                        }
                    }

                    // Execute both in parallel
                    await Promise.all([
                        writeBatch(toWrite),
                        deleteBatch(toDelete),
                    ]);
                }
            }
        },
        saveCreds: async () => {
            await writeData('creds.json', creds);
        }
    };
};
