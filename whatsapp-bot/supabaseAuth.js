const { initAuthCreds, BufferJSON, proto } = require('@whiskeysockets/baileys');

/**
 * Creates Baileys auth state linked to a Supabase table.
 * 
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} prefix - Optional prefix for keys (e.g. 'auth_info')
 * @returns {Promise<{state: Object, saveCreds: Function}>}
 */
module.exports = async (supabase, prefix = 'wa_') => {
    
    // Read from Supabase Table `whatsapp_auth`
    const readData = async (fileName) => {
        const key = `${prefix}${fileName}`;
        try {
            const { data, error } = await supabase
                .from('whatsapp_auth')
                .select('file_data')
                .eq('file_name', key)
                .single();
                
            if (error || !data) return null;
            return JSON.parse(data.file_data, BufferJSON.reviver);
        } catch (e) {
            return null;
        }
    };

    // Write to Supabase Table `whatsapp_auth`
    const writeData = async (fileName, data) => {
        const key = `${prefix}${fileName}`;
        const str = JSON.stringify(data, BufferJSON.replacer);
        await supabase
            .from('whatsapp_auth')
            .upsert({ file_name: key, file_data: str }, { onConflict: 'file_name' });
    };

    // Delete from Supabase Table `whatsapp_auth`
    const removeData = async (fileName) => {
        const key = `${prefix}${fileName}`;
        await supabase
            .from('whatsapp_auth')
            .delete()
            .eq('file_name', key);
    };

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
                    const dict = {};
                    for (const id of ids) {
                        const file = `${type}-${id}.json`;
                        let data = await readData(file);
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
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const file = `${category}-${id}.json`;
                            if (value) {
                                await writeData(file, value);
                            } else {
                                await removeData(file);
                            }
                        }
                    }
                }
            }
        },
        saveCreds: async () => {
            await writeData('creds.json', creds);
        }
    };
};
