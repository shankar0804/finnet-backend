const API_BASE = import.meta.env.VITE_API_BASE || '';

/**
 * Centralized API fetch wrapper.
 * Auto-attaches JWT token from sessionStorage and prepends API_BASE.
 */
export async function api(path, options = {}) {
  const token = sessionStorage.getItem('trakr_token') || '';
  const headers = { ...options.headers };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  const data = await res.json();

  if (!res.ok) {
    const err = new Error(data.error || data.details || `Request failed (${res.status})`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

/** Shorthand for GET */
api.get = (path) => api(path);

/** Shorthand for POST with JSON body */
api.post = (path, body) =>
  api(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

/** Shorthand for PUT with JSON body */
api.put = (path, body) =>
  api(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

/** Shorthand for DELETE */
api.del = (path) => api(path, { method: 'DELETE' });

/** POST with FormData (file uploads) — no Content-Type header so browser sets boundary */
api.upload = (path, formData) => {
  const token = sessionStorage.getItem('trakr_token') || '';
  const headers = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return fetch(`${API_BASE}${path}`, { method: 'POST', headers, body: formData }).then(async (res) => {
    const data = await res.json();
    if (!res.ok) {
      const err = new Error(data.error || 'Upload failed');
      err.data = data;
      throw err;
    }
    return data;
  });
};
