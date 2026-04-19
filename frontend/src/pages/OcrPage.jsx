import { useState, useRef, useCallback } from 'react';
import { api } from '../utils/api';

const API_BASE = import.meta.env.VITE_API_BASE || '';

export default function OcrPage() {
  const [files, setFiles] = useState([]);
  const [previews, setPreviews] = useState([]);
  const [igLink, setIgLink] = useState('');
  const [processing, setProcessing] = useState(false);
  const [progress, setProgress] = useState('');
  const [results, setResults] = useState(null);
  const [error, setError] = useState('');
  const fileInputRef = useRef(null);
  const dropRef = useRef(null);

  const addFiles = useCallback((newFiles) => {
    const images = Array.from(newFiles).filter((f) => f.type.startsWith('image/'));
    if (!images.length) { setError('Please upload image files.'); return; }
    setError('');
    setResults(null);

    const updated = [...files, ...images];
    setFiles(updated);

    // Generate previews
    images.forEach((file) => {
      const reader = new FileReader();
      reader.onload = (e) => setPreviews((prev) => [...prev, { name: file.name, src: e.target.result, status: 'pending' }]);
      reader.readAsDataURL(file);
    });
  }, [files]);

  const clearFiles = () => {
    setFiles([]);
    setPreviews([]);
    setResults(null);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const handleExtract = async () => {
    if (!files.length || !igLink.trim()) return;
    setProcessing(true);
    setError('');
    setResults(null);

    const target = igLink.trim();
    let merged = {};
    let successCount = 0;

    for (let i = 0; i < files.length; i++) {
      setProgress(`Processing screenshot ${i + 1} of ${files.length}…`);

      // Update thumbnail status
      setPreviews((prev) => prev.map((p, idx) => idx === i ? { ...p, status: 'processing' } : p));

      const formData = new FormData();
      formData.append('image', files[i]);
      formData.append('target_username', target);

      try {
        const token = sessionStorage.getItem('trakr_token') || '';
        const headers = {};
        if (token) headers['Authorization'] = `Bearer ${token}`;

        const res = await fetch(`${API_BASE}/api/upload`, { method: 'POST', headers, body: formData });
        const data = await res.json();

        if (res.ok && data.result) {
          successCount++;
          Object.entries(data.result).forEach(([k, v]) => {
            if (v && v !== '-' && v !== 'N/A' && v !== '') merged[k] = v;
          });
          setPreviews((prev) => prev.map((p, idx) => idx === i ? { ...p, status: 'success' } : p));
        } else {
          setPreviews((prev) => prev.map((p, idx) => idx === i ? { ...p, status: 'error' } : p));
        }
      } catch {
        setPreviews((prev) => prev.map((p, idx) => idx === i ? { ...p, status: 'error' } : p));
      }
    }

    setProcessing(false);
    setProgress('');

    if (successCount === 0) { setError('All screenshots failed to process.'); return; }
    setResults(merged);
  };

  const canExtract = files.length > 0 && igLink.trim().length > 0 && !processing;

  return (
    <div className="page-container">
      <header className="page-header">
        <h1>Analytics OCR Extraction</h1>
        <p>Upload backend dashboard screenshots to automatically structure performance metrics like AVD and Skip Rate.</p>
      </header>

      <div className="card">
        {/* Target username */}
        <div className="form-group" style={{ marginBottom: 16 }}>
          <label className="form-label">Target Instagram Link (Mandatory)</label>
          <input className="input" placeholder="https://instagram.com/username" value={igLink} onChange={(e) => setIgLink(e.target.value)} />
        </div>

        {/* Drop Zone */}
        {files.length === 0 ? (
          <div
            ref={dropRef}
            className="drop-zone"
            onClick={() => fileInputRef.current?.click()}
            onDragOver={(e) => { e.preventDefault(); dropRef.current?.classList.add('dragover'); }}
            onDragLeave={() => dropRef.current?.classList.remove('dragover')}
            onDrop={(e) => { e.preventDefault(); dropRef.current?.classList.remove('dragover'); addFiles(e.dataTransfer.files); }}
          >
            <input ref={fileInputRef} type="file" accept="image/*" multiple hidden onChange={(e) => addFiles(e.target.files)} />
            <svg className="drop-zone-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="17 8 12 3 7 8" /><line x1="12" y1="3" x2="12" y2="15" />
            </svg>
            <h3>Drag & Drop Screenshots</h3>
            <p>Upload multiple screenshots at once, or click to browse</p>
          </div>
        ) : (
          <div style={{ position: 'relative' }}>
            {/* Thumbnails */}
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', padding: 12 }}>
              {previews.map((p, i) => (
                <img
                  key={i}
                  src={p.src}
                  alt={p.name}
                  style={{
                    width: 72, height: 72, objectFit: 'cover', borderRadius: 8,
                    border: `2px solid ${p.status === 'success' ? 'var(--success)' : p.status === 'error' ? 'var(--danger)' : p.status === 'processing' ? 'var(--accent)' : 'var(--border)'}`,
                  }}
                />
              ))}
            </div>
            <p style={{ textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.82rem' }}>
              {files.length} screenshot{files.length > 1 ? 's' : ''} selected
            </p>
            <div style={{ textAlign: 'center', margin: '8px 0' }}>
              <button className="btn btn-ghost btn-sm" onClick={clearFiles}>✕ Clear all</button>
            </div>

            {processing && (
              <div style={{ textAlign: 'center', padding: 16 }}>
                <div className="spinner" style={{ margin: '0 auto 8px' }} />
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{progress}</p>
              </div>
            )}
          </div>
        )}

        {/* Actions */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12, marginTop: 16 }}>
          {error && <span style={{ color: 'var(--danger)', fontSize: '0.85rem', alignSelf: 'center' }}>{error}</span>}
          <button className="btn btn-primary" onClick={handleExtract} disabled={!canExtract}>
            {processing ? 'Processing…' : 'Extract Metrics'}
          </button>
        </div>
      </div>

      {/* Results */}
      {results && (
        <div className="card">
          <h3>Extraction Results</h3>
          <div className="metrics-grid">
            <MetricCard label="Engaged Views" value={results.engaged_views} />
            <MetricCard label="Unique Viewers" value={results.unique_viewers} />
            <MetricCard label="Watch Time" value={results.watch_time_hours || results.watch_time} />
            <MetricCard label="Avg Duration" value={results.average_view_duration} />
          </div>
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value }) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className="metric-val">{value || '—'}</div>
    </div>
  );
}
