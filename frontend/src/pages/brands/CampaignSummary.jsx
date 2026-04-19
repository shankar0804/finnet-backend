import { useState } from 'react';
import { api } from '../../utils/api';
import { formatMoney, formatNumber, formatDate } from '../../utils/formatters';

/** Level 2: Campaign summary dashboard with entry management */
export default function CampaignSummary({ brand, campaign, metrics, onViewEntries, onRefreshEntries }) {
  const [showAddEntry, setShowAddEntry] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [entryError, setEntryError] = useState('');
  const [entrySuccess, setEntrySuccess] = useState('');
  const [importError, setImportError] = useState('');
  const [importSuccess, setImportSuccess] = useState('');
  const [importing, setImporting] = useState(false);

  // ─── Add Single Entry ───
  const addEntry = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const creator_username = fd.get('creator_username')?.trim();
    if (!creator_username) { setEntryError('Creator username is required.'); return; }
    setEntryError('');
    setEntrySuccess('');
    try {
      await api.post('/api/entries', {
        campaign_id: campaign.id,
        creator_username,
        deliverable_type: fd.get('deliverable_type') || 'Reel',
        content_link: fd.get('content_link')?.trim() || '',
        amount: parseFloat(fd.get('amount')) || 0,
        delivery_date: fd.get('delivery_date') || null,
        poc: fd.get('poc')?.trim() || '',
        notes: fd.get('notes')?.trim() || '',
      });
      setEntrySuccess(`✅ @${creator_username} added!`);
      e.target.reset();
      if (onRefreshEntries) onRefreshEntries();
    } catch (err) {
      setEntryError(err.message);
    }
  };

  // ─── Google Sheets Import ───
  const importSheet = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const sheet_url = fd.get('sheet_url')?.trim();
    if (!sheet_url) { setImportError('Paste a Google Sheet URL.'); return; }
    setImporting(true);
    setImportError('');
    setImportSuccess('');
    try {
      const result = await api.post(`/api/campaigns/${campaign.id}/import-sheet`, { sheet_url });
      setImportSuccess(`✅ Imported ${result.imported} entries from the sheet!`);
      e.target.reset();
      if (onRefreshEntries) onRefreshEntries();
    } catch (err) {
      setImportError(err.message);
    } finally {
      setImporting(false);
    }
  };

  const platformsList = (campaign.platforms || '').split(',').filter(Boolean).map(p => p.trim()).join(', ');

  return (
    <>
      <header className="page-header">
        <div className="page-header-row">
          <div>
            <h1>{campaign.campaign_name}</h1>
            <p>{brand.brand_name} · {platformsList} · {formatDate(campaign.start_date)} — {formatDate(campaign.end_date)}</p>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="btn btn-ghost btn-sm" onClick={() => { setShowAddEntry(!showAddEntry); setShowImport(false); }}>
              {showAddEntry ? 'Cancel' : '+ Add Entry'}
            </button>
            <button className="btn btn-ghost btn-sm" onClick={() => { setShowImport(!showImport); setShowAddEntry(false); }}>
              {showImport ? 'Cancel' : '📊 Import Sheet'}
            </button>
            <button className="btn btn-primary" onClick={onViewEntries}>View All Entries →</button>
          </div>
        </div>
      </header>

      {/* ─── Add Single Entry Form ─── */}
      {showAddEntry && (
        <div className="card" style={{ marginBottom: 16 }}>
          <h3 style={{ marginBottom: 12 }}>Add Creator Entry</h3>
          <form onSubmit={addEntry}>
            <div className="form-row" style={{ marginBottom: 8 }}>
              <div className="form-group" style={{ flex: 1.2 }}>
                <label className="form-label">Creator Username *</label>
                <input className="input" name="creator_username" placeholder="e.g. virat.kohli" />
              </div>
              <div className="form-group" style={{ flex: 0.6 }}>
                <label className="form-label">Deliverable</label>
                <select className="select" name="deliverable_type">
                  <option>Reel</option>
                  <option>Story</option>
                  <option>Post</option>
                  <option>Video</option>
                  <option>Other</option>
                </select>
              </div>
              <div className="form-group" style={{ flex: 0.5 }}>
                <label className="form-label">Amount (₹)</label>
                <input className="input" name="amount" type="number" placeholder="0" />
              </div>
              <div className="form-group" style={{ flex: 0.5 }}>
                <label className="form-label">Delivery Date</label>
                <input className="input" name="delivery_date" type="date" />
              </div>
            </div>
            <div className="form-row" style={{ marginBottom: 10 }}>
              <div className="form-group" style={{ flex: 1 }}>
                <label className="form-label">Content Link</label>
                <input className="input" name="content_link" placeholder="https://instagram.com/p/..." />
              </div>
              <div className="form-group" style={{ flex: 0.6 }}>
                <label className="form-label">PoC</label>
                <input className="input" name="poc" placeholder="Assigned to" />
              </div>
              <div className="form-group" style={{ flex: 0.8 }}>
                <label className="form-label">Notes</label>
                <input className="input" name="notes" placeholder="Optional" />
              </div>
            </div>
            <button className="btn btn-primary btn-sm" type="submit">Add Entry</button>
            {entryError && <div className="error-box" style={{ marginTop: 8 }}>{entryError}</div>}
            {entrySuccess && <div style={{ marginTop: 8, padding: '8px 14px', background: 'var(--success-light)', border: '1px solid rgba(34,197,94,0.2)', borderRadius: 'var(--radius-sm)', color: 'var(--success)', fontSize: '0.82rem' }}>{entrySuccess}</div>}
          </form>
        </div>
      )}

      {/* ─── Google Sheet Import Form ─── */}
      {showImport && (
        <div className="card" style={{ marginBottom: 16 }}>
          <h3 style={{ marginBottom: 6 }}>Import from Google Sheets</h3>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.78rem', marginBottom: 12 }}>
            Paste a Google Sheet URL with columns: <code>username</code>, <code>deliverable</code>, <code>amount</code>, <code>date</code>, <code>link</code>, <code>poc</code>, <code>notes</code>.
            The sheet must be shared as <strong>"Anyone with the link"</strong>.
          </p>
          <form onSubmit={importSheet}>
            <div className="form-row" style={{ marginBottom: 8 }}>
              <div className="form-group" style={{ flex: 1 }}>
                <input className="input" name="sheet_url" placeholder="https://docs.google.com/spreadsheets/d/..." style={{ fontFamily: 'monospace', fontSize: '0.8rem' }} />
              </div>
              <button className="btn btn-primary btn-sm" type="submit" disabled={importing} style={{ alignSelf: 'flex-end' }}>
                {importing ? 'Importing…' : '📥 Import'}
              </button>
            </div>
            {importError && <div className="error-box" style={{ marginTop: 8 }}>{importError}</div>}
            {importSuccess && <div style={{ marginTop: 8, padding: '8px 14px', background: 'var(--success-light)', border: '1px solid rgba(34,197,94,0.2)', borderRadius: 'var(--radius-sm)', color: 'var(--success)', fontSize: '0.82rem' }}>{importSuccess}</div>}
          </form>
        </div>
      )}

      {/* Brand + PoC info */}
      <div className="card" style={{ padding: '14px 18px' }}>
        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', alignItems: 'center' }}>
          <div>
            <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Brand PoC</span>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 4 }}>
              {(brand.brand_poc || []).filter(Boolean).map((p, i) => (
                <span key={i} className="brand-poc-chip">{p}</span>
              ))}
            </div>
          </div>
          <div>
            <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Finnet PoC</span>
            <div style={{ marginTop: 4 }}>
              <span className="brand-poc-chip finnet">{brand.finnet_poc}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Content Summary */}
      {metrics && (
        <div className="card">
          <h3>Content Summary</h3>
          <div className="metrics-grid">
            <MetricCard label="Total Entries" value={metrics.entryCount || 0} />
            <MetricCard label="Live Content" value={metrics.liveCount || 0} accent />
            <MetricCard label="Total Spend" value={formatMoney(metrics.totalSpend)} />
            <MetricCard label="Budget" value={formatMoney(campaign.budget)} />
          </div>
        </div>
      )}
    </>
  );
}

function MetricCard({ label, value, accent }) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className="metric-val" style={accent ? { color: 'var(--accent-hover)' } : undefined}>{value}</div>
    </div>
  );
}
