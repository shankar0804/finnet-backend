import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { api } from '../utils/api';
import { formatNumber, formatMoney, formatDate } from '../utils/formatters';

/**
 * Brand Portal — View-only 4-level drill-down for brand users.
 * Route: /brand-portal/:hash
 * Same hierarchy as internal Brand Management, minus any create/edit/delete actions.
 */
export default function BrandPortalPage() {
  const { hash } = useParams();
  const [brand, setBrand] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [level, setLevel] = useState(1);
  const [selectedCampaign, setSelectedCampaign] = useState(null);
  const [selectedEntry, setSelectedEntry] = useState(null);

  useEffect(() => {
    if (!hash) { setLoading(false); return; }
    api.get(`/api/brand-portal/${hash}`)
      .then(setBrand)
      .catch((err) => setError(err.message || 'Brand not found'))
      .finally(() => setLoading(false));
  }, [hash]);

  const goBack = (toLevel) => {
    setLevel(toLevel);
    if (toLevel <= 2) setSelectedEntry(null);
    if (toLevel <= 1) setSelectedCampaign(null);
  };

  if (loading) {
    return <div className="page-container" style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>Loading brand portal…</div>;
  }

  if (error || !brand) {
    return (
      <div className="page-container">
        <div className="card" style={{ textAlign: 'center', padding: '60px 24px' }}>
          <div style={{ fontSize: '3rem', marginBottom: 16 }}>🔒</div>
          <h3 style={{ marginBottom: 8 }}>Access Denied</h3>
          <p style={{ color: 'var(--text-muted)', maxWidth: 400, margin: '0 auto' }}>
            {error || 'This brand portal link is invalid or you do not have access.'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="page-container">
      {/* Breadcrumb */}
      <div className="breadcrumb">
        <button className={`breadcrumb-item${level === 1 ? ' active' : ''}`} onClick={() => goBack(1)}>
          {brand.brand_name}
        </button>
        {level >= 2 && selectedCampaign && (
          <>
            <span className="breadcrumb-sep">›</span>
            <button className={`breadcrumb-item${level === 2 ? ' active' : ''}`} onClick={() => goBack(2)}>
              {selectedCampaign.campaign_name}
            </button>
          </>
        )}
        {level === 3 && selectedEntry && (
          <>
            <span className="breadcrumb-sep">›</span>
            <span className="breadcrumb-item active">@{selectedEntry.creator_username}</span>
          </>
        )}
      </div>

      {level === 1 && <BrandOverview brand={brand} onSelectCampaign={(c) => { setSelectedCampaign(c); setLevel(2); }} />}
      {level === 2 && selectedCampaign && <CampaignView campaign={selectedCampaign} onSelectEntry={(e) => { setSelectedEntry(e); setLevel(3); }} />}
      {level === 3 && selectedEntry && <EntryView entry={selectedEntry} />}
    </div>
  );
}

/* ─── Level 1: Brand overview with campaign list ─── */
function BrandOverview({ brand, onSelectCampaign }) {
  return (
    <>
      <header className="page-header">
        <h1>{brand.brand_name}</h1>
        <p>Your dedicated brand dashboard</p>
      </header>

      {/* PoC Info */}
      <div className="card" style={{ padding: '14px 18px', marginBottom: 20 }}>
        <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
          <div>
            <span className="form-label">Brand Contacts</span>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 4 }}>
              {[brand.brand_poc_1, brand.brand_poc_2, brand.brand_poc_3].filter(Boolean).map((p, i) => (
                <span key={i} className="brand-poc-chip">{p}</span>
              ))}
            </div>
          </div>
          {brand.finnet_poc && (
            <div>
              <span className="form-label">Finnet PoC</span>
              <div style={{ marginTop: 4 }}>
                <span className="brand-poc-chip finnet">{brand.finnet_poc}</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Campaigns */}
      <div className="card">
        <h3>Campaigns</h3>
        {(!brand.campaigns || brand.campaigns.length === 0) ? (
          <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>No campaigns yet.</p>
        ) : (
          <div className="campaign-chips" style={{ flexDirection: 'column' }}>
            {brand.campaigns.map((c) => (
              <button key={c.id} className="campaign-chip" style={{ width: '100%', justifyContent: 'space-between' }} onClick={() => onSelectCampaign(c)}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span className={`chip-dot ${c.status}`} />
                  <span className="chip-name">{c.campaign_name}</span>
                </div>
                <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
                  {(c.platforms || '').split(',').filter(Boolean).map((p, i) => (
                    <span key={i} className="badge badge-muted">{p.trim()}</span>
                  ))}
                  <span className="chip-date">{formatDate(c.start_date)}</span>
                  {c.budget > 0 && <span className="chip-budget">{formatMoney(c.budget)}</span>}
                  <span className="badge badge-muted">{(c.entries || []).length} entries</span>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    </>
  );
}

/* ─── Level 2: Campaign with entries table ─── */
function CampaignView({ campaign, onSelectEntry }) {
  const entries = campaign.entries || [];
  const liveCount = entries.filter(e => e.content_link).length;

  return (
    <>
      <header className="page-header">
        <h1>{campaign.campaign_name}</h1>
        <p>{campaign.platform} · {formatDate(campaign.start_date)} — {formatDate(campaign.end_date)}</p>
      </header>

      {/* Summary metrics */}
      <div className="card">
        <h3>Summary</h3>
        <div className="metrics-grid">
          <div className="metric-card">
            <div className="metric-label">Total Creators</div>
            <div className="metric-val">{entries.length}</div>
          </div>
          <div className="metric-card">
            <div className="metric-label">Live Content</div>
            <div className="metric-val" style={{ color: 'var(--accent-hover)' }}>{liveCount}</div>
          </div>
          {campaign.budget > 0 && (
            <div className="metric-card">
              <div className="metric-label">Budget</div>
              <div className="metric-val">{formatMoney(campaign.budget)}</div>
            </div>
          )}
        </div>
      </div>

      {/* Entries table */}
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div className="platform-header">
          <span className="platform-icon">📋</span>
          <h3 style={{ margin: 0 }}>Creator Entries</h3>
          <span className="badge badge-muted">{entries.length}</span>
        </div>
        {entries.length === 0 ? (
          <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-muted)' }}>No entries yet.</div>
        ) : (
          <div className="table-wrap" style={{ maxHeight: 'none' }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Sl</th><th>Creator</th><th>Deliverable</th><th>Status</th>
                  <th>Content Link</th><th>Amount</th><th>Delivery Date</th><th>PoC</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((e, i) => {
                  const statusCls = e.status === 'delivered' || e.status === 'approved' ? 'badge-success' : e.status === 'in_progress' ? 'badge-accent' : 'badge-muted';
                  return (
                    <tr key={e.id} className="entry-row" onClick={() => onSelectEntry(e)}>
                      <td>{i + 1}</td>
                      <td style={{ fontWeight: 600, color: 'var(--accent-hover)' }}>@{e.creator_username}</td>
                      <td><span className="badge badge-muted">{e.deliverable_type}</span></td>
                      <td><span className={`badge ${statusCls}`}>{e.status}</span></td>
                      <td>{e.content_link ? <a href={e.content_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>View ↗</a> : <span style={{ color: 'var(--text-muted)' }}>Pending</span>}</td>
                      <td>{e.amount > 0 ? formatMoney(e.amount) : '—'}</td>
                      <td style={{ fontSize: '0.8rem' }}>{formatDate(e.delivery_date)}</td>
                      <td style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{e.poc || '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  );
}

/* ─── Level 3: Entry detail (view-only) ─── */
function EntryView({ entry }) {
  return (
    <>
      <header className="page-header">
        <h1>@{entry.creator_username}</h1>
        <p>{entry.deliverable_type} · {entry.status}</p>
      </header>

      <div className="card">
        <h3>Entry Details</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16 }}>
          <Field label="Creator" value={`@${entry.creator_username}`} />
          <Field label="Deliverable" value={entry.deliverable_type} />
          <Field label="Status" value={entry.status} />
          <Field label="Amount" value={entry.amount > 0 ? formatMoney(entry.amount) : '—'} />
          <Field label="Delivery Date" value={formatDate(entry.delivery_date)} />
          <Field label="PoC" value={entry.poc} />
        </div>
      </div>

      {entry.content_link && (
        <div className="card">
          <h3>Content</h3>
          <a href={entry.content_link} target="_blank" rel="noreferrer" style={{ fontSize: '0.85rem', wordBreak: 'break-all' }}>{entry.content_link}</a>
        </div>
      )}

      {entry.notes && (
        <div className="card">
          <h3>Notes</h3>
          <p className="detail-caption">{entry.notes}</p>
        </div>
      )}
    </>
  );
}

function Field({ label, value }) {
  return (
    <div>
      <div className="form-label">{label}</div>
      <span style={{ fontSize: '0.85rem', color: value ? 'var(--text-primary)' : 'var(--text-muted)' }}>{value || '—'}</span>
    </div>
  );
}
