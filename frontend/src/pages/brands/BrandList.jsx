import { useState } from 'react';
import { api } from '../../utils/api';
import { formatMoney } from '../../utils/formatters';

/** Level 1: Brand cards with campaigns grouped by Fiscal Year */
export default function BrandList({ brands, onSelectCampaign, onRefresh }) {
  const [showOnboard, setShowOnboard] = useState(false);
  const [formError, setFormError] = useState('');
  const [formSuccess, setFormSuccess] = useState('');
  const [addCampaignFor, setAddCampaignFor] = useState(null);
  const [campaignError, setCampaignError] = useState('');

  // ─── Onboard Brand ───
  const onboardBrand = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const brand_name = fd.get('brand_name')?.trim();
    const brand_username = fd.get('brand_username')?.trim().toLowerCase();
    const password = fd.get('password')?.trim();
    const brand_poc_1 = fd.get('brand_poc_1')?.trim();
    const brand_poc_2 = fd.get('brand_poc_2')?.trim();
    const brand_poc_3 = fd.get('brand_poc_3')?.trim();
    const finnet_poc = fd.get('finnet_poc')?.trim();
    const notes = fd.get('notes')?.trim();
    setFormError('');
    setFormSuccess('');

    if (!brand_name) { setFormError('Brand name is required.'); return; }
    if (!brand_username) { setFormError('Username is required.'); return; }
    if (/[^a-z0-9._-]/.test(brand_username)) { setFormError('Username can only contain lowercase letters, numbers, dots, hyphens.'); return; }
    if (!password || password.length < 6) { setFormError('Password must be at least 6 characters.'); return; }
    if (!finnet_poc) { setFormError('Finnet PoC email is required.'); return; }

    try {
      const result = await api.post('/api/partnerships', {
        brand_name,
        brand_username,
        password,
        brand_poc_1: brand_poc_1 || '',
        brand_poc_2: brand_poc_2 || '',
        brand_poc_3: brand_poc_3 || '',
        finnet_poc,
        notes: notes || '',
      });
      const loginEmail = result.brand_login_email || `${brand_username}@finnetmedia.com`;
      setFormSuccess(`✅ "${brand_name}" onboarded! Login: ${loginEmail}`);
      e.target.reset();
      if (onRefresh) onRefresh();
    } catch (err) { setFormError(err.message); }
  };

  // ─── Add Campaign ───
  const addCampaign = async (e, brandId) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const campaign_name = fd.get('campaign_name')?.trim();
    const selectedPlatforms = fd.getAll('platforms'); // checkboxes
    const start_date = fd.get('start_date') || null;
    const end_date = fd.get('end_date') || null;
    const budget = parseFloat(fd.get('budget')) || 0;
    setCampaignError('');

    if (!campaign_name) { setCampaignError('Campaign name is required.'); return; }
    if (selectedPlatforms.length === 0) { setCampaignError('Select at least one platform.'); return; }

    try {
      await api.post('/api/campaigns', {
        partnership_id: brandId,
        campaign_name,
        platforms: selectedPlatforms.join(','),
        start_date,
        end_date,
        budget,
      });
      e.target.reset();
      setAddCampaignFor(null);
      if (onRefresh) onRefresh();
    } catch (err) { setCampaignError(err.message); }
  };

  return (
    <>
      <header className="page-header">
        <div className="page-header-row">
          <div>
            <h1>Brand Management</h1>
            <p>Manage partnerships, campaigns, and creator deliverables.</p>
          </div>
          <button
            className="btn btn-primary btn-sm"
            onClick={() => { setShowOnboard(!showOnboard); setFormError(''); setFormSuccess(''); }}
          >
            {showOnboard ? 'Cancel' : '+ Onboard Brand'}
          </button>
        </div>
      </header>

      {/* ─── Onboard Brand Form ─── */}
      {showOnboard && (
        <div className="card" style={{ marginBottom: 20 }}>
          <h3 style={{ marginBottom: 14 }}>Onboard Brand</h3>
          <form onSubmit={onboardBrand}>
            {/* Brand Name + Username */}
            <div className="form-row" style={{ marginBottom: 12 }}>
              <div className="form-group" style={{ flex: 1.5 }}>
                <label className="form-label">Brand Name *</label>
                <input className="input" name="brand_name" placeholder="e.g. boAt Lifestyle" />
              </div>
              <div className="form-group" style={{ flex: 1 }}>
                <label className="form-label">Username *</label>
                <div style={{ position: 'relative' }}>
                  <input className="input" name="brand_username" placeholder="e.g. boat" style={{ paddingRight: 145 }} />
                  <span style={{ position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)', fontSize: '0.75rem', color: 'var(--text-muted)', pointerEvents: 'none' }}>@finnetmedia.com</span>
                </div>
              </div>
              <div className="form-group" style={{ flex: 0.7 }}>
                <label className="form-label">Password *</label>
                <input className="input" name="password" type="password" placeholder="Min 6 chars" />
              </div>
            </div>

            {/* Finnet PoC + Brand PoCs */}
            <div className="form-row" style={{ marginBottom: 12 }}>
              <div className="form-group" style={{ flex: 1 }}>
                <label className="form-label">Finnet PoC (Email) *</label>
                <input className="input" name="finnet_poc" type="email" placeholder="team-member@finnetmedia.com" />
              </div>
              <div className="form-group" style={{ flex: 0.8 }}>
                <label className="form-label">Brand PoC 1</label>
                <input className="input" name="brand_poc_1" placeholder="Primary contact name" />
              </div>
              <div className="form-group" style={{ flex: 0.8 }}>
                <label className="form-label">Brand PoC 2</label>
                <input className="input" name="brand_poc_2" placeholder="Optional" />
              </div>
              <div className="form-group" style={{ flex: 0.8 }}>
                <label className="form-label">Brand PoC 3</label>
                <input className="input" name="brand_poc_3" placeholder="Optional" />
              </div>
            </div>

            {/* Notes */}
            <div style={{ marginBottom: 14 }}>
              <label className="form-label">Notes</label>
              <input className="input" name="notes" placeholder="Internal notes (optional)" style={{ marginTop: 5 }} />
            </div>

            <button className="btn btn-primary" type="submit">Onboard Brand</button>

            {formError && <div className="error-box" style={{ marginTop: 10 }}>{formError}</div>}
            {formSuccess && (
              <div style={{ marginTop: 10, padding: '10px 14px', background: 'var(--success-light)', border: '1px solid rgba(34,197,94,0.2)', borderRadius: 'var(--radius-sm)', color: 'var(--success)', fontSize: '0.85rem' }}>
                {formSuccess}
              </div>
            )}
          </form>
        </div>
      )}

      {/* ─── Brand Cards ─── */}
      <div className="brand-list">
        {brands.map((brand) => {
          const byFY = {};
          (brand.campaigns || []).forEach((c) => {
            if (!byFY[c.fy]) byFY[c.fy] = [];
            byFY[c.fy].push(c);
          });
          const fyKeys = Object.keys(byFY).sort().reverse();

          return (
            <div key={brand.id} className="brand-block card">
              <div className="brand-block-header">
                <div>
                  <h2 className="brand-block-name">{brand.brand_name}</h2>
                  <div className="brand-pocs">
                    {(brand.brand_poc || []).filter(Boolean).map((poc, i) => (
                      <span key={i} className="brand-poc-chip">{poc}</span>
                    ))}
                    {brand.finnet_poc && (
                      <span className="brand-poc-chip finnet">Finnet: {brand.finnet_poc}</span>
                    )}
                  </div>
                  {brand.brand_hash && (
                    <div style={{ marginTop: 6 }}>
                      <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Portal: </span>
                      <code style={{ fontSize: '0.68rem', color: 'var(--accent-hover)', background: 'var(--bg-elevated)', padding: '2px 6px', borderRadius: 4 }}>
                        /brand-portal/{brand.brand_hash}
                      </code>
                    </div>
                  )}
                </div>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <StatusPill status={brand.status} />
                  <button
                    className="btn btn-ghost btn-sm"
                    onClick={(e) => { e.stopPropagation(); setAddCampaignFor(addCampaignFor === brand.id ? null : brand.id); setCampaignError(''); }}
                  >
                    + Campaign
                  </button>
                </div>
              </div>

              {/* Add Campaign Form */}
              {addCampaignFor === brand.id && (
                <div className="inline-form" style={{ marginBottom: 14 }}>
                  <form onSubmit={(e) => addCampaign(e, brand.id)}>
                    <div className="form-row" style={{ marginBottom: 8 }}>
                      <div className="form-group" style={{ flex: 2 }}>
                        <label className="form-label">Campaign Name *</label>
                        <input className="input" name="campaign_name" placeholder="e.g. Summer Vibes 2026" />
                      </div>
                      <div className="form-group" style={{ flex: 1.2 }}>
                        <label className="form-label">Platforms *</label>
                        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', paddingTop: 6 }}>
                          {['Instagram', 'YouTube', 'LinkedIn', 'Twitter', 'Other'].map((p) => (
                            <label key={p} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: '0.82rem', cursor: 'pointer', color: 'var(--text-secondary)' }}>
                              <input type="checkbox" name="platforms" value={p} style={{ accentColor: 'var(--accent)' }} />
                              {p}
                            </label>
                          ))}
                        </div>
                      </div>
                    </div>
                    <div className="form-row" style={{ marginBottom: 8 }}>
                      <div className="form-group" style={{ flex: 0.5 }}>
                        <label className="form-label">Start Date</label>
                        <input className="input" name="start_date" type="date" />
                      </div>
                      <div className="form-group" style={{ flex: 0.5 }}>
                        <label className="form-label">End Date</label>
                        <input className="input" name="end_date" type="date" />
                      </div>
                      <div className="form-group" style={{ flex: 0.4 }}>
                        <label className="form-label">Budget (₹)</label>
                        <input className="input" name="budget" type="number" placeholder="0" />
                      </div>
                      <button className="btn btn-primary btn-sm" type="submit" style={{ alignSelf: 'flex-end' }}>Create</button>
                      <button className="btn btn-ghost btn-sm" type="button" style={{ alignSelf: 'flex-end' }} onClick={() => setAddCampaignFor(null)}>Cancel</button>
                    </div>
                    {campaignError && <div className="error-box" style={{ marginTop: 8 }}>{campaignError}</div>}
                  </form>
                </div>
              )}

              {/* Campaign list by FY */}
              {fyKeys.length > 0 ? (
                <div className="fy-groups">
                  {fyKeys.map((fy) => (
                    <div key={fy} className="fy-group">
                      <div className="fy-label">{fy}</div>
                      <div className="campaign-chips">
                        {byFY[fy].map((c) => (
                          <button key={c.id} className="campaign-chip" onClick={() => onSelectCampaign(brand, c)}>
                            <span className={`chip-dot ${c.status}`} />
                            <span className="chip-name">{c.campaign_name}</span>
                            <span className="chip-date">{c.month} {c.year}</span>
                            <span className="chip-budget">{formatMoney(c.budget)}</span>
                          </button>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem', fontStyle: 'italic', paddingTop: 8 }}>
                  No campaigns yet — click "+ Campaign" to add one.
                </div>
              )}
            </div>
          );
        })}
      </div>
    </>
  );
}

function StatusPill({ status }) {
  const cls = status === 'active' ? 'badge-success' : status === 'paused' ? 'badge-warning' : 'badge-accent';
  return <span className={`badge ${cls}`}>{status}</span>;
}
