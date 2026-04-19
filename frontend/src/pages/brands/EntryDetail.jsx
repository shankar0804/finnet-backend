import { formatNumber, formatMoney, formatDate } from '../../utils/formatters';

/** Level 4: Expanded entry detail — demographics, caption, comments, transcript */
export default function EntryDetail({ entry, platform }) {
  const e = entry;
  const hasDemographics = e.demographics && typeof e.demographics === 'object';

  return (
    <>
      <header className="page-header">
        <h1>@{e.username}</h1>
        <p>
          <a href={e.profile_link} target="_blank" rel="noreferrer">{e.profile_link}</a>
          {' · '}{e.deliverable}{' · '}{formatMoney(e.commercials)}
        </p>
      </header>

      {/* Key Metrics */}
      <div className="card">
        <h3>Performance Metrics</h3>
        <div className="metrics-grid">
          {e.followers != null && <Metric label="Followers" value={formatNumber(e.followers)} />}
          {e.subscribers != null && <Metric label="Subscribers" value={formatNumber(e.subscribers)} />}
          {e.video_views > 0 && <Metric label="Video Views" value={formatNumber(e.video_views)} accent />}
          {e.play_count > 0 && <Metric label="Play Count" value={formatNumber(e.play_count)} />}
          {e.impressions > 0 && <Metric label="Impressions" value={formatNumber(e.impressions)} />}
          {e.likes > 0 && <Metric label="Likes" value={formatNumber(e.likes)} />}
          {e.comments > 0 && <Metric label="Comments" value={formatNumber(e.comments)} />}
          {e.shares > 0 && <Metric label="Shares" value={formatNumber(e.shares)} />}
          {e.saves > 0 && <Metric label="Saves" value={formatNumber(e.saves)} />}
          {e.engagement_rate > 0 && <Metric label="Engagement Rate" value={`${e.engagement_rate}%`} accent />}
          {e.duration_secs > 0 && <Metric label="Duration" value={e.duration_secs > 60 ? `${Math.floor(e.duration_secs / 60)}m ${e.duration_secs % 60}s` : `${e.duration_secs}s`} />}
          {e.avd && <Metric label="AVD" value={e.avd} accent />}
          {e.skip_rate && <Metric label="Skip Rate" value={e.skip_rate} />}
          {e.apv && <Metric label="APV" value={e.apv} />}
          {e.ctr && <Metric label="CTR" value={e.ctr} />}
        </div>
      </div>

      {/* Demographics */}
      {hasDemographics && (
        <div className="card">
          <h3>Audience Demographics</h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            {/* Age */}
            <div>
              <div className="form-label" style={{ marginBottom: 10 }}>Age Distribution</div>
              <div className="demo-bars">
                {[
                  ['13-17', e.demographics.age_13_17],
                  ['18-24', e.demographics.age_18_24],
                  ['25-34', e.demographics.age_25_34],
                  ['35-44', e.demographics.age_35_44],
                  ['45-54', e.demographics.age_45_54],
                ].map(([label, val]) => (
                  <DemoBar key={label} label={label} value={val} />
                ))}
              </div>
            </div>

            {/* Gender + Cities */}
            <div>
              <div className="form-label" style={{ marginBottom: 10 }}>Gender Split</div>
              <div style={{ display: 'flex', gap: 10, marginBottom: 16 }}>
                <div className="demo-pill" style={{ flex: 1 }}>
                  <span className="demo-pill-label">♂ Male</span>
                  <span className="demo-pill-val">{e.demographics.male}</span>
                </div>
                <div className="demo-pill" style={{ flex: 1 }}>
                  <span className="demo-pill-label">♀ Female</span>
                  <span className="demo-pill-val">{e.demographics.female}</span>
                </div>
              </div>

              <div className="form-label" style={{ marginBottom: 10 }}>Top Cities</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(e.demographics.cities || []).map((city, i) => (
                  <span key={i} className="badge badge-muted">{city}</span>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Links & Metadata */}
      <div className="card">
        <h3>Links & Metadata</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 14 }}>
          <DetailField label="UTM Link" value={e.utm_link} link />
          <DetailField label="Live Link" value={e.live_link} link />
          <DetailField label="Timestamp" value={formatDate(e.timestamp)} />
          <DetailField label="Avg Views (Est)" value={formatNumber(e.avg_views_est)} />
        </div>
      </div>

      {/* Caption & Hashtags */}
      {(e.caption || e.hashtags?.length > 0) && (
        <div className="card">
          <h3>📝 Caption & Hashtags</h3>
          {e.caption && (
            <p className="detail-caption">{e.caption}</p>
          )}
          {e.hashtags?.length > 0 && (
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 10 }}>
              {e.hashtags.map((h, i) => (
                <span key={i} className="badge badge-accent">{h}</span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Comments */}
      {e.comments_list?.length > 0 && (
        <div className="card">
          <h3>💬 Latest Comments</h3>
          <div className="comments-list">
            {e.comments_list.map((c, i) => (
              <div key={i} className="comment-item">
                <div className="comment-header">
                  <span className="comment-user">@{c.user}</span>
                  <span className="comment-time">{c.time}</span>
                </div>
                <p className="comment-text">{c.text}</p>
                {c.likes > 0 && <span className="comment-likes">❤️ {c.likes}</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Transcript */}
      {e.transcript && (
        <div className="card">
          <h3>✍️ Video Transcript</h3>
          <p className="detail-transcript">{e.transcript}</p>
        </div>
      )}
    </>
  );
}

function Metric({ label, value, accent }) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className="metric-val" style={accent ? { color: 'var(--accent-hover)' } : undefined}>{value}</div>
    </div>
  );
}

function DemoBar({ label, value }) {
  const pct = parseFloat(value) || 0;
  return (
    <div className="demo-bar-row">
      <span className="demo-bar-label">{label}</span>
      <div className="demo-bar-track">
        <div className="demo-bar-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="demo-bar-val">{value}</span>
    </div>
  );
}

function DetailField({ label, value, link }) {
  return (
    <div>
      <div className="form-label">{label}</div>
      {link && value ? (
        <a href={value} target="_blank" rel="noreferrer" style={{ fontSize: '0.85rem', wordBreak: 'break-all' }}>{value}</a>
      ) : (
        <span style={{ fontSize: '0.85rem', color: value ? 'var(--text-primary)' : 'var(--text-muted)' }}>{value || '—'}</span>
      )}
    </div>
  );
}
