import { formatNumber, formatMoney, formatDate } from '../../utils/formatters';

/** Level 3: Platform-specific entry tables */
export default function EntryTables({ campaign, entries, onSelectEntry }) {
  const platforms = [
    { key: 'instagram', label: 'Instagram', icon: '📸', render: InstagramTable },
    { key: 'youtube',   label: 'YouTube',   icon: '▶️',  render: YouTubeTable },
    { key: 'linkedin',  label: 'LinkedIn',  icon: '💼', render: LinkedInTable },
    { key: 'twitter',   label: 'Twitter',   icon: '🐦', render: TwitterTable },
    { key: 'custom',    label: 'Custom',    icon: '⚙️',  render: CustomTable },
  ];

  const activePlatforms = platforms.filter((p) => entries[p.key]?.length > 0);

  return (
    <>
      <header className="page-header">
        <h1>{campaign.campaign_name} — Entries</h1>
        <p>{activePlatforms.length} platform{activePlatforms.length !== 1 ? 's' : ''} with entries</p>
      </header>

      {activePlatforms.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">📝</div>
          <h3>No entries yet</h3>
          <p>Add creator entries to this campaign to see them here.</p>
        </div>
      ) : (
        activePlatforms.map((plat) => (
          <div key={plat.key} className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div className="platform-header">
              <span className="platform-icon">{plat.icon}</span>
              <h3 style={{ margin: 0 }}>{plat.label}</h3>
              <span className="badge badge-muted">{entries[plat.key].length} entries</span>
            </div>
            <plat.render entries={entries[plat.key]} onSelect={(e) => onSelectEntry(e, plat.key)} />
          </div>
        ))
      )}
    </>
  );
}

function InstagramTable({ entries, onSelect }) {
  return (
    <div className="table-wrap" style={{ maxHeight: 'none' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Sl</th><th>Creator</th><th>Followers</th><th>Deliverable</th>
            <th>Commercials</th><th>Avg Views (Est)</th><th>UTM</th><th>Timestamp</th>
            <th>Live Link</th><th>Views</th><th>Play Count</th><th>Likes</th>
            <th>Comments</th><th>Shares</th><th>Saves</th><th>Eng %</th>
            <th>Duration</th><th>AVD</th><th>Skip Rate</th><th></th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={e.id} className="entry-row" onClick={() => onSelect(e)}>
              <td>{i + 1}</td>
              <td>
                <div className="creator-cell">
                  <a href={e.profile_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>@{e.username}</a>
                </div>
              </td>
              <td>{formatNumber(e.followers)}</td>
              <td><span className="badge badge-muted">{e.deliverable}</span></td>
              <td>{formatMoney(e.commercials)}</td>
              <td>{formatNumber(e.avg_views_est)}</td>
              <td>{e.utm_link ? <a href={e.utm_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>UTM ↗</a> : '—'}</td>
              <td style={{ fontSize: '0.72rem' }}>{formatDate(e.timestamp)}</td>
              <td>{e.live_link ? <a href={e.live_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>View ↗</a> : <span style={{ color: 'var(--text-muted)' }}>Pending</span>}</td>
              <td style={{ fontWeight: 600 }}>{formatNumber(e.video_views)}</td>
              <td>{formatNumber(e.play_count)}</td>
              <td>{formatNumber(e.likes)}</td>
              <td>{formatNumber(e.comments)}</td>
              <td>{formatNumber(e.shares)}</td>
              <td>{formatNumber(e.saves)}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{e.engagement_rate > 0 ? `${e.engagement_rate}%` : '—'}</td>
              <td>{e.duration_secs > 0 ? `${e.duration_secs}s` : '—'}</td>
              <td style={{ color: 'var(--accent-hover)', fontWeight: 700 }}>{e.avd || '—'}</td>
              <td style={{ color: 'var(--accent-hover)', fontWeight: 700 }}>{e.skip_rate || '—'}</td>
              <td><span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Details →</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function YouTubeTable({ entries, onSelect }) {
  return (
    <div className="table-wrap" style={{ maxHeight: 'none' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Sl</th><th>Creator</th><th>Subscribers</th><th>Deliverable</th>
            <th>Commercials</th><th>Avg Views (Est)</th><th>UTM</th><th>Timestamp</th>
            <th>Live Link</th><th>Views</th><th>Impressions</th><th>Likes</th>
            <th>Comments</th><th>Duration</th><th>Eng %</th><th>AVD</th>
            <th>APV</th><th>CTR</th><th></th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={e.id} className="entry-row" onClick={() => onSelect(e)}>
              <td>{i + 1}</td>
              <td><a href={e.profile_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>@{e.username}</a></td>
              <td>{formatNumber(e.subscribers)}</td>
              <td><span className="badge badge-muted">{e.deliverable}</span></td>
              <td>{formatMoney(e.commercials)}</td>
              <td>{formatNumber(e.avg_views_est)}</td>
              <td>{e.utm_link ? <a href={e.utm_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>UTM ↗</a> : '—'}</td>
              <td style={{ fontSize: '0.72rem' }}>{formatDate(e.timestamp)}</td>
              <td>{e.live_link ? <a href={e.live_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>View ↗</a> : '—'}</td>
              <td style={{ fontWeight: 600 }}>{formatNumber(e.video_views)}</td>
              <td>{formatNumber(e.impressions)}</td>
              <td>{formatNumber(e.likes)}</td>
              <td>{formatNumber(e.comments)}</td>
              <td>{e.duration_secs > 0 ? `${Math.floor(e.duration_secs / 60)}m ${e.duration_secs % 60}s` : '—'}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{e.engagement_rate}%</td>
              <td style={{ color: 'var(--accent-hover)', fontWeight: 700 }}>{e.avd || '—'}</td>
              <td>{e.apv || '—'}</td>
              <td>{e.ctr || '—'}</td>
              <td><span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Details →</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LinkedInTable({ entries, onSelect }) {
  return (
    <div className="table-wrap" style={{ maxHeight: 'none' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Sl</th><th>Creator</th><th>Followers</th><th>Deliverable</th>
            <th>Commercials</th><th>UTM</th><th>Timestamp</th><th>Live Link</th>
            <th>Impressions</th><th>Reacts</th><th>Comments</th><th>Re-shares</th>
            <th>Eng %</th><th></th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={e.id} className="entry-row" onClick={() => onSelect(e)}>
              <td>{i + 1}</td>
              <td><a href={e.profile_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>@{e.username}</a></td>
              <td>{formatNumber(e.followers)}</td>
              <td><span className="badge badge-muted">{e.deliverable}</span></td>
              <td>{formatMoney(e.commercials)}</td>
              <td>{e.utm_link ? 'UTM ↗' : '—'}</td>
              <td style={{ fontSize: '0.72rem' }}>{formatDate(e.timestamp)}</td>
              <td>{e.live_link ? <a href={e.live_link} target="_blank" rel="noreferrer">View ↗</a> : '—'}</td>
              <td>{formatNumber(e.impressions)}</td>
              <td>{formatNumber(e.reacts)}</td>
              <td>{formatNumber(e.comments)}</td>
              <td>{formatNumber(e.reshares)}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{e.engagement_rate}%</td>
              <td><span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Details →</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TwitterTable({ entries, onSelect }) {
  return (
    <div className="table-wrap" style={{ maxHeight: 'none' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Sl</th><th>Creator</th><th>Followers</th><th>Deliverable</th>
            <th>Commercials</th><th>UTM</th><th>Timestamp</th><th>Live Link</th>
            <th>Impressions</th><th>Likes</th><th>Comments</th><th>Re-Tweets</th>
            <th>Eng %</th><th></th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={e.id} className="entry-row" onClick={() => onSelect(e)}>
              <td>{i + 1}</td>
              <td><a href={e.profile_link} target="_blank" rel="noreferrer" onClick={(ev) => ev.stopPropagation()}>@{e.username}</a></td>
              <td>{formatNumber(e.followers)}</td>
              <td><span className="badge badge-muted">{e.deliverable}</span></td>
              <td>{formatMoney(e.commercials)}</td>
              <td>{e.utm_link ? 'UTM ↗' : '—'}</td>
              <td style={{ fontSize: '0.72rem' }}>{formatDate(e.timestamp)}</td>
              <td>{e.live_link ? <a href={e.live_link} target="_blank" rel="noreferrer">View ↗</a> : '—'}</td>
              <td>{formatNumber(e.impressions)}</td>
              <td>{formatNumber(e.likes)}</td>
              <td>{formatNumber(e.comments)}</td>
              <td>{formatNumber(e.retweets)}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{e.engagement_rate}%</td>
              <td><span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Details →</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CustomTable({ entries, onSelect }) {
  if (!entries.length) return null;
  const fields = Object.keys(entries[0]).filter(k => !['id', 'profile_link'].includes(k));
  return (
    <div className="table-wrap" style={{ maxHeight: 'none' }}>
      <table className="table">
        <thead><tr>{fields.map(f => <th key={f}>{f}</th>)}<th></th></tr></thead>
        <tbody>
          {entries.map((e) => (
            <tr key={e.id} className="entry-row" onClick={() => onSelect(e)}>
              {fields.map(f => <td key={f}>{e[f] ?? '—'}</td>)}
              <td><span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Details →</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
