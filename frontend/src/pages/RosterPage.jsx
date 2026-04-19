import { useState, useEffect, useCallback } from 'react';
import { api } from '../utils/api';
import { formatNumber, formatDate, formatDuration, renderVal } from '../utils/formatters';
import { useAuth } from '../context/AuthContext';

const TABS = [
  { key: 'instagram', label: 'Instagram', icon: '📷', cssClass: 'tab-instagram' },
  { key: 'youtube',   label: 'YouTube',   icon: '▶',  cssClass: 'tab-youtube' },
  { key: 'linkedin',  label: 'LinkedIn',  icon: '💼', cssClass: 'tab-linkedin' },
];

export default function RosterPage() {
  const { isPrivileged } = useAuth();
  const [activeTab, setActiveTab] = useState('instagram');

  // ─── Instagram State ───
  const [igRoster, setIgRoster] = useState([]);
  const [igLoading, setIgLoading] = useState(true);
  const [igError, setIgError] = useState('');
  const [igNewUsername, setIgNewUsername] = useState('');
  const [igAdding, setIgAdding] = useState(false);
  const [igMinFollowers, setIgMinFollowers] = useState(0);
  const [igMinViews, setIgMinViews] = useState(0);
  const [igMinEngagement, setIgMinEngagement] = useState(0);

  // ─── YouTube State ───
  const [ytRoster, setYtRoster] = useState([]);
  const [ytLoading, setYtLoading] = useState(true);
  const [ytError, setYtError] = useState('');
  const [ytNewChannel, setYtNewChannel] = useState('');
  const [ytAdding, setYtAdding] = useState(false);
  const [ytMinSubs, setYtMinSubs] = useState(0);

  // ─── LinkedIn State ───
  const [liRoster, setLiRoster] = useState([]);
  const [liLoading, setLiLoading] = useState(true);
  const [liError, setLiError] = useState('');
  const [liNewProfile, setLiNewProfile] = useState('');
  const [liAdding, setLiAdding] = useState(false);

  // ─── Linking State ───
  const [linkModal, setLinkModal] = useState(null);
  const [linkIg, setLinkIg] = useState('');
  const [linkYt, setLinkYt] = useState('');
  const [linkLi, setLinkLi] = useState('');
  const [linkedProfiles, setLinkedProfiles] = useState(null);

  // ─── Data Fetching ───
  const fetchIg = useCallback(async () => {
    setIgLoading(true);
    try { const data = await api.get('/api/roster'); setIgRoster(data); setIgError(''); }
    catch (err) { setIgError(err.message); }
    finally { setIgLoading(false); }
  }, []);
  const fetchYt = useCallback(async () => {
    setYtLoading(true);
    try { const data = await api.get('/api/youtube-roster'); setYtRoster(data); setYtError(''); }
    catch (err) { setYtError(err.message); }
    finally { setYtLoading(false); }
  }, []);
  const fetchLi = useCallback(async () => {
    setLiLoading(true);
    try { const data = await api.get('/api/linkedin-roster'); setLiRoster(data); setLiError(''); }
    catch (err) { setLiError(err.message); }
    finally { setLiLoading(false); }
  }, []);

  useEffect(() => { fetchIg(); fetchYt(); fetchLi(); }, [fetchIg, fetchYt, fetchLi]);

  // ─── Add Handlers ───
  const handleAddIg = async (e) => {
    e.preventDefault();
    if (!igNewUsername.trim()) return;
    setIgAdding(true);
    try {
      await api.post('/api/scrape-instagram', { username: igNewUsername.trim() });
      setIgNewUsername(''); fetchIg();
    } catch (err) {
      if (err.status === 422) alert(`⚠️ Not enough reel data for @${igNewUsername.trim()}\n\n${err.data?.details || err.message}\n\nCreator was NOT added.`);
      else alert(err.message);
    } finally { setIgAdding(false); }
  };
  const handleAddYt = async (e) => {
    e.preventDefault();
    if (!ytNewChannel.trim()) return;
    setYtAdding(true);
    try {
      await api.post('/api/scrape-youtube', { channel: ytNewChannel.trim() });
      setYtNewChannel(''); fetchYt();
    } catch (err) {
      if (err.status === 422) alert(`⚠️ Not enough video data\n\n${err.data?.details || err.message}\n\nChannel was NOT added.`);
      else alert(err.message);
    } finally { setYtAdding(false); }
  };
  const handleAddLi = async (e) => {
    e.preventDefault();
    if (!liNewProfile.trim()) return;
    setLiAdding(true);
    try {
      await api.post('/api/scrape-linkedin', { profile: liNewProfile.trim() });
      setLiNewProfile(''); fetchLi();
    } catch (err) { alert(err.message); }
    finally { setLiAdding(false); }
  };

  // ─── Delete Handlers ───
  const handleDeleteIg = async (username) => {
    if (!confirm(`Delete @${username}?`)) return;
    try { await api.del(`/api/roster/${username}`); fetchIg(); } catch { alert('Failed'); }
  };
  const handleDeleteYt = async (channelId) => {
    if (!confirm(`Delete YT channel ${channelId}?`)) return;
    try { await api.del(`/api/youtube-roster/${channelId}`); fetchYt(); } catch { alert('Failed'); }
  };
  const handleDeleteLi = async (profileId) => {
    if (!confirm(`Delete LI profile ${profileId}?`)) return;
    try { await api.del(`/api/linkedin-roster/${profileId}`); fetchLi(); } catch { alert('Failed'); }
  };

  // ─── Linking ───
  const openLinkModal = async (platform, identifier, groupId) => {
    setLinkModal({ platform, identifier, groupId });
    setLinkIg(''); setLinkYt(''); setLinkLi('');
    if (platform === 'instagram') setLinkIg(identifier);
    else if (platform === 'youtube') setLinkYt(identifier);
    else if (platform === 'linkedin') setLinkLi(identifier);
    if (groupId) {
      try { const data = await api.get(`/api/linked-profiles/${groupId}`); setLinkedProfiles(data); }
      catch { setLinkedProfiles(null); }
    } else { setLinkedProfiles(null); }
  };
  const handleLink = async () => {
    const body = {};
    if (linkIg) body.instagram_username = linkIg;
    if (linkYt) body.youtube_channel_id = linkYt;
    if (linkLi) body.linkedin_profile_id = linkLi;
    if ([linkIg, linkYt, linkLi].filter(Boolean).length < 2) { alert('Select at least 2 platforms.'); return; }
    try {
      await api.post('/api/link-profiles', body);
      alert('✅ Profiles linked!'); setLinkModal(null); fetchIg(); fetchYt(); fetchLi();
    } catch (err) { alert(err.message); }
  };
  const handleUnlink = async (platform, identifier) => {
    if (!confirm(`Unlink this ${platform} profile?`)) return;
    try {
      await api.post('/api/unlink-profile', { platform, identifier });
      setLinkModal(null); fetchIg(); fetchYt(); fetchLi();
    } catch (err) { alert(err.message); }
  };

  // ─── Cross-platform link helpers ───
  const getLinkedYt = (groupId) => {
    if (!groupId) return null;
    return ytRoster.find(r => r.creator_group_id === groupId) || null;
  };
  const getLinkedLi = (groupId) => {
    if (!groupId) return null;
    return liRoster.find(r => r.creator_group_id === groupId) || null;
  };
  const getLinkedIg = (groupId) => {
    if (!groupId) return null;
    return igRoster.find(r => r.creator_group_id === groupId) || null;
  };
  const platformLink = (profile, platform) => {
    if (!profile) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
    const label = platform === 'instagram' ? `@${profile.username}` : platform === 'youtube' ? profile.channel_name : profile.full_name;
    return <a href={profile.profile_link} target="_blank" rel="noreferrer" style={{ fontSize: '0.78rem' }}>{label}</a>;
  };
  const linkBadges = (groupId, currentPlatform) => {
    if (!groupId) return null;
    const badges = [];
    if (currentPlatform !== 'instagram' && igRoster.some(r => r.creator_group_id === groupId)) badges.push(<span key="ig" className="platform-badge badge-ig">📷</span>);
    if (currentPlatform !== 'youtube' && ytRoster.some(r => r.creator_group_id === groupId)) badges.push(<span key="yt" className="platform-badge badge-yt">▶</span>);
    if (currentPlatform !== 'linkedin' && liRoster.some(r => r.creator_group_id === groupId)) badges.push(<span key="li" className="platform-badge badge-li">💼</span>);
    return badges.length > 0 ? <span className="platform-badges">{badges}</span> : null;
  };

  // ─── Filters ───
  const igFiltered = igRoster.filter(r => r.followers >= igMinFollowers && r.avg_views >= igMinViews && r.engagement_rate >= igMinEngagement);
  const ytFiltered = ytRoster.filter(r => r.subscribers >= ytMinSubs);

  // ─── Skeleton ───
  const skeletonRows = (cols, count = 5) => Array.from({ length: count }).map((_, i) => (
    <tr key={i}>{Array.from({ length: cols }).map((_, j) => (
      <td key={j}><div className="skeleton" style={{ width: 50, height: 14 }} /></td>
    ))}</tr>
  ));

  const tabCounts = { instagram: igRoster.length, youtube: ytRoster.length, linkedin: liRoster.length };
  const ts = (v) => v ? formatDate(v) : '—';
  const v = (val) => renderVal(val);

  return (
    <div className="page-container">
      <header className="page-header">
        <h1>Creator Database</h1>
        <p>Manage your creator roster across Instagram, YouTube, and LinkedIn.</p>
      </header>

      {/* ═══ Platform Tabs ═══ */}
      <div className="platform-tabs">
        {TABS.map(tab => (
          <button key={tab.key} className={`platform-tab ${tab.cssClass} ${activeTab === tab.key ? 'active' : ''}`} onClick={() => setActiveTab(tab.key)}>
            <span className="platform-tab-icon">{tab.icon}</span>
            {tab.label}
            <span className="platform-tab-count">{tabCounts[tab.key]}</span>
          </button>
        ))}
      </div>

      {/* ═══════════════ INSTAGRAM TAB ═══════════════ */}
      {activeTab === 'instagram' && (
        <>
          <div className="card">
            <h3>Add Instagram Creator</h3>
            <form className="form-row" onSubmit={handleAddIg}>
              <div className="form-group" style={{ flex: 3 }}>
                <input className="input" placeholder="Instagram username or profile link" value={igNewUsername} onChange={e => setIgNewUsername(e.target.value)} />
              </div>
              <button className="btn btn-primary" type="submit" disabled={igAdding}>{igAdding ? '⏳ Scraping…' : '🔍 Fetch & Add'}</button>
            </form>
          </div>

          <div className="card">
            <div className="filter-bar">
              <div className="form-group" style={{ minWidth: 120 }}>
                <label className="form-label">Min Followers</label>
                <select className="select" value={igMinFollowers} onChange={e => setIgMinFollowers(+e.target.value)}>
                  <option value={0}>Any</option><option value={100000}>&gt; 100K</option><option value={500000}>&gt; 500K</option><option value={1000000}>&gt; 1M</option>
                </select>
              </div>
              <div className="form-group" style={{ minWidth: 120 }}>
                <label className="form-label">Min Avg Views</label>
                <select className="select" value={igMinViews} onChange={e => setIgMinViews(+e.target.value)}>
                  <option value={0}>Any</option><option value={50000}>&gt; 50K</option><option value={250000}>&gt; 250K</option><option value={1000000}>&gt; 1M</option>
                </select>
              </div>
              <div className="form-group" style={{ minWidth: 140 }}>
                <label className="form-label">Min Engagement</label>
                <select className="select" value={igMinEngagement} onChange={e => setIgMinEngagement(+e.target.value)}>
                  <option value={0}>Any</option><option value={2}>&gt; 2%</option><option value={4}>&gt; 4%</option><option value={8}>&gt; 8%</option>
                </select>
              </div>
            </div>
            <div className="table-wrap">
              <table className="table">
                <thead><tr>
                  <th>Username</th><th>Name</th><th>Link</th><th>Platform</th><th>Niche</th><th>Language</th><th>Gender</th><th>Location</th>
                  <th>Followers</th><th>Avg Views</th><th>Eng %</th><th>Vid Len</th>
                  <th>AVD</th><th>Skip Rate</th>
                  <th>13-17</th><th>18-24</th><th>25-34</th><th>35-44</th><th>45-54</th>
                  <th>Male</th><th>Female</th>
                  <th>City 1</th><th>City 2</th><th>City 3</th><th>City 4</th><th>City 5</th>
                  <th>Contact</th><th>Mail</th><th>Managed By</th>
                  <th>Scraped</th><th>OCR</th><th>Manual</th>
                  <th>YT Link</th><th>LI Link</th>
                  <th>Link</th><th>Actions</th>
                </tr></thead>
                <tbody>
                  {igLoading ? skeletonRows(35) : igError ? (
                    <tr><td colSpan={35} className="empty-state">{igError}</td></tr>
                  ) : igFiltered.length === 0 ? (
                    <tr><td colSpan={35} className="empty-state">No creators match filters.</td></tr>
                  ) : igFiltered.map(r => (
                    <tr key={r.username}>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer" style={{ fontWeight: 600, color: '#e1306c' }}>@{r.username}</a></td>
                      <td style={{ fontWeight: 500 }}>{r.creator_name}</td>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer">Open</a></td>
                      <td>Instagram</td>
                      <td>{v(r.niche)}</td><td>{v(r.language)}</td><td>{v(r.gender)}</td><td>{v(r.location)}</td>
                      <td>{formatNumber(r.followers)}</td>
                      <td style={{ fontWeight: 600 }}>{formatNumber(r.avg_views)}</td>
                      <td style={{ color: 'var(--success)', fontWeight: 600 }}>{r.engagement_rate}%</td>
                      <td>{formatDuration(r.avg_video_length)}</td>
                      <td>{v(r.avd)}</td><td>{v(r.skip_rate)}</td>
                      <td>{v(r.age_13_17)}</td><td>{v(r.age_18_24)}</td><td>{v(r.age_25_34)}</td><td>{v(r.age_35_44)}</td><td>{v(r.age_45_54)}</td>
                      <td>{v(r.male_pct)}</td><td>{v(r.female_pct)}</td>
                      <td>{v(r.city_1)}</td><td>{v(r.city_2)}</td><td>{v(r.city_3)}</td><td>{v(r.city_4)}</td><td>{v(r.city_5)}</td>
                      <td>{v(r.contact_numbers)}</td><td>{v(r.mail_id)}</td><td>{v(r.managed_by)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_scraped_at)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_ocr_at)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_manual_at)}</td>
                      <td>{platformLink(getLinkedYt(r.creator_group_id), 'youtube')}</td>
                      <td>{platformLink(getLinkedLi(r.creator_group_id), 'linkedin')}</td>
                      <td>
                        <button className={`btn-link-profile ${r.creator_group_id ? 'linked' : ''}`} onClick={() => openLinkModal('instagram', r.username, r.creator_group_id)}>
                          🔗{linkBadges(r.creator_group_id, 'instagram')}
                        </button>
                      </td>
                      <td>{isPrivileged && <button className="btn btn-danger btn-sm" onClick={() => handleDeleteIg(r.username)}>Delete</button>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {/* ═══════════════ YOUTUBE TAB ═══════════════ */}
      {activeTab === 'youtube' && (
        <>
          <div className="card">
            <h3>Add YouTube Creator</h3>
            <form className="form-row" onSubmit={handleAddYt}>
              <div className="form-group" style={{ flex: 3 }}>
                <input className="input" placeholder="YouTube channel URL or @handle" value={ytNewChannel} onChange={e => setYtNewChannel(e.target.value)} />
              </div>
              <button className="btn btn-primary" type="submit" disabled={ytAdding}>{ytAdding ? '⏳ Scraping…' : '🔍 Fetch & Add'}</button>
            </form>
          </div>

          <div className="card">
            <div className="filter-bar">
              <div className="form-group" style={{ minWidth: 140 }}>
                <label className="form-label">Min Subscribers</label>
                <select className="select" value={ytMinSubs} onChange={e => setYtMinSubs(+e.target.value)}>
                  <option value={0}>Any</option><option value={100000}>&gt; 100K</option><option value={500000}>&gt; 500K</option><option value={1000000}>&gt; 1M</option>
                </select>
              </div>
            </div>
            <div className="table-wrap">
              <table className="table">
                <thead><tr>
                  <th>Channel</th><th>Handle</th><th>Link</th><th>Platform</th><th>Niche</th><th>Language</th><th>Gender</th><th>Location</th>
                  <th>Subs</th><th>Total Vids</th>
                  <th>Long Views</th><th>Long ER%</th><th>Long Dur</th>
                  <th>Short Views</th><th>Short ER%</th><th>Short Dur</th>
                  <th>AVD</th><th>Skip Rate</th>
                  <th>13-17</th><th>18-24</th><th>25-34</th><th>35-44</th><th>45-54</th>
                  <th>Male</th><th>Female</th>
                  <th>City 1</th><th>City 2</th><th>City 3</th><th>City 4</th><th>City 5</th>
                  <th>Contact</th><th>Mail</th><th>Managed By</th>
                  <th>Scraped</th><th>OCR</th><th>Manual</th>
                  <th>IG Link</th><th>LI Link</th>
                  <th>Link</th><th>Actions</th>
                </tr></thead>
                <tbody>
                  {ytLoading ? skeletonRows(39) : ytError ? (
                    <tr><td colSpan={39} className="empty-state">{ytError}</td></tr>
                  ) : ytFiltered.length === 0 ? (
                    <tr><td colSpan={39} className="empty-state">No YouTube creators yet.</td></tr>
                  ) : ytFiltered.map(r => (
                    <tr key={r.channel_id}>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer" style={{ fontWeight: 600, color: '#ff4444' }}>{r.channel_name}</a></td>
                      <td style={{ color: 'var(--text-muted)' }}>@{r.channel_handle}</td>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer">Open</a></td>
                      <td>YouTube</td>
                      <td>{v(r.niche)}</td><td>{v(r.language)}</td><td>{v(r.gender)}</td><td>{v(r.location)}</td>
                      <td style={{ fontWeight: 700 }}>{formatNumber(r.subscribers)}</td>
                      <td>{r.total_videos || '—'}</td>
                      <td style={{ fontWeight: 600 }}>{r.avg_long_views ? formatNumber(r.avg_long_views) : '—'}</td>
                      <td style={{ color: 'var(--success)', fontWeight: 600 }}>{r.long_engagement_rate ? `${r.long_engagement_rate}%` : '—'}</td>
                      <td>{r.avg_long_duration ? formatDuration(r.avg_long_duration) : '—'}</td>
                      <td style={{ fontWeight: 600, color: 'var(--purple)' }}>{r.avg_short_views ? formatNumber(r.avg_short_views) : '—'}</td>
                      <td style={{ color: 'var(--purple)', fontWeight: 600 }}>{r.short_engagement_rate ? `${r.short_engagement_rate}%` : '—'}</td>
                      <td>{r.avg_short_duration ? formatDuration(r.avg_short_duration) : '—'}</td>
                      <td>{v(r.avd)}</td><td>{v(r.skip_rate)}</td>
                      <td>{v(r.age_13_17)}</td><td>{v(r.age_18_24)}</td><td>{v(r.age_25_34)}</td><td>{v(r.age_35_44)}</td><td>{v(r.age_45_54)}</td>
                      <td>{v(r.male_pct)}</td><td>{v(r.female_pct)}</td>
                      <td>{v(r.city_1)}</td><td>{v(r.city_2)}</td><td>{v(r.city_3)}</td><td>{v(r.city_4)}</td><td>{v(r.city_5)}</td>
                      <td>{v(r.contact_numbers)}</td><td>{v(r.mail_id)}</td><td>{v(r.managed_by)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_scraped_at)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_ocr_at)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_manual_at)}</td>
                      <td>{platformLink(getLinkedIg(r.creator_group_id), 'instagram')}</td>
                      <td>{platformLink(getLinkedLi(r.creator_group_id), 'linkedin')}</td>
                      <td>
                        <button className={`btn-link-profile ${r.creator_group_id ? 'linked' : ''}`} onClick={() => openLinkModal('youtube', r.channel_id, r.creator_group_id)}>
                          🔗{linkBadges(r.creator_group_id, 'youtube')}
                        </button>
                      </td>
                      <td>{isPrivileged && <button className="btn btn-danger btn-sm" onClick={() => handleDeleteYt(r.channel_id)}>Delete</button>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {/* ═══════════════ LINKEDIN TAB ═══════════════ */}
      {activeTab === 'linkedin' && (
        <>
          <div className="card">
            <h3>Add LinkedIn Profile</h3>
            <form className="form-row" onSubmit={handleAddLi}>
              <div className="form-group" style={{ flex: 3 }}>
                <input className="input" placeholder="LinkedIn profile URL or username" value={liNewProfile} onChange={e => setLiNewProfile(e.target.value)} />
              </div>
              <button className="btn btn-primary" type="submit" disabled={liAdding}>{liAdding ? '⏳ Scraping…' : '🔍 Fetch & Add'}</button>
            </form>
          </div>

          <div className="card">
            <div className="table-wrap">
              <table className="table">
                <thead><tr>
                  <th>Name</th><th>Profile ID</th><th>Link</th><th>Platform</th><th>Headline</th><th>Summary</th>
                  <th>Company</th><th>Title</th><th>Industry</th>
                  <th>Niche</th><th>Language</th><th>Gender</th><th>Location</th><th>Connections</th>
                  <th>Contact</th><th>Mail</th><th>Managed By</th>
                  <th>Scraped</th><th>Manual</th>
                  <th>IG Link</th><th>YT Link</th>
                  <th>Link</th><th>Actions</th>
                </tr></thead>
                <tbody>
                  {liLoading ? skeletonRows(23) : liError ? (
                    <tr><td colSpan={23} className="empty-state">{liError}</td></tr>
                  ) : liRoster.length === 0 ? (
                    <tr><td colSpan={23} className="empty-state">No LinkedIn profiles yet.</td></tr>
                  ) : liRoster.map(r => (
                    <tr key={r.profile_id}>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer" style={{ fontWeight: 600, color: '#4a9fd5' }}>{r.full_name}</a></td>
                      <td style={{ color: 'var(--text-muted)' }}>{r.profile_id}</td>
                      <td><a href={r.profile_link} target="_blank" rel="noreferrer">Open</a></td>
                      <td>LinkedIn</td>
                      <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={r.headline}>{v(r.headline)}</td>
                      <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={r.summary}>{v(r.summary)}</td>
                      <td>{v(r.current_company)}</td><td>{v(r.current_title)}</td><td>{v(r.industry)}</td>
                      <td>{v(r.niche)}</td><td>{v(r.language)}</td><td>{v(r.gender)}</td><td>{v(r.location)}</td>
                      <td style={{ fontWeight: 600 }}>{formatNumber(r.connections)}</td>
                      <td>{v(r.contact_numbers)}</td><td>{v(r.mail_id)}</td><td>{v(r.managed_by)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_scraped_at)}</td>
                      <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{ts(r.last_manual_at)}</td>
                      <td>{platformLink(getLinkedIg(r.creator_group_id), 'instagram')}</td>
                      <td>{platformLink(getLinkedYt(r.creator_group_id), 'youtube')}</td>
                      <td>
                        <button className={`btn-link-profile ${r.creator_group_id ? 'linked' : ''}`} onClick={() => openLinkModal('linkedin', r.profile_id, r.creator_group_id)}>
                          🔗{linkBadges(r.creator_group_id, 'linkedin')}
                        </button>
                      </td>
                      <td>{isPrivileged && <button className="btn btn-danger btn-sm" onClick={() => handleDeleteLi(r.profile_id)}>Delete</button>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {/* ═══ LINK MODAL ═══ */}
      {linkModal && (
        <div className="link-modal-overlay" onClick={() => setLinkModal(null)}>
          <div className="link-modal" onClick={e => e.stopPropagation()}>
            <div className="link-modal-header">
              <h3>🔗 Link Creator Profiles</h3>
              <button className="link-modal-close" onClick={() => setLinkModal(null)}>✕</button>
            </div>
            <div className="link-modal-body">
              {linkedProfiles && (linkedProfiles.instagram?.length > 0 || linkedProfiles.youtube?.length > 0 || linkedProfiles.linkedin?.length > 0) && (
                <div className="link-modal-linked">
                  <div className="link-modal-linked-title">Currently Linked</div>
                  {linkedProfiles.instagram?.map(p => (
                    <div key={p.username} className="link-modal-linked-item">
                      <div className="link-modal-linked-info"><span className="link-platform-dot dot-ig" /><strong>@{p.username}</strong><span style={{ color: 'var(--text-muted)' }}>— {p.creator_name || 'Instagram'}</span></div>
                      <button className="btn btn-danger btn-sm" onClick={() => handleUnlink('instagram', p.username)}>Unlink</button>
                    </div>
                  ))}
                  {linkedProfiles.youtube?.map(p => (
                    <div key={p.channel_id} className="link-modal-linked-item">
                      <div className="link-modal-linked-info"><span className="link-platform-dot dot-yt" /><strong>{p.channel_name}</strong><span style={{ color: 'var(--text-muted)' }}>— YouTube</span></div>
                      <button className="btn btn-danger btn-sm" onClick={() => handleUnlink('youtube', p.channel_id)}>Unlink</button>
                    </div>
                  ))}
                  {linkedProfiles.linkedin?.map(p => (
                    <div key={p.profile_id} className="link-modal-linked-item">
                      <div className="link-modal-linked-info"><span className="link-platform-dot dot-li" /><strong>{p.full_name}</strong><span style={{ color: 'var(--text-muted)' }}>— {p.headline || 'LinkedIn'}</span></div>
                      <button className="btn btn-danger btn-sm" onClick={() => handleUnlink('linkedin', p.profile_id)}>Unlink</button>
                    </div>
                  ))}
                </div>
              )}
              <div className="link-field">
                <label className="link-field-label"><span className="link-field-icon icon-ig">📷</span>Instagram Username</label>
                <input className="input" placeholder="e.g. virat.kohli" value={linkIg} onChange={e => setLinkIg(e.target.value)} />
              </div>
              <div className="link-field">
                <label className="link-field-label"><span className="link-field-icon icon-yt">▶</span>YouTube Channel ID</label>
                <input className="input" placeholder="e.g. UCxxx or channel ID from YouTube tab" value={linkYt} onChange={e => setLinkYt(e.target.value)} />
              </div>
              <div className="link-field">
                <label className="link-field-label"><span className="link-field-icon icon-li">💼</span>LinkedIn Profile ID</label>
                <input className="input" placeholder="e.g. johndoe (from LinkedIn tab)" value={linkLi} onChange={e => setLinkLi(e.target.value)} />
              </div>
            </div>
            <div className="link-modal-footer">
              <button className="btn btn-primary" onClick={handleLink}>🔗 Link Profiles</button>
              <button className="btn btn-ghost" onClick={() => setLinkModal(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
