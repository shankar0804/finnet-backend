import { useState, useEffect, useCallback } from 'react';
import { api } from '../utils/api';
import { formatDate } from '../utils/formatters';
import { useAuth } from '../context/AuthContext';

export default function TeamPage() {
  const { isAdmin, role: currentRole } = useAuth();
  const [users, setUsers] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);
  const [auditFilter, setAuditFilter] = useState('');
  const [loading, setLoading] = useState(true);

  const loadTeam = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.get('/api/users');
      setUsers(data);
    } catch { /* silent */ }
    setLoading(false);
  }, []);

  const loadAudit = useCallback(async () => {
    if (!isAdmin) return;
    try {
      const data = await api.get('/api/audit-logs?limit=200');
      setAuditLogs(data);
    } catch { /* silent */ }
  }, [isAdmin]);

  useEffect(() => { loadTeam(); loadAudit(); }, [loadTeam, loadAudit]);

  const toggleRole = async (email, newRole) => {
    if (!confirm(`Change ${email} to ${newRole}?`)) return;
    try {
      await api.post('/api/users/role', { email, role: newRole });
      loadTeam();
      loadAudit();
    } catch (err) { alert(err.message); }
  };

  const filteredLogs = auditFilter ? auditLogs.filter((l) => l.operation === auditFilter) : auditLogs;

  const OP_COLORS = {
    INSERT: 'var(--success)', UPSERT: 'var(--success)', UPDATE: 'var(--accent)',
    DELETE: 'var(--danger)', LOGIN: '#a78bfa', EXPORT: '#f59e0b', BULK_IMPORT: 'var(--cyan)',
  };

  return (
    <div className="page-container">
      <header className="page-header">
        <h1>Team Management</h1>
        <p>Manage user roles and view audit logs.</p>
      </header>

      {/* Team Members */}
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, flexWrap: 'wrap', gap: 8 }}>
          <h3 style={{ margin: 0 }}>Team Members</h3>
          <div style={{ display: 'flex', gap: 8 }}>
            <span className="badge badge-accent">Admin</span>
            <span className="badge badge-success">Senior</span>
            <span className="badge badge-muted">Junior</span>
          </div>
        </div>


        <div className="table-wrap">
          <table className="table">
            <thead><tr><th>User</th><th>Email</th><th>Role</th><th>Type</th><th>Joined</th><th>Actions</th></tr></thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={6} style={{ textAlign: 'center', padding: 24, color: 'var(--text-muted)' }}>Loading…</td></tr>
              ) : users.length === 0 ? (
                <tr><td colSpan={6} style={{ textAlign: 'center', padding: 24, color: 'var(--text-muted)' }}>No team members yet.</td></tr>
              ) : (
                users.map((u) => {
                  const roleClass = u.role === 'admin' ? 'badge-accent' : u.role === 'senior' ? 'badge-success' : u.role === 'brand' ? 'badge-purple' : 'badge-muted';
                  const authBadge = u.auth_method === 'password' ? 'badge-purple' : 'badge-accent';
                  const isUserAdmin = u.role === 'admin';
                  const isBrand = u.role === 'brand';

                  let action = null;
                  if (!isUserAdmin && !isBrand && isAdmin) {
                    action = u.role === 'senior'
                      ? <button className="btn btn-danger btn-sm" onClick={() => toggleRole(u.email, 'junior')}>Demote</button>
                      : <button className="btn btn-primary btn-sm" onClick={() => toggleRole(u.email, 'senior')}>Promote</button>;
                  }

                  return (
                    <tr key={u.email}>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          {u.picture && <img src={u.picture} alt="" style={{ width: 28, height: 28, borderRadius: '50%', border: '2px solid var(--border)' }} />}
                          <span style={{ fontWeight: 600 }}>{u.name || u.email.split('@')[0]}</span>
                        </div>
                      </td>
                      <td>{u.email}</td>
                      <td><span className={`badge ${roleClass}`}>{u.role}</span></td>
                      <td><span className={`badge ${authBadge}`}>{u.auth_method || 'google'}</span></td>
                      <td style={{ fontSize: '0.8rem' }}>{formatDate(u.created_at)}</td>
                      <td>{action || <span style={{ color: 'var(--text-muted)' }}>—</span>}</td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Audit Log (Admin only) */}
      {isAdmin && (
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, flexWrap: 'wrap', gap: 8 }}>
            <h3 style={{ margin: 0 }}>Audit Log</h3>
            <div style={{ display: 'flex', gap: 8 }}>
              <select className="select" value={auditFilter} onChange={(e) => setAuditFilter(e.target.value)}>
                <option value="">All Operations</option>
                <option value="UPSERT">UPSERT</option><option value="INSERT">INSERT</option>
                <option value="UPDATE">UPDATE</option><option value="DELETE">DELETE</option>
                <option value="LOGIN">LOGIN</option><option value="EXPORT">EXPORT</option>
                <option value="BULK_IMPORT">BULK IMPORT</option>
              </select>
              <button className="btn btn-ghost btn-sm" onClick={loadAudit}>↻</button>
            </div>
          </div>

          <div className="table-wrap" style={{ maxHeight: 400 }}>
            <table className="table">
              <thead><tr><th>Time</th><th>Op</th><th>User</th><th>Table</th><th>Target</th><th>Details</th><th>Source</th><th>IP</th></tr></thead>
              <tbody>
                {filteredLogs.length === 0 ? (
                  <tr><td colSpan={8} style={{ textAlign: 'center', padding: 24, color: 'var(--text-muted)' }}>No logs.</td></tr>
                ) : (
                  filteredLogs.map((log) => {
                    const detailStr = log.details && Object.keys(log.details).length
                      ? Object.entries(log.details).map(([k, v]) => `${k}: ${v}`).join(', ')
                      : '-';
                    const timeStr = log.created_at
                      ? new Date(log.created_at).toLocaleString('en-IN', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false })
                      : '-';
                    return (
                      <tr key={log.id}>
                        <td style={{ fontSize: '0.72rem', whiteSpace: 'nowrap' }}>{timeStr}</td>
                        <td><span style={{ color: OP_COLORS[log.operation] || 'var(--text-muted)', fontWeight: 700, fontSize: '0.72rem', textTransform: 'uppercase' }}>{log.operation}</span></td>
                        <td style={{ fontSize: '0.8rem' }}>{(log.performed_by || 'system').split('@')[0]}</td>
                        <td style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{log.target_table}</td>
                        <td style={{ fontSize: '0.8rem', fontWeight: 600 }}>{log.target_id || '-'}</td>
                        <td style={{ fontSize: '0.72rem', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={detailStr}>{detailStr}</td>
                        <td><SourceBadge source={log.source} /></td>
                        <td style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{log.ip_address || '-'}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function SourceBadge({ source }) {
  if (source === 'whatsapp_bot') return <span style={{ color: 'var(--success)', fontSize: '0.75rem' }}>bot</span>;
  if (source === 'bulk_import') return <span style={{ color: 'var(--cyan)', fontSize: '0.75rem' }}>bulk</span>;
  return <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>dash</span>;
}
