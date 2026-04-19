import { useState, useEffect, useCallback } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || '';

export default function WhatsAppPage() {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/whatsapp/status`);
      const data = await res.json();
      setStatus(data);
    } catch {
      setStatus({ state: 'offline', qr: null, phone: null });
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 10000); // Poll every 10s
    return () => clearInterval(interval);
  }, [fetchStatus]);

  const stateColor = {
    open: 'var(--success)',
    connecting: 'var(--warning)',
    offline: 'var(--text-muted)',
    close: 'var(--danger)',
  };

  return (
    <div className="page-container">
      <header className="page-header">
        <h1>WhatsApp Bot</h1>
        <p>Monitor the FinBot WhatsApp integration status.</p>
      </header>

      <div className="card">
        {loading && !status ? (
          <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>Checking status…</div>
        ) : (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
              <div style={{
                width: 10, height: 10, borderRadius: '50%',
                background: stateColor[status?.state] || stateColor.offline,
                boxShadow: status?.state === 'open' ? '0 0 8px var(--success)' : 'none',
              }} />
              <span style={{ fontSize: '1.1rem', fontWeight: 600, textTransform: 'capitalize' }}>
                {status?.state || 'offline'}
              </span>
              {status?.phone && (
                <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                  — {status.phone}
                </span>
              )}
            </div>

            <div className="metrics-grid" style={{ marginBottom: 20 }}>
              <div className="metric-card">
                <div className="metric-label">Status</div>
                <div className="metric-val" style={{ fontSize: '1rem', color: stateColor[status?.state] || stateColor.offline }}>
                  {status?.state === 'open' ? '🟢 Connected' : status?.state === 'connecting' ? '🟡 Connecting' : '⚫ Offline'}
                </div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Phone</div>
                <div className="metric-val" style={{ fontSize: '1rem' }}>{status?.phone || '—'}</div>
              </div>
            </div>

            {/* QR Code for scanning */}
            {status?.qrBase64 && status?.state !== 'open' && (
              <div style={{ textAlign: 'center', padding: 24 }}>
                <p style={{ color: 'var(--text-secondary)', marginBottom: 12, fontSize: '0.9rem' }}>Scan this QR code with WhatsApp to connect:</p>
                <img
                  src={status.qrBase64}
                  alt="WhatsApp QR"
                  style={{ width: 240, height: 240, borderRadius: 12, border: '1px solid var(--border)' }}
                />
              </div>
            )}

            <div style={{ textAlign: 'right' }}>
              <button className="btn btn-ghost btn-sm" onClick={fetchStatus}>↻ Refresh</button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
