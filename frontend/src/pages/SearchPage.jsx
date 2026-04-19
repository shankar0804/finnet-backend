import { useState } from 'react';
import { api } from '../utils/api';

export default function SearchPage() {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState(null);
  const [insight, setInsight] = useState('');
  const [error, setError] = useState('');
  const [exporting, setExporting] = useState(false);
  const [sheetUrl, setSheetUrl] = useState('');

  const handleSearch = async (e) => {
    e?.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError('');
    setResults(null);
    setInsight('');
    setSheetUrl('');

    try {
      const data = await api.post('/api/custom-search', { query: query.trim() });
      const ans = data.answer;

      if (ans?.type === 'error') { setError(ans.message); return; }

      if (ans?.type === 'data') {
        if (ans.insight) setInsight(ans.insight);
        setResults(ans.data?.length ? ans.data : []);
      } else {
        setError(typeof ans === 'string' ? ans : JSON.stringify(ans));
      }
    } catch (err) {
      setError(err.message || 'Network error');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    if (!results?.length) return;
    setExporting(true);
    try {
      const data = await api.post('/api/export-to-sheet', {
        data: results,
        title: `TRAKR Export — ${new Date().toLocaleDateString('en-IN')}`,
      });
      setSheetUrl(data.sheet_url);
    } catch (err) {
      alert(err.message || 'Export failed');
    } finally {
      setExporting(false);
    }
  };

  const columns = results?.length ? Object.keys(results[0]) : [];

  return (
    <div className="page-container">
      <header className="page-header">
        <h1>AI Search</h1>
        <p>Query your influencer database using natural language.</p>
      </header>

      {/* Search bar */}
      <div className="card" style={{ padding: 14 }}>
        <form className="form-row" onSubmit={handleSearch}>
          <div className="form-group" style={{ flex: 3 }}>
            <input className="input" placeholder="e.g. Show creators with over 1M followers…" value={query} onChange={(e) => setQuery(e.target.value)} />
          </div>
          <button className="btn btn-primary" type="submit" disabled={loading}>
            {loading ? 'Querying…' : 'Query DB'}
          </button>
        </form>
      </div>

      {/* Loading */}
      {loading && (
        <div style={{ textAlign: 'center', padding: 32 }}>
          <div className="spinner" style={{ margin: '0 auto 12px' }} />
          <span style={{ color: 'var(--text-muted)' }}>Querying database…</span>
        </div>
      )}

      {/* Insight */}
      {insight && (
        <div className="card" style={{ padding: '14px 18px', marginBottom: 16 }}>
          <strong style={{ color: 'var(--accent-hover)' }}>✨ Insight: </strong>
          <span dangerouslySetInnerHTML={{ __html: insight }} />
        </div>
      )}

      {/* Error */}
      {error && <div className="error-box" style={{ marginBottom: 16 }}>{error}</div>}

      {/* Results */}
      {results && (
        results.length === 0 ? (
          <div className="empty-state">
            <div className="empty-state-icon">🔍</div>
            <h3>No records found</h3>
            <p>Try a different query.</p>
          </div>
        ) : (
          <>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12, marginBottom: 12, alignItems: 'center' }}>
              {sheetUrl && (
                <a href={sheetUrl} target="_blank" rel="noreferrer" style={{ fontSize: '0.85rem', fontWeight: 600 }}>
                  📄 Open Google Sheet ↗
                </a>
              )}
              <button className="btn btn-primary btn-sm" onClick={handleExport} disabled={exporting}>
                {exporting ? '⏳ Exporting…' : '📊 Export to Sheets'}
              </button>
            </div>

            <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
                  </thead>
                  <tbody>
                    {results.map((row, i) => (
                      <tr key={i}>
                        {columns.map((c) => <td key={c}>{row[c] ?? '-'}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )
      )}
    </div>
  );
}
