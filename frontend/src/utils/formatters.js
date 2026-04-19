/** Shared formatting utilities */

export function formatNumber(num) {
  if (num === '' || num === undefined || num === null) return '-';
  if (typeof num !== 'number') return num;
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

export function formatDate(ts) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: '2-digit' });
  } catch {
    return ts;
  }
}

export function formatDuration(secs) {
  if (!secs || secs === 0) return '-';
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export function formatMoney(n) {
  if (!n) return '—';
  return `₹${Number(n).toLocaleString('en-IN')}`;
}

/** Renders a safe display value — replaces nullish/empty with a dim dash */
export function renderVal(val) {
  return (val === '' || val === null || val === undefined) ? '-' : val;
}
