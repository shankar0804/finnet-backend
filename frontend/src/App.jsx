import { useState, useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from './context/AuthContext';
import LoginPage from './pages/LoginPage';
import Layout from './components/Layout';
import RosterPage from './pages/RosterPage';
import OcrPage from './pages/OcrPage';
import SearchPage from './pages/SearchPage';
import BrandsPage from './pages/BrandsPage';
import TeamPage from './pages/TeamPage';
import BrandPortalPage from './pages/BrandPortalPage';
import WhatsAppPage from './pages/WhatsAppPage';
import { api } from './utils/api';

function ProtectedRoute({ children }) {
  const { isAuthenticated } = useAuth();
  return isAuthenticated ? children : <Navigate to="/login" replace />;
}

function RoleRoute({ children, allowed }) {
  const { role } = useAuth();
  return allowed.includes(role) ? children : <Navigate to="/" replace />;
}

export default function App() {
  const { isAuthenticated, isBrand } = useAuth();

  return (
    <Routes>
      <Route path="/login" element={isAuthenticated ? <Navigate to="/" replace /> : <LoginPage />} />

      <Route path="/" element={<ProtectedRoute><Layout /></ProtectedRoute>}>
        {/* Default redirect based on role */}
        <Route index element={isBrand ? <Navigate to="/brand-portal" replace /> : <Navigate to="/roster" replace />} />

        <Route path="roster" element={<RosterPage />} />
        <Route path="ocr" element={<OcrPage />} />
        <Route path="search" element={<SearchPage />} />
        <Route path="brands" element={
          <RoleRoute allowed={['admin', 'senior']}><BrandsPage /></RoleRoute>
        } />
        <Route path="team" element={
          <RoleRoute allowed={['admin', 'senior']}><TeamPage /></RoleRoute>
        } />
        {/* Brand portal — hash-based route for brand-specific view-only data */}
        <Route path="brand-portal" element={
          <RoleRoute allowed={['brand']}><BrandPortalRedirect /></RoleRoute>
        } />
        <Route path="brand-portal/:hash" element={
          <RoleRoute allowed={['brand', 'admin', 'senior']}><BrandPortalPage /></RoleRoute>
        } />
        <Route path="whatsapp" element={
          <RoleRoute allowed={['admin', 'senior']}><WhatsAppPage /></RoleRoute>
        } />
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

/**
 * Fetches the brand's partnership hash and redirects to /brand-portal/:hash
 */
function BrandPortalRedirect() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [redirected, setRedirected] = useState(false);

  useEffect(() => {
    if (!user?.email || redirected) return;
    api.get('/api/partnerships')
      .then((data) => {
        if (data.length > 0 && data[0].brand_hash) {
          setRedirected(true);
          navigate(`/brand-portal/${data[0].brand_hash}`, { replace: true });
        } else {
          setLoading(false);
        }
      })
      .catch(() => setLoading(false));
  }, [user, navigate, redirected]);

  if (loading) {
    return <div style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>Redirecting to your brand portal…</div>;
  }

  return (
    <div className="page-container">
      <div className="card" style={{ textAlign: 'center', padding: '60px 24px' }}>
        <div style={{ fontSize: '3rem', marginBottom: 16 }}>🚀</div>
        <h3 style={{ marginBottom: 8 }}>Coming Soon</h3>
        <p style={{ color: 'var(--text-muted)', maxWidth: 400, margin: '0 auto' }}>
          Your brand dashboard is being set up. Please contact your Finnet PoC for access.
        </p>
      </div>
    </div>
  );
}

