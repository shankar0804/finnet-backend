import { useState, useEffect, useRef } from 'react';
import { Outlet, NavLink, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import {
  IconUsers, IconUpload, IconSearch, IconFolder, IconShield, IconBriefcase, IconPhone, IconLogout,
} from './Icons';

const NAV_ITEMS = [
  { to: '/roster',  label: 'Database',   icon: IconUsers,     roles: ['admin', 'senior', 'junior'] },
  { to: '/ocr',     label: 'OCR Intake', icon: IconUpload,    roles: ['admin', 'senior', 'junior'] },
  { to: '/search',  label: 'AI Search',  icon: IconSearch,    roles: ['admin', 'senior', 'junior'] },
  { to: '/brands',  label: 'Brands',     icon: IconFolder,    roles: ['admin', 'senior'] },
  { to: '/team',    label: 'Team',       icon: IconShield,    roles: ['admin', 'senior'] },
  { to: '/whatsapp',label: 'WhatsApp',   icon: IconPhone,     roles: ['admin', 'senior'] },
  { to: '/brand-portal', label: 'Portal', icon: IconBriefcase, roles: ['brand'] },
];

export default function Layout() {
  const { user, role, logout } = useAuth();
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);

  const visibleItems = NAV_ITEMS.filter((item) => item.roles.includes(role));
  const initial = (user?.name || user?.email || '?')[0].toUpperCase();

  // Close menu on route change
  useEffect(() => { setMenuOpen(false); }, [location.pathname]);

  // Close menu on outside click
  useEffect(() => {
    if (!menuOpen) return;
    const handler = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false);
    };
    document.addEventListener('mousedown', handler);
    document.addEventListener('touchstart', handler);
    return () => {
      document.removeEventListener('mousedown', handler);
      document.removeEventListener('touchstart', handler);
    };
  }, [menuOpen]);

  return (
    <div className="app-shell">
      {/* ─── Top Bar ─── */}
      <header className="topbar">
        <button className="hamburger" onClick={() => setMenuOpen((v) => !v)} aria-label="Toggle menu">
          <span className={`hamburger-line ${menuOpen ? 'open' : ''}`} />
          <span className={`hamburger-line ${menuOpen ? 'open' : ''}`} />
          <span className={`hamburger-line ${menuOpen ? 'open' : ''}`} />
        </button>
        <span className="topbar-logo">TRAKR<span>X</span></span>
        <div className="topbar-spacer" />
        <div className="topbar-user">
          {user?.picture ? (
            <img src={user.picture} alt="" className="topbar-avatar" />
          ) : (
            <div className="topbar-avatar-fallback">{initial}</div>
          )}
          <span className="topbar-user-name">{user?.name || user?.email?.split('@')[0]}</span>
          <span className="topbar-role-badge">{role}</span>
        </div>
      </header>

      {/* ─── Backdrop (visible when menu open) ─── */}
      {menuOpen && <div className="menu-backdrop" onClick={() => setMenuOpen(false)} />}

      {/* ─── Slide-out Menu ─── */}
      <nav ref={menuRef} className={`slide-menu ${menuOpen ? 'open' : ''}`}>
        <div className="slide-menu-header">
          <span className="slide-menu-logo">TRAKR<span>X</span></span>
        </div>
        <div className="slide-menu-links">
          {visibleItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => `slide-menu-link${isActive ? ' active' : ''}`}
            >
              <item.icon />
              {item.label}
            </NavLink>
          ))}
        </div>
        <div className="slide-menu-footer">
          <button className="slide-menu-signout" onClick={logout}>
            <IconLogout /> Sign Out
          </button>
        </div>
      </nav>

      {/* ─── Main Content ─── */}
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}
