import { useState, useEffect, useCallback, useRef } from 'react';
import { useAuth } from '../context/AuthContext';
import { IconBriefcase, IconShield, IconArrowLeft } from '../components/Icons';

const API_BASE = import.meta.env.VITE_API_BASE || '';
const GOOGLE_CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID;
const ALLOWED_DOMAIN = 'finnetmedia.com';

export default function LoginPage() {
  const { login } = useAuth();
  const [step, setStep] = useState('choose'); // choose | google | password
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const googleBtnRef = useRef(null);

  // Google Sign-In callback — must be global
  const handleGoogleCredential = useCallback(async (response) => {
    try {
      const payload = JSON.parse(atob(response.credential.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
      const domain = (payload.email || '').split('@')[1] || '';

      if (domain.toLowerCase() !== ALLOWED_DOMAIN) {
        setError(`Access denied. "${payload.email}" is not a @${ALLOWED_DOMAIN} account.`);
        return;
      }

      const res = await fetch(`${API_BASE}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: payload.email, name: payload.name || '', picture: payload.picture || '' }),
      });
      const data = await res.json();

      if (data.error) { setError(data.error); return; }

      login({ email: payload.email, name: payload.name || '', picture: payload.picture || '', role: data.role || 'junior', exp: payload.exp }, data.token);
    } catch (err) {
      console.error('Google sign-in error:', err);
      setError('Sign-in failed. Please try again.');
    }
  }, [login]);

  // Initialize Google Sign-In when the Google step is shown
  useEffect(() => {
    if (step !== 'google' || !window.google?.accounts?.id) return;

    window.google.accounts.id.initialize({
      client_id: GOOGLE_CLIENT_ID,
      callback: handleGoogleCredential,
      auto_select: false,
    });

    if (googleBtnRef.current) {
      window.google.accounts.id.renderButton(googleBtnRef.current, {
        theme: 'outline',
        size: 'large',
        shape: 'pill',
        text: 'signin_with',
        width: 280,
      });
    }
  }, [step, handleGoogleCredential]);

  // Password login
  const handlePasswordLogin = async (e) => {
    e.preventDefault();
    if (!email || !password) { setError('Please enter email and password.'); return; }

    setLoading(true);
    setError('');
    try {
      const res = await fetch(`${API_BASE}/api/auth/login-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();
      if (!res.ok) { setError(data.error || 'Login failed'); return; }

      login({ email: data.email, name: data.name || '', picture: '', role: data.role || 'junior' }, data.token);
    } catch {
      setError('Network error. Try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-page">
      <div className="login-card">
        <h1 className="login-title">TRAKR<span>X</span></h1>
        <p className="login-subtitle">Influencer Intelligence Platform</p>
        <div className="login-divider" />

        {/* Step 1: Choose role */}
        {step === 'choose' && (
          <>
            <p className="login-desc">How would you like to sign in?</p>
            <div className="login-role-cards">
              <button className="login-role-card" onClick={() => { setError(''); setStep('password'); }}>
                <div className="login-role-icon"><IconBriefcase /></div>
                <span className="login-role-title">Brand Login</span>
                <span className="login-role-sub">For brand partners &amp; external teams</span>
              </button>
              <button className="login-role-card" onClick={() => { setError(''); setStep('google'); }}>
                <div className="login-role-icon internal"><IconShield /></div>
                <span className="login-role-title">Internal Team</span>
                <span className="login-role-sub">Finnet Media employees</span>
              </button>
            </div>
          </>
        )}

        {/* Step 2a: Password Login */}
        {step === 'password' && (
          <>
            <button className="login-back-btn" onClick={() => { setStep('choose'); setError(''); }}>
              <IconArrowLeft width={14} height={14} /> Back
            </button>
            <p className="login-desc">Sign in with your brand credentials</p>
            <form className="login-form" onSubmit={handlePasswordLogin}>
              <input className="input" type="email" placeholder="Email address" value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="email" />
              <input className="input" type="password" placeholder="Password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" />
              <button className="login-submit" type="submit" disabled={loading}>{loading ? 'Signing in…' : 'Sign In'}</button>
            </form>
          </>
        )}

        {/* Step 2b: Google Login */}
        {step === 'google' && (
          <>
            <button className="login-back-btn" onClick={() => { setStep('choose'); setError(''); }}>
              <IconArrowLeft width={14} height={14} /> Back
            </button>
            <p className="login-desc">Sign in with your <strong>@finnetmedia.com</strong> account</p>
            <div ref={googleBtnRef} className="g_id_signin" />
          </>
        )}

        {error && <div className="login-error">{error}</div>}
        <p className="login-footer">Authorized personnel only.</p>
      </div>
    </div>
  );
}
