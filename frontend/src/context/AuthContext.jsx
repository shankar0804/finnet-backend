import { createContext, useContext, useState, useEffect, useCallback } from 'react';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null); // { email, name, picture, role }
  const [token, setToken] = useState('');

  // Restore session on mount
  useEffect(() => {
    const stored = sessionStorage.getItem('trakr_user');
    const storedToken = sessionStorage.getItem('trakr_token');
    if (stored && storedToken) {
      try {
        const parsed = JSON.parse(stored);
        // Check expiry if present (exp is seconds)
        if (parsed.exp && parsed.exp * 1000 < Date.now()) {
          sessionStorage.removeItem('trakr_user');
          sessionStorage.removeItem('trakr_token');
          return;
        }
        setUser(parsed);
        setToken(storedToken);
      } catch {
        sessionStorage.removeItem('trakr_user');
        sessionStorage.removeItem('trakr_token');
      }
    }
  }, []);

  const login = useCallback((userData, jwt) => {
    sessionStorage.setItem('trakr_user', JSON.stringify(userData));
    sessionStorage.setItem('trakr_token', jwt);
    setUser(userData);
    setToken(jwt);
  }, []);

  const logout = useCallback(() => {
    sessionStorage.removeItem('trakr_user');
    sessionStorage.removeItem('trakr_token');
    setUser(null);
    setToken('');
    if (window.google?.accounts?.id) {
      window.google.accounts.id.disableAutoSelect();
    }
  }, []);

  const value = {
    user,
    token,
    isAuthenticated: !!user,
    role: user?.role || 'junior',
    isAdmin: user?.role === 'admin',
    isSenior: user?.role === 'senior',
    isBrand: user?.role === 'brand',
    isPrivileged: user?.role === 'admin' || user?.role === 'senior',
    login,
    logout,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
