import { useId, useState, type FormEvent } from 'react';
import { ApiError, login, type Me } from '../api';
import Button from '../components/Button';

// SignIn is the one screen before a session exists: a heading, one line of
// help, a name, a password and a button.
export default function SignIn({ onSignedIn }: { onSignedIn: (me: Me) => void }) {
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const nameId = useId();
  const passwordId = useId();
  const errorId = useId();

  async function submit(e: FormEvent) {
    e.preventDefault();
    const n = name.trim();
    const p = password;
    // Cleared before the request, success or not: the password should sit
    // in the page no longer than it takes to send. It is never written to
    // any storage; the server answers with its own HttpOnly session cookie.
    setPassword('');
    if (!n || !p) {
      setError('Enter your name and password.');
      return;
    }
    setBusy(true);
    setError('');
    try {
      onSignedIn(await login(n, p));
    } catch (err) {
      // One fixed line per kind of failure, never the server's own words:
      // a login error must not say whether the name exists.
      if (err instanceof ApiError && err.status === 429) setError('Too many attempts. Wait a minute.');
      else if (err instanceof ApiError && err.status === 401) setError('Wrong name or password.');
      else if (err instanceof ApiError) setError('The console refused the sign-in. Try again.');
      else setError('Could not reach cutover. Try again.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="sign-in">
      <form className="sign-in-box" onSubmit={submit}>
        <h1 className="sign-in-title">Sign in to cutover</h1>
        <p className="sign-in-lede">Use the account you were given.</p>
        <label className="sign-in-label" htmlFor={nameId}>
          Name
        </label>
        <input
          id={nameId}
          className="sign-in-input"
          type="text"
          name="name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          autoComplete="username"
          autoCapitalize="none"
          spellCheck={false}
          autoFocus
          aria-invalid={error ? true : undefined}
          aria-describedby={error ? errorId : undefined}
        />
        <label className="sign-in-label" htmlFor={passwordId}>
          Password
        </label>
        <input
          id={passwordId}
          className="sign-in-input"
          type="password"
          name="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
          aria-invalid={error ? true : undefined}
          aria-describedby={error ? errorId : undefined}
        />
        {error && (
          <p id={errorId} className="sign-in-error" role="alert">
            {error}
          </p>
        )}
        <Button type="submit" className="sign-in-submit" disabled={busy}>
          {busy ? 'Signing in…' : 'Sign in'}
        </Button>
      </form>
    </main>
  );
}
