import { useEffect, useRef, useState } from 'react';
import { logout, onSignedOut, whoami, type Me } from './api';
import { navigate, parseHash, useHash, type Route } from './router';
import { openStream } from './stream';
import Shell from './components/Shell';
import SignIn from './views/SignIn';

export default function App() {
  const hash = useHash();
  const route = parseHash(hash);
  // undefined: still asking /api/me; null: signed out.
  const [me, setMe] = useState<Me | null | undefined>(undefined);
  // Where to go back to after signing in, so a 401 mid-task returns the
  // operator to the page they were on.
  const returnTo = useRef('#/');
  if (route.name !== 'login' && route.name !== 'notfound' && hash) returnTo.current = hash;

  useEffect(() => {
    const off = onSignedOut(() => setMe(null));
    whoami().then(setMe, () => setMe(null));
    return off;
  }, []);

  useEffect(() => {
    if (!me) return;
    if (window.location.hash === '#/login') navigate(returnTo.current);
    return openStream();
  }, [me]);

  if (me === undefined) return <div className="boot" aria-busy="true" />;
  if (me === null) return <SignIn onSignedIn={setMe} />;

  return (
    <Shell route={route} me={me} onSignOut={() => void logout().catch(() => {})}>
      <Screen route={route} />
    </Shell>
  );
}

// Screen maps a route to its page. The migration pages arrive with the
// views that fill them; until then each says so plainly.
function Screen({ route }: { route: Route }) {
  switch (route.name) {
    case 'login':
    // Signed in but still on #/login for the moment before the effect
    // above sends the operator back to where they were.
    case 'migrations':
      return <Page title="Migrations" />;
    case 'migration':
      return <Page title={route.id} />;
    case 'rehearsal':
      return <Page title={`${route.id}: rehearsal`} />;
    case 'audit':
      return <Page title="Audit" />;
    case 'notfound':
      return (
        <div className="page">
          <h1 className="page-title">There is no such page.</h1>
          <p className="page-lede">
            <a href="#/">Go to Migrations</a>
          </p>
        </div>
      );
  }
}

function Page({ title }: { title: string }) {
  return (
    <div className="page">
      <div className="page-head">
        <h1 className="page-title">{title}</h1>
        <p className="page-lede">Nothing to show here yet.</p>
      </div>
    </div>
  );
}
