import { useEffect, useRef, useState } from 'react';
import { logout, onSignedOut, whoami, type Me } from './api';
import { navigate, parseHash, useHash, type Route } from './router';
import { openStream } from './stream';
import Shell from './components/Shell';
import SignIn from './views/SignIn';
import Migrations from './views/Migrations';
import MigrationView from './views/MigrationView';
import Rehearsal from './views/Rehearsal';
import Audit from './views/Audit';

export default function App() {
  const hash = useHash();
  const route = parseHash(hash);
  // undefined: still asking /api/me; null: signed out.
  const [me, setMe] = useState<Me | null | undefined>(undefined);
  // Where to go back to after signing in, so a 401 mid-task returns the
  // operator to the page they were on.
  const returnTo = useRef('#/');
  const streamFor = route.name === 'migration' ? route.id : undefined;
  if (route.name !== 'login' && route.name !== 'notfound' && hash) returnTo.current = hash;

  useEffect(() => {
    const off = onSignedOut(() => setMe(null));
    whoami().then(setMe, () => setMe(null));
    return off;
  }, []);

  useEffect(() => {
    if (!me) return;
    if (window.location.hash === '#/login') navigate(returnTo.current);
    // The migration page needs its status events, which the server sends
    // only on a stream scoped to that migration.
    return openStream(streamFor);
  }, [me, streamFor]);

  if (me === undefined) return <div className="boot" aria-busy="true" />;
  if (me === null) return <SignIn onSignedIn={setMe} />;

  return (
    <Shell route={route} me={me} onSignOut={() => void logout().catch(() => {})}>
      <Screen route={route} me={me} />
    </Shell>
  );
}

// Screen maps a route to its page.
function Screen({ route, me }: { route: Route; me: Me }) {
  switch (route.name) {
    case 'login':
    // Signed in but still on #/login for the moment before the effect
    // above sends the operator back to where they were.
    case 'migrations':
      return <Migrations />;
    case 'migration':
      return <MigrationView key={route.id} id={route.id} me={me} />;
    case 'rehearsal':
      return <Rehearsal key={route.id} id={route.id} />;
    case 'audit':
      return <Audit />;
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
